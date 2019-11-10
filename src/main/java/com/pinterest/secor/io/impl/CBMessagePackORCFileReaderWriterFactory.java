package com.pinterest.secor.io.impl;

import cb.javabeat.pingdecoder.InvalidPingFieldException;
import cb.javabeat.pingdecoder.MabPing;
import cb.javabeat.pingdecoder.PingSanitizer;
import cb.javabeat.pingdecoder.exception.PingDecoderConfigException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.ReflectionUtil;
import com.pinterest.secor.util.orc.JsonFieldFiller;
import com.pinterest.secor.util.orc.VectorColumnFiller;
import com.pinterest.secor.util.orc.schema.ORCSchemaProvider;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ZlibCodec;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;
import org.msgpack.MessageTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CBMessagePackORCFileReaderWriterFactory implements FileReaderWriterFactory {
   private static final Logger LOG = LoggerFactory.getLogger(FileRegistry.class);
   private ORCSchemaProvider schemaProvider;

   public CBMessagePackORCFileReaderWriterFactory(SecorConfig config) throws Exception {
      schemaProvider = ReflectionUtil.createORCSchemaProvider(config.getORCSchemaProviderClass(), config);
   }

   @Override
   public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
      return new CBMessagePackORCFileReader(logFilePath, codec);
   }

   @Override
   public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
      return new CBMessagePackORCFileWriter(logFilePath, codec);
   }

   private CompressionKind resolveCompression(CompressionCodec codec) {
      if (codec instanceof Lz4Codec) {
         return CompressionKind.LZ4;
      } else if (codec instanceof SnappyCodec) {
         return CompressionKind.SNAPPY;
      } else {
         return codec instanceof ZlibCodec ? CompressionKind.ZLIB : CompressionKind.NONE;
      }
   }

   protected class CBMessagePackORCFileWriter implements FileWriter {
      private Gson gson = new Gson();
      private PingSanitizer pingSanitizer;
      private Writer writer;
      private VectorColumnFiller.JsonConverter[] converters;
      private VectorizedRowBatch batch;
      private int rowIndex;
      private TypeDescription schema;

      public CBMessagePackORCFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException, PingDecoderConfigException {
         Configuration conf = new Configuration();
         Path path = new Path(logFilePath.getLogFilePath());
         schema = schemaProvider.getSchema(logFilePath.getTopic(), logFilePath);
         List fieldTypes = schema.getChildren();
         converters = new VectorColumnFiller.JsonConverter[fieldTypes.size()];

         for(int c = 0; c < converters.length; ++c) {
            converters[c] = VectorColumnFiller.createConverter((TypeDescription)fieldTypes.get(c));
         }

         writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).compress(resolveCompression(codec)).setSchema(schema));
         batch = schema.createRowBatch();
         pingSanitizer = new PingSanitizer((Reader)null, (Reader)null, (Reader)null, (String)null, (String)null, (String)null, (MemcachedClient)null, (Reader)null, (File)null);
      }

      @Override
      public long getLength() throws IOException {
         return writer.getRawDataSize();
      }

      private String decodeMsgpackIntoJsonString(byte[] data) throws IOException {
         MabPing mabPing = null;

         try {
            mabPing = MabPing.decodeMessagePack(data);
            pingSanitizer.sanitizePing(mabPing);
         } catch (InvalidPingFieldException var5) {
            LOG.debug("Caught InvalidPingFieldException while sanitizing: " + var5.getMessage());
            return null;
         } catch (MessageTypeException var6) {
            return new String(data);
         }

         MyMessage myMessage = new MyMessage();
         myMessage.keys = new Keys();
         myMessage.ts = mabPing.getTimestamp();
         myMessage.keys.me = mabPing.mabErrorType;
         myMessage.keys.h = mabPing.host;
         myMessage.keys.d = mabPing.domain;
         myMessage.keys.p = mabPing.path;
         myMessage.keys.x = mabPing.mabExperimentId;
         myMessage.keys.v = mabPing.mabVariantId;
         myMessage.keys.u = mabPing.userSessionId;
         myMessage.keys.e = mabPing.mabEngagedTimeOnPageSeconds;
         myMessage.keys.sl = mabPing.specificLocation;
         myMessage.keys.c = mabPing.timeOnPageMinutes;
         myMessage.keys.G = mabPing.userAgent;
         myMessage.keys.a = mabPing.ipAddress;
         myMessage.keys.xo = mabPing.mabExperimentOrigin;
         myMessage.keys.ml = mabPing.mabLiftGroup;
         myMessage.keys.md = mabPing.mabDebug;
         String json = gson.toJson(myMessage);
         return json;
      }

      @Override
      public void write(KeyValue keyValue) throws IOException {
         String jsonString = decodeMsgpackIntoJsonString(keyValue.getValue());
         if (jsonString != null) {
            rowIndex = batch.size++;
            VectorColumnFiller.fillRow(rowIndex, converters, schema, batch, (JsonObject)gson.fromJson(jsonString, JsonObject.class));
            if (batch.size == batch.getMaxSize()) {
               writer.addRowBatch(batch);
               batch.reset();
            }

         }
      }

      @Override
      public void close() throws IOException {
         writer.addRowBatch(batch);
         writer.close();
      }
   }

   protected class CBMessagePackORCFileReader implements FileReader {
      private int rowIndex = 0;
      private long offset;
      private RecordReader rows;
      private VectorizedRowBatch batch;
      private TypeDescription schema;

      public CBMessagePackORCFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
         schema = schemaProvider.getSchema(logFilePath.getTopic(), logFilePath);
         Path path = new Path(logFilePath.getLogFilePath());
         org.apache.orc.Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration(true)));
         offset = logFilePath.getOffset();
         rows = reader.rows();
         batch = reader.getSchema().createRowBatch();
         rows.nextBatch(batch);
      }

      @Override
      public KeyValue next() throws IOException {
         boolean endOfBatch = false;
         StringWriter sw = new StringWriter();
         if (rowIndex > batch.size - 1) {
            endOfBatch = !rows.nextBatch(batch);
            rowIndex = 0;
         }

         if (endOfBatch) {
            rows.close();
            return null;
         } else {
            try {
               JsonFieldFiller.processRow(new JSONWriter(sw), batch, schema, rowIndex);
            } catch (JSONException var4) {
               LOG.error("Unable to parse json {}", sw.toString());
               return null;
            }

            ++rowIndex;
            return new KeyValue((long)(offset++), sw.toString().getBytes("UTF-8"));
         }
      }

      @Override
      public void close() throws IOException {
         rows.close();
      }
   }

   public static class MyMessage {
      public Double ts;
      public Keys keys;
   }

   public static class Keys {
      public Integer me;
      public String h;
      public String d;
      public String p;
      public String x;
      public String v;
      public String u;
      public Integer e;
      public String sl;
      public Double c;
      public String G;
      public String a;
      public String xo;
      public String ml;
      public Boolean md;
   }
}
