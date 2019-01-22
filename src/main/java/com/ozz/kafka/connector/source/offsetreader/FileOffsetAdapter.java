package com.ozz.kafka.connector.source.offsetreader;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.Callback;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileOffsetAdapter implements Closeable {

  private OffsetStorageReaderImpl offsetReader;
  private FileOffsetBackingStore store;
  private String namespace;

  private Converter keyConverter;
  private Converter valueConverter;

  public static void main(String[] args) throws Exception {
    String connectorName = "localtest_source";
    String file = "C:\\Users\\ouzezhou\\Desktop\\connect.offsets";

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> partition = objectMapper.readValue("{\"db\":\"BJNIS2\",\"table\":\"test_bs_lesson\"}", new TypeReference<HashMap<String, String>>() {});
    Map<String, String> partition2 = objectMapper.readValue("{\"db\":\"BJNIS2\",\"table\":\"xxx\"}", new TypeReference<HashMap<String, String>>() {});

    try (FileOffsetAdapter reader = new FileOffsetAdapter(connectorName, file);) {
      // read old offset
      Map<String, Object> offset = reader.offset(partition);
      System.out.println(offset);

      // read new offset
      System.out.println(reader.offset(partition2));

      // put offset
      List<Pair<Map<String, String>, Map<String, Long>>> list = new ArrayList<>();
      Map<String, Long> offset2 = objectMapper.readValue("{\"lastUpdateVersion\":-1,\"lastLoadId\":-2}", new TypeReference<HashMap<String, Long>>() {});
      list.add(Pair.of(partition2, offset2));
      reader.put(list);
    }

    try (FileOffsetAdapter reader = new FileOffsetAdapter(connectorName, file);) {
      // read old offset
      Map<String, Object> offset = reader.offset(partition);
      System.out.println(offset);

      // read new offset
      System.out.println(reader.offset(partition2));
    }
  }

  public FileOffsetAdapter(String connectorName, String file) {
    this.namespace = connectorName;

    ConfigDef def = new ConfigDef().define(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, Type.STRING, Importance.HIGH, "path");
    Map<String, String> props = new HashMap<>();
    props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, file);
    WorkerConfig config = new WorkerConfig(def, props);

    store = new FileOffsetBackingStore();
    store.configure(config);
    store.start();

    Map<String, Object> arg0 = Collections.singletonMap("schemas.enable", Boolean.FALSE);
    keyConverter = new JsonConverter();
    keyConverter.configure(arg0, true);
    valueConverter = new JsonConverter();
    valueConverter.configure(arg0, false);

    offsetReader = new OffsetStorageReaderImpl(store, connectorName, keyConverter, valueConverter);
  }

  public <T> Map<String, Object> offset(Map<String, T> partition) {
    return offsetReader.offset(partition);
  }

  public <T, U> void put(List<Pair<Map<String, T>, Map<String, U>>> list) {
    Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
    for (Pair<Map<String, T>, Map<String, U>> item : list) {
      Map<String, T> partition = item.getKey();
      Map<String, U> offset = item.getValue();
      values.put(toByteBuffer(keyConverter, partition, true), toByteBuffer(valueConverter, offset, false));
    }

    store.set(values, new Callback<Void>() {
      @Override
      public void onCompletion(Throwable error, Void result) {
        error = new RuntimeException("test");// XXX
        if (error != null) {
          if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
          } else {
            throw new RuntimeException(error);
          }
        }
      }
    });
    System.out.println("--End--");
  }

  private <T> ByteBuffer toByteBuffer(Converter converter, Map<String, T> v, boolean iskey) {
    byte[] keySerialized = converter.fromConnectData(namespace, null, iskey ? Arrays.asList(namespace, v) : v);
    ByteBuffer keyBuffer = (keySerialized != null) ? ByteBuffer.wrap(keySerialized) : null;
    return keyBuffer;
  }

  @Override
  public void close() {
    store.stop();
  }
}
