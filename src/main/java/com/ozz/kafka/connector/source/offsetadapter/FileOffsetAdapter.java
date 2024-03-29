package com.ozz.kafka.connector.source.offsetadapter;

import cn.hutool.core.lang.Pair;
import cn.hutool.log.StaticLog;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.FutureCallback;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class FileOffsetAdapter implements Closeable {
  private OffsetStorageReaderImpl offsetReader;
  private FileOffsetBackingStoreAdapter offsetStore;
  private String namespace;

  private Converter keyConverter;
  private Converter valueConverter;

  public static void main(String[] args) throws Exception {
    String connectorName = "localtest_source";
    String offsetsFile = "C:\\Users\\ouzezhou\\Desktop\\connect.offsets";

    printOffsets(connectorName, offsetsFile);

    // put offset
    ObjectMapper objectMapper = new ObjectMapper();
    try (FileOffsetAdapter adapter = new FileOffsetAdapter(connectorName, offsetsFile);) {
      Map<String, String> partition = objectMapper.readValue("{\"topic\":\"connect-test\",\"file\":\"xxx\"}", new TypeReference<HashMap<String, String>>() {});
      Map<String, Long> offset = objectMapper.readValue(String.format("{\"position\":%s}", System.currentTimeMillis()),
                                                        new TypeReference<HashMap<String, Long>>() {});
      adapter.put(Collections.singletonList(Pair.of(partition, offset)));
    }

    printOffsets(connectorName, offsetsFile);
  }

  private static void printOffsets(String connectorName, String offsetsFile) {
    try (FileOffsetAdapter adapter = new FileOffsetAdapter(connectorName, offsetsFile);) {
      // read offsets
      Map<Map<String, String>, Map<String, Object>> offsets = adapter.offsets();
      StaticLog.info(String.format("--print start, size=%d--", offsets.size()));
      for (Entry<Map<String, String>, Map<String, Object>> en : offsets.entrySet()) {
        StaticLog.info(String.format("%s = %s", en.getKey(), en.getValue()));
      }
      StaticLog.info(String.format("--print end, size=%d--", offsets.size()));
    }
  }

  public FileOffsetAdapter(String connectorName, String file) {
    this.namespace = connectorName;

    ConfigDef def = new ConfigDef().define(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, Type.STRING, Importance.HIGH, "path");
    Map<String, String> props = new HashMap<>();
    props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, file);
    WorkerConfig config = new WorkerConfig(def, props);

    offsetStore = new FileOffsetBackingStoreAdapter();
    offsetStore.configure(config);
    offsetStore.start();

    Map<String, Object> arg0 = Collections.singletonMap("schemas.enable", Boolean.FALSE);
    keyConverter = new JsonConverter();
    keyConverter.configure(arg0, true);
    valueConverter = new JsonConverter();
    valueConverter.configure(arg0, false);

    offsetReader = new OffsetStorageReaderImpl(offsetStore, connectorName, keyConverter, valueConverter);
  }

  public class FileOffsetBackingStoreAdapter extends FileOffsetBackingStore {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> List<Map<String, T>> partitions() {
      List<Map<String, T>> list = new ArrayList<>();
      for (ByteBuffer key : data.keySet()) {
        SchemaAndValue sv = keyConverter.toConnectData(namespace, key.array());
        List arr = (List) sv.value();
        list.add((Map<String, T>) arr.get(1));
      }
      return list;
    }
  }

  public <T> Map<String, Object> offset(Map<String, T> partition) {
    return offsetReader.offset(partition);
  }

  public <T> Map<Map<String, T>, Map<String, Object>> offsets() {
    return offsetReader.offsets(offsetStore.partitions());
  }

  public <T, U> void put(List<Pair<Map<String, T>, Map<String, U>>> offsets) {
    Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
    for (Pair<Map<String, T>, Map<String, U>> item : offsets) {
      Map<String, T> partition = item.getKey();
      Map<String, U> offset = item.getValue();
      values.put(toByteBuffer(keyConverter, partition, true), toByteBuffer(valueConverter, offset, false));
    }

    FutureCallback<Void> callback = new FutureCallback<>();
    offsetStore.set(values, callback);

    try {
      callback.get(10, TimeUnit.SECONDS);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <T> ByteBuffer toByteBuffer(Converter converter, Map<String, T> v, boolean iskey) {
    byte[] keySerialized = converter.fromConnectData(namespace, null, iskey ? Arrays.asList(namespace, v) : v);
    ByteBuffer keyBuffer = (keySerialized != null) ? ByteBuffer.wrap(keySerialized) : null;
    return keyBuffer;
  }

  @Override
  public void close() {
    offsetStore.stop();
  }
}
