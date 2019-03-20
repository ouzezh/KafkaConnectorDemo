package com.ozz.kafka.connector.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ozz.kafka.connector.util.Util;

/**
 * @start 保存配置
 * @poll 拉数据
 * @stop 停止，回收资源
 */
public class FileStreamSourceTask extends SourceTask {
  private Logger log = LoggerFactory.getLogger(getClass());

  public static final String POSITION_FIELD = "position";
  private static Schema KEY_SCHEMA;
  private static Schema VALUE_SCHEMA;
//  private static org.apache.avro.Schema VALUE_SCHEMA;
//  private AvroData avroData = new AvroData(1);

  private String name;
  private String filename;
  private String topic;
  private int partition;
  private int batchSize;

  private Long sourceOffset = null;

  @Override
  public void initialize(SourceTaskContext context) {
    super.initialize(context);

    KEY_SCHEMA = Schema.STRING_SCHEMA;
    VALUE_SCHEMA = SchemaBuilder.struct().field("content", Schema.STRING_SCHEMA).field("ts", Schema.INT64_SCHEMA).build();
  }

  @Override
  public String version() {
    return new FileStreamSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(Util.getConnectorMsg("start task", props.get(FileStreamSourceConnector.NAME_CONFIG), version()));

    // config
    name = props.get(FileStreamSourceConnector.NAME_CONFIG);
    filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
    topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
    partition = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_PARTITION_CONFIG));
    batchSize = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));

    // recover offset safe point
    Map<String, Object> offset = context.offsetStorageReader().offset(offsetKey(topic, filename));
    if (offset != null && offset.get(POSITION_FIELD) != null) {
      sourceOffset = (Long) offset.get(POSITION_FIELD);
    }
    if (sourceOffset == null) {
      sourceOffset = 0L;
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    // check file exists
    if (filename == null || !Files.exists(Paths.get(filename)) || Files.isDirectory(Paths.get(filename))) {
      log.warn("read file {} : not exists", filename);
      Util.sleep();
      return Collections.emptyList();
    }

    Path path = Paths.get(filename);
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      // skip record before offset
      if (sourceOffset > 0) {
        for (long i = sourceOffset; i > 0; i--) {
          if (reader.readLine() == null) {
            Util.sleep();
            return Collections.emptyList();
          }
        }
      }

      // poll
      List<SourceRecord> records = new ArrayList<>();
      while (records.size() < batchSize) {
        // poll data
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        sourceOffset++;
        log.info("task {}: read file {}, partition:{}, line {}: {}", name, path.getFileName(), partition, sourceOffset, line);

        SourceRecord record = new SourceRecord(offsetKey(topic, filename), offsetValue(sourceOffset), topic, partition,
                                  KEY_SCHEMA, String.format("%s,%d", path.getFileName(), sourceOffset), VALUE_SCHEMA, new Struct(VALUE_SCHEMA).put("content", line).put("ts", System.currentTimeMillis()));

        // commit
        records.add(record);
      }

      if (records.isEmpty()) {
        Util.sleep();
      }
      return records;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * the stop() method is synchronized. This will be necessary because SourceTasks are given a
   * dedicated thread which they can block indefinitely, so they need to be stopped with a call from
   * a different thread in the Worker
   *
   */
  @Override
  public synchronized void stop() {
    log.info(Util.getConnectorMsg("stop task", name, version()));
  }

  private Map<String, String> offsetKey(String topic, String filename) {
    Map<String, String> map = new HashMap<>();
    map.put(FileStreamSourceConnector.TOPIC_CONFIG, filename);
    map.put(FileStreamSourceConnector.FILE_CONFIG, filename);
    return map;
  }

  private Map<String, Long> offsetValue(Long sourceOffset) {
    return Collections.singletonMap(POSITION_FIELD, sourceOffset);
  }
}
