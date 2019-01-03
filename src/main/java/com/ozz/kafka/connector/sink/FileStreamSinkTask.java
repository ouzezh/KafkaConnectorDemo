package com.ozz.kafka.connector.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ozz.kafka.connector.util.Util;

public class FileStreamSinkTask extends SinkTask {
  private Logger log = LoggerFactory.getLogger(getClass());

  private String taskId;
  private String filename;
  public static final String TASKID_FIELD = "taskId";

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(Util.getConnectorMsg("start", this, version(), context.configs()));

    this.taskId = props.get(TASKID_FIELD);
    this.filename = props.get(FileStreamSinkConnector.FILE_CONFIG);

    try {
      Path path = Paths.get(filename);
      if(Files.notExists(path)) {
        Files.createFile(path);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if(records.isEmpty()) {
      return;
    }
    Iterator<SinkRecord> var2 = records.iterator();

    List<String> lines = new ArrayList<>(records.size());
    while (var2.hasNext()) {
      SinkRecord record = (SinkRecord) var2.next();
      log.info("task {} write line to {}: {}", this.taskId, this.filename, record.value());
      lines.add(record.value().toString());
    }
    if(lines.isEmpty()) {
      return;
    }

    try {
      Files.write(Paths.get(filename), lines, StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   * used during the offset commit process
   *
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
  }

  @Override
  public void stop() {
    log.info(Util.getConnectorMsg("stop", this, version(), context.configs()));
  }

}
