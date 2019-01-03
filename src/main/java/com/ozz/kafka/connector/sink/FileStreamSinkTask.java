package com.ozz.kafka.connector.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ozz.kafka.connector.util.Util;

public class FileStreamSinkTask extends SinkTask {
  private Logger log = LoggerFactory.getLogger(getClass());

  private String name;
  private String filename;

  private FileWriter fileWriter;

  @Override
  public void initialize(SinkTaskContext context) {
    super.initialize(context);

    fileWriter = new FileWriter();
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(Util.getConnectorMsg("start task", props.get(FileStreamSinkConnector.NAME_CONFIG), version(), context.configs()));

    name = props.get(FileStreamSinkConnector.NAME_CONFIG);
    filename = props.get(FileStreamSinkConnector.FILE_CONFIG);

    // init
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

    fileWriter.write(name, filename, records);
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   * used during the offset commit process
   *
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    super.flush(currentOffsets);

    fileWriter.flush();
  }

  @Override
  public void stop() {
    log.info(Util.getConnectorMsg("stop task", name, version(), context.configs()));
  }

  /**
   * 释放资源，再平衡开始时调用
   *
   */
  @Override
  public void close(Collection<TopicPartition> partitions) {
    super.close(partitions);
    log.info(Util.getConnectorMsg("close task", name, version(), context.configs()));
  }

  /**
   * 分配资源，再平衡完成时调用
   *
   */
  @Override
  public void open(Collection<TopicPartition> partitions) {
    super.open(partitions);
    log.info(Util.getConnectorMsg("open task", name, version(), context.configs()));
  }
}
