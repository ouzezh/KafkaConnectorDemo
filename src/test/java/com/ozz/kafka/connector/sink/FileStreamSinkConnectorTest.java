package com.ozz.kafka.connector.sink;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.sink.SinkRecord;

public class FileStreamSinkConnectorTest {
  private FileStreamSinkTask task;
  private String bootstrapServers = "k1:9092";
  private String topics = "localtest_topic";
  private String testGroup = "test";

  public static void main(String[] args) throws Exception {
    new FileStreamSinkConnectorTest().testPut();
  }

  private KafkaConsumer<byte[], byte[]> createNewConsumer(String brokers, String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 512000);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    return new KafkaConsumer<>(props);
  }

  private void testPut() throws Exception {
    // init
    init();

    // run
    try (KafkaConsumer<byte[], byte[]> consumer = createNewConsumer(bootstrapServers, testGroup)) {
      consumer.subscribe(Arrays.asList(topics.split(",")));

      for (int i = 0; i < 3; i++) {
        long starttime = System.currentTimeMillis();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
        testPut(records);
        System.out.println(String.format("test put %d, count %d, cost %dms", i + 1, records.count(), System.currentTimeMillis() - starttime));
        Thread.sleep(5000);
      }
    } finally {
      task.close(null);
      task.stop();
    }
  }

  private void init() {
    task = new FileStreamSinkTask();

    Map<String, String> props = new HashMap<String, String>();
    props.put(FileStreamSinkConnector.NAME_CONFIG, "testtask-0");
    props.put(FileStreamSinkConnector.FILE_CONFIG, System.getenv("USERPROFILE") + "\\Desktop\\test.txt");
    task.start(props);
    task.open(null);
  }

  private void testPut(ConsumerRecords<byte[], byte[]> records) {
    for (ConsumerRecord<byte[], byte[]> item : records) {
      SinkRecord record = new SinkRecord(item.topic(), item.partition(), null, item.key(), null, item.value(), item.offset());

      System.out.println(String.format("topic:%s, partition:%d, offset:%d, key:%s , value:%s",
                                       record.topic(),
                                       record.kafkaPartition(),
                                       record.kafkaOffset(),
                                       new String(item.key()),
                                       new String(item.value())));
      task.put(Collections.singleton(record));
    }
  }
}
