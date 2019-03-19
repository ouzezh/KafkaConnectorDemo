package com.ozz.kafka.connector.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class FileStreamSinkConnectorTest {
  private FileStreamSinkTask task;
  private String bootstrapServers = "k1:9092";
  private String topics = "connect-test";
  private String testGroup = "test";
  private String schemaRegistryUrl = "http://k1:8081";

  public static void main(String[] args) throws Exception {
    new FileStreamSinkConnectorTest().testPut();
  }

  private KafkaConsumer<String, GenericRecord> createNewConsumer(String brokers, String schemaRegistryUrl, String group) {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 512000);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
    return new KafkaConsumer<>(props);
  }

  private void testPut() throws Exception {
    // init
    init();

    // run
    try (KafkaConsumer<String, GenericRecord> consumer = createNewConsumer(bootstrapServers, schemaRegistryUrl, testGroup)) {
      consumer.subscribe(Arrays.asList(topics.split(",")));

      for (int i = 0; i < 3; i++) {
        long starttime = System.currentTimeMillis();
        ConsumerRecords<String, GenericRecord> records = consumer.poll(10000);
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

  private void testPut(ConsumerRecords<String, GenericRecord> records) {
    for (ConsumerRecord<String, GenericRecord> item : records) {
      SinkRecord record = new SinkRecord(item.topic(), item.partition(), null, item.key(), null, item.value(), item.offset());
      System.out.println(String.format("topic:%s, partition:%d, offset:%d, key:%s , value:%s",
                                       record.topic(),
                                       record.kafkaPartition(),
                                       record.kafkaOffset(),
                                       item.key(),
                                       item.value()));
      task.put(Collections.singleton(record));
    }
  }
}
