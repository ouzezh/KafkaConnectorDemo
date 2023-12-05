package com.ozz.kafka.client;

import cn.hutool.log.StaticLog;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class ConsumerTest {

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tnode-2:9092,tnode-3:9092,tnode-4:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dev-registry");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put("schema.registry.url", "http://10.15.4.165:8181");// 与KafkaAvroDeserializer配合使用
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));

      consumer.subscribe(Collections.singletonList("dev-registry"));
      while (true) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMinutes(1));
        for (ConsumerRecord<String, GenericRecord> record : records) {
          StaticLog.info(String.format("poll record: %S", parseRecord(record)));
        }
      }
    }
  }

  private static String parseRecord(ConsumerRecord<String, GenericRecord> record) {
    return String.format("key=%s, value=%s", record.key(), parseSchema(record.value()));
  }

  private static String parseSchema(GenericRecord value) {
    if (value == null) {
      return "";
    }

    Iterator<Field> it = value.getSchema().getFields().iterator();
    Field f = it.next();
    StringBuilder b = new StringBuilder(String.format("%s:%s", f.name(), value.get(f.name())));
    while (it.hasNext()) {
      f = it.next();
      b.append(",").append(String.format("%s:%s", f.name(), value.get(f.name())));
    }
    return b.toString();
  }
}
