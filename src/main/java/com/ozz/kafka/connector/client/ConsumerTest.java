package com.ozz.kafka.connector.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class ConsumerTest {

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "tnode-2:9092,tnode-3:9092,tnode-4:9092");
    props.put("group.id", "dev-registry");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
    props.put("schema.registry.url", "http://10.15.4.165:8181");
    KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singletonList("dev-registry"));

    try {
      while (true) {
        ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
        for (ConsumerRecord<String, GenericRecord> record : records) {
          System.out.println(String.format("poll record: %S", parseRecord(record)));
        }
      }
    } finally {
      consumer.close();
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
