package com.ozz.kafka.connector.client;

import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "tnode-2:9092,tnode-3:9092,tnode-4:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    props.put("schema.registry.url", "http://10.15.4.165:8181");

    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props);) {

      // Schema.Parser parser = new Schema.Parser();
      // Schema schema = parser.parse("{\"type\": \"record\", \"name\": \"User\", " + "\"fields\":
      // [{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"},
      // {\"name\": \"age\", \"type\": \"int\"}]}");
      RecordBuilder<Schema> v = SchemaBuilder.record("User");
      FieldAssembler<Schema> f = v.fields();
      f.requiredInt("id");
      f.requiredString("name");
      f.optionalInt("age");
      Schema schema = f.endRecord();

      Random rand = new Random();
      int id = 0;

      while (id < 10) {
        id++;
        String name = "name" + id;
        int age = rand.nextInt(40) + 1;
        GenericRecord user = new GenericData.Record(schema);
        user.put("id", id);
        user.put("name", name);
        user.put("age", age);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("dev-registry", user);

        producer.send(record);
        Thread.sleep(1000);
      }
    }
  }
}
