package com.ozz.kafka.client;

import cn.hutool.log.StaticLog;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerTest {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "tnode-2:9092,tnode-3:9092,tnode-4:9092");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", "http://10.15.4.165:8181");// 与KafkaAvroSerializer配合使用
    props.put("retries", 5);
    props.put("acks", "all");
    props.put("max.in.flight.requests.per.connection", 1);

    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props);) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));

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

      int count = 10;
      CountDownLatch cdl = new CountDownLatch(count);
      for(int id=1; id<=count; id++) {
        String name = "name" + id;
        int age = rand.nextInt(40) + 1;
        GenericRecord user = new GenericData.Record(schema);
        user.put("id", id);
        user.put("name", name);
        user.put("age", age);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("dev-test", user);

        int finalId = id;
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception e) {
            if(e != null) {
              e.printStackTrace();
            } else {
              cdl.countDown();
              StaticLog.info(String.format("callback %s: topic=%s, partition=%s, offset:%s", finalId, metadata.topic(), metadata.partition(), metadata.offset()));
            }
          }
        });
        Thread.sleep(1000);
      }
      producer.flush();
      if(cdl.await(30, TimeUnit.SECONDS)) {
        StaticLog.info("records submitted");
      } else {
        StaticLog.info("records submit error, see the callback log for more information");
      }
    }
  }

}
