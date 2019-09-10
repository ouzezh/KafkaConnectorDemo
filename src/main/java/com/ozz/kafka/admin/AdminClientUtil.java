package com.ozz.kafka.admin;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AdminClientUtil implements Closeable {
  AdminClient adminClient;

  public static void main(String[] args) {
    try (AdminClientUtil adminClient = new AdminClientUtil("tnode-2:9092,tnode-3:9092,tnode-4:9092")) {
      Runtime.getRuntime().addShutdownHook(new Thread(()->adminClient.close()));
//      List<String> list = adminClient.listTopics();
//      System.out.println(list.size());
//
//      adminClient.createTopics("ou_test", 3, (short) 3);
//
//      TopicDescription td = adminClient.describeTopics("ou_test");
//      adminClient.getTopicOffset(td);
//
//      adminClient.deleteTopics(Collections.singleton("ou_test"));
//
//      System.out.println(adminClient.listConsumerGroups());
//
//      adminClient.describeConsumerGroups(Collections.singleton("connect-sink-inst-mdm"));
//
//      adminClient.listConsumerGroupOffsets("connect-database-sink-bm3-qenv2");
    }
  }

  public AdminClientUtil(String bootstrapServers) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    this.adminClient = AdminClient.create(props);
  }

  @Override
  public void close() {
    if (adminClient != null) {
      adminClient.close();
    }
  }

  public List<String> listTopics() {
    try {
      ListTopicsResult res = adminClient.listTopics();
      KafkaFuture<Collection<TopicListing>> f = res.listings();
      Collection<TopicListing> tl = f.get();
      List<String> list = new ArrayList<>();
      for (TopicListing item : tl) {
        list.add(item.name());
      }
      return list;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public TopicDescription describeTopics(String topic) {
    try {
      DescribeTopicsResult res = adminClient.describeTopics(Collections.singleton(topic));
      Map<String, KafkaFuture<TopicDescription>> f = res.values();
      TopicDescription td = null;
      for (Entry<String, KafkaFuture<TopicDescription>> en : f.entrySet()) {
        td = en.getValue().get();
        List<TopicPartitionInfo> ps = td.partitions();
        System.out.println(String.format("Topic:%s PartitionCount:%s", en.getKey(), td.partitions().size()));
        for (TopicPartitionInfo p : ps) {
          System.out.println(String.format("\tPartition:%s  Leader:%s Replicas: 4,2,3 Isr: 4,2,3", p.partition(), p.leader().id(), p.replicas(), p.isr()));
        }
      }
      return td;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void createTopics(String name, int numPartitions, short replicationFactor) {
    try {
      NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
      CreateTopicsResult res = adminClient.createTopics(Collections.singleton(topic));
      Map<String, KafkaFuture<Void>> fl = res.values();
      for (Entry<String, KafkaFuture<Void>> en : fl.entrySet()) {
        KafkaFuture<Void> f = en.getValue();
        f.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteTopics(Collection<String> topics) {
    try {
      DeleteTopicsResult res = adminClient.deleteTopics(topics);
      Map<String, KafkaFuture<Void>> fl = res.values();
      for (Entry<String, KafkaFuture<Void>> en : fl.entrySet()) {
        KafkaFuture<Void> f = en.getValue();
        f.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> listConsumerGroups() {
    try {
      ListConsumerGroupsResult res = adminClient.listConsumerGroups();
      KafkaFuture<Collection<ConsumerGroupListing>> fl = res.all();
      Collection<ConsumerGroupListing> list = fl.get();
      List<String> rl = new ArrayList<>();
      for (ConsumerGroupListing item : list) {
        rl.add(item.groupId());
      }
      return rl;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void describeConsumerGroups(Collection<String> groupIds) {
    try {
      DescribeConsumerGroupsResult res = adminClient.describeConsumerGroups(groupIds);
      KafkaFuture<Map<String, ConsumerGroupDescription>> fl = res.all();
      Map<String, ConsumerGroupDescription> fm = fl.get();
      for (Entry<String, ConsumerGroupDescription> en : fm.entrySet()) {
        ConsumerGroupDescription cd = en.getValue();
        System.out.println(String.format("groupId:%s, coordinator:%s, state:%s, partitionAssignor:%s",
                                         en.getKey(),
                                         cd.coordinator(),
                                         cd.state(),
                                         cd.partitionAssignor()));
        Collection<MemberDescription> ml = cd.members();
        for (MemberDescription m : ml) {
          System.out.println(String.format("\t%s", m.toString()));
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void listConsumerGroupOffsets(String groupId) {
    try {
      ListConsumerGroupOffsetsResult res = adminClient.listConsumerGroupOffsets(groupId);
      KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> f = res.partitionsToOffsetAndMetadata();
      Map<TopicPartition, OffsetAndMetadata> om = f.get();
      System.out.println(String.format("groupId:%s", groupId));
      for (Entry<TopicPartition, OffsetAndMetadata> en : om.entrySet()) {
        TopicPartition p = en.getKey();
        OffsetAndMetadata o = en.getValue();
        System.out.println(String.format("\ttopic:%s, partition:%s, offset:%s", p.topic(), p.partition(), o.offset()));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void getTopicOffset(TopicDescription td) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tnode-2:9092,tnode-3:9092,tnode-4:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("test-%s", RandomUtils.nextLong()));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));
      Collection<TopicPartition> partitions = new ArrayList<>();
      td.partitions().forEach((item)->partitions.add(new TopicPartition(td.name(), item.partition())));
      consumer.assign(partitions);
      consumer.seekToEnd(partitions);
      System.out.println(String.format("topic:%s", td.name()));
      for(TopicPartition p : partitions) {
        long offset = consumer.position(p);
        System.out.println(String.format("\tpartitions:%s, offset:%s", p.partition(), offset));
      }
    }
  }
}
