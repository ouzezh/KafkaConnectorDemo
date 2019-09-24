package com.ozz.kafka.admin;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
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
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class AdminClientUtil implements Closeable {
  AdminClient adminClient;


  public static void main(String[] args) {
    String bootstrapServers = "10.15.4.150:9092,10.15.4.150:9093";
    try (AdminClientUtil adminClient = new AdminClientUtil(bootstrapServers)) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> adminClient.close()));
      // List<String> list = adminClient.listTopics();
      // System.out.println(list.size());
      //
      // adminClient.createTopics("ou_test", 3, (short) 1);
      //
//      adminClient.describeTopics("ou_test");
//      adminClient.getTopicOffset(bootstrapServers, Collections.singleton(new TopicPartition("ou_test", 0)));
      //
      // adminClient.deleteTopics(Collections.singleton("ou_test"));
      //
//       System.out.println(adminClient.listConsumerGroups());
      //
      // adminClient.describeConsumerGroups(Collections.singleton("connect-sink-inst-mdm"));
      //
      // adminClient.listConsumerGroupOffsets("connect-database-sink-bm3-qenv2");
      //
      // adminClient.createAcl(ResourceType.TOPIC, "ou_test", "User:ANONYMOUS", "10.200.79.172",
      // AclOperation.READ);
      //
      // adminClient.describeAcls(ResourceType.TOPIC, "ou_test");

      // adminClient.deleteAcls(ResourceType.TOPIC, "ou_test", "User:ANONYMOUS", "10.200.79.172",
      // AclOperation.READ);
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

  public Map<String, TopicDescription> describeTopics(Collection<String> topics) {
    try {
      DescribeTopicsResult res = adminClient.describeTopics(topics);
      Map<String, KafkaFuture<TopicDescription>> f = res.values();
      Map<String, TopicDescription> map = new HashMap<>();
      TopicDescription td;
      for (Entry<String, KafkaFuture<TopicDescription>> en : f.entrySet()) {
        td = en.getValue().get();
        List<TopicPartitionInfo> ps = td.partitions();
        map.put(en.getKey(), td);
        System.out.println(String.format("Topic:%s PartitionCount:%s", en.getKey(), td.partitions().size()));
        for (TopicPartitionInfo p : ps) {
          System.out.println(String.format("\tPartition:%s  Leader:%s Replicas: %s Isr: %s", p.partition(), p.leader().id(), p.replicas(), p.isr()));
        }
      }
      return map;
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
        System.out.println(String.format("group:%s, coordinator:%s, state:%s, partitionAssignor:%s",
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

  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId) {
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
      return om;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<TopicPartition, Long> getTopicOffset(String bootstrapServers, Collection<TopicPartition> partitions) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("test-%s", RandomUtils.nextLong()));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);) {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));
      Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
      for (Entry<TopicPartition, Long> en : offsets.entrySet()) {
        System.out.println(String.format("topic:%s\tpartitions:%s, offset:%s", en.getKey().topic(), en.getKey().partition(), en.getValue()));
      }
      return offsets; 
    }
  }

  /**
   * @param principal 用户，如：User:ANONYMOUS
   */
  public void createAcl(ResourceType resourceType, String name, String principal, String host, AclOperation aclOperation) {
    try {
      ResourcePattern resource = new ResourcePattern(resourceType, name, PatternType.LITERAL);
      AccessControlEntry entry = new AccessControlEntry(principal, host, aclOperation, AclPermissionType.ALLOW);
      Collection<AclBinding> acls = Collections.singleton(new AclBinding(resource, entry));
      CreateAclsResult res = adminClient.createAcls(acls);
      KafkaFuture<Void> f = res.all();
      f.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void describeAcls(ResourceType resourceType, String name) {
    try {
      PatternType patternType = PatternType.ANY;
      ResourcePatternFilter patternFilter = new ResourcePatternFilter(resourceType, name, patternType);
      AclBindingFilter filter = new AclBindingFilter(patternFilter, AccessControlEntryFilter.ANY);
      DescribeAclsResult res = adminClient.describeAcls(filter);
      KafkaFuture<Collection<AclBinding>> f = res.values();
      Collection<AclBinding> acls = f.get();
      for (AclBinding acl : acls) {
        System.out.println(String.format("%s:%s:%s", acl.pattern().resourceType(), acl.pattern().name(), acl.pattern().patternType()));
        System.out.println(String.format("\t%s has %s permission for operations: %s from hosts: %s",
                                         acl.entry().principal(),
                                         acl.entry().permissionType(),
                                         acl.entry().operation(),
                                         acl.entry().host()));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteAcls(ResourceType resourceType, String name, String principal, String host, AclOperation aclOperation) {
    try {
      PatternType patternType = PatternType.ANY;
      ResourcePatternFilter patternFilter = new ResourcePatternFilter(resourceType, name, patternType);
      AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(principal, host, aclOperation, AclPermissionType.ALLOW);
      AclBindingFilter filter = new AclBindingFilter(patternFilter, entryFilter);
      DeleteAclsResult res = adminClient.deleteAcls(Collections.singleton(filter));
      KafkaFuture<Collection<AclBinding>> f = res.all();
      Collection<AclBinding> acls = f.get();
      for (AclBinding acl : acls) {
        System.out.println(String.format("%s:%s:%s", acl.pattern().resourceType(), acl.pattern().name(), acl.pattern().patternType()));
        System.out.println(String.format("\t%s has %s permission for operations: %s from hosts: %s",
                                         acl.entry().principal(),
                                         acl.entry().permissionType(),
                                         acl.entry().operation(),
                                         acl.entry().host()));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
