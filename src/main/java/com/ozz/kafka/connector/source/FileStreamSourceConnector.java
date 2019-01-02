package com.ozz.kafka.connector.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ozz.kafka.connector.util.Util;

/**
 * (1)start 读取配置信息到类变量，包含任务配置、版本
 * (2)taskConfigs 分配配置到各个任务实例
 * (3)config 处理属性默认值
 * (4)taskClass 要启动的实例类
 * (5)stop 停止
 */
public class FileStreamSourceConnector extends SourceConnector {
  private Logger log = LoggerFactory.getLogger(getClass());

  public static final String FILE_CONFIG = "file";
  public static final String TOPIC_CONFIG = "topic";
  public static final String TASK_PARTITION_CONFIG = "partition";
  public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

  private List<String> filename;
  private String topic;
  private int batchSize;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FILE_CONFIG, Type.LIST, null, Importance.HIGH, "Source filename. If not specified, the standard input will be used")
      .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
      .define(TASK_BATCH_SIZE_CONFIG, Type.INT, 10, Importance.LOW, "The maximum number of records the Source task can read from file one time");

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(Util.getConnectorMsg("start", this, version(), props));

    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    filename = parsedConfig.getList(FILE_CONFIG);
    List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
    if (topics.size() != 1) {
      throw new ConfigException("'topic' in FileStreamSourceConnector configuration requires definition of a single topic");
    }
    topic = topics.get(0);
    batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    if(filename.size() > maxTasks) {
      throw new ConfigException(String.format("'topic' in FileStreamSourceConnector configuration file count %d more than maxTasks %d", filename.size(), maxTasks));
    }

    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for(int i=0; i<filename.size(); i++) {
      String tmp = filename.get(i);
      Map<String, String> config = new HashMap<>();
      config.put(FILE_CONFIG, tmp);
      config.put(TOPIC_CONFIG, topic);
      config.put(TASK_PARTITION_CONFIG, String.valueOf(i));
      config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
      configs.add(config);
    }

    return configs;
  }

  /**
   * 属性定义，描述、默认值...，描述可以在UI上展示
   */
  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return FileStreamSourceTask.class;
  }

  @Override
  public void stop() {
    log.info(Util.getConnectorMsg("stop", this, version(), null));
  }

}
