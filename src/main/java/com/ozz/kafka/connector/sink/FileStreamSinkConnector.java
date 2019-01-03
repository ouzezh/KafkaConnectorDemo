package com.ozz.kafka.connector.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ozz.kafka.connector.util.Util;

public class FileStreamSinkConnector extends SinkConnector {
  private Logger log = LoggerFactory.getLogger(getClass());

  public static final String NAME_CONFIG = "name";
  public static final String FILE_CONFIG = "file";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(NAME_CONFIG, Type.STRING, null, Importance.HIGH, "connector's own property. just for print log")
      .define(FILE_CONFIG, Type.STRING, (Object) null, Importance.HIGH, "Destination filename");

  private String name;
  private String filename;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(Util.getConnectorMsg("start connector", props.get(NAME_CONFIG), version(), props));

    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    this.name = parsedConfig.getString(NAME_CONFIG);
    this.filename = parsedConfig.getString(FILE_CONFIG);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();

    for (int i = 0; i < maxTasks; ++i) {
      Map<String, String> config = new HashMap<>();
      if (this.filename != null) {
        config.put(NAME_CONFIG, String.format("%s-%d", this.name, i));
        config.put(FILE_CONFIG, this.filename);
      }

      configs.add(config);
    }

    return configs;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return FileStreamSinkTask.class;
  }

  @Override
  public void stop() {
    log.info(Util.getConnectorMsg("stop connector", this.name, version(), null));
  }

}
