package com.ozz.kafka.connector.util;

import java.util.Map;

public class Util {
  public static void sleep() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> String getConnectorMsg(String msg, T clz, String version, Map<String, String> props) {
    msg = String.format("%s: classname=%s,version=%s", msg, clz.getClass().getSimpleName(), version);
    if (props != null) {
      msg = String.format("%s,props=%s", msg, props.toString());
    }
    return msg;
  }
}
