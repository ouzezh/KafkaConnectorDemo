package com.ozz.kafka.connector.util;

public class Util {
  public static void sleep() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> String getConnectorMsg(String prefixMsg, String name, String version) {
    String msg = String.format("%s %s, version=%s", prefixMsg, name, version);
    return msg;
  }
}
