package com.ozz.kafka.connector.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriter {
  private Logger log = LoggerFactory.getLogger(getClass());

  public void write(String name, String filename, Collection<SinkRecord> records) {
    Iterator<SinkRecord> var2 = records.iterator();

    List<String> lines = new ArrayList<>(records.size());
    while (var2.hasNext()) {
      String record = parseRecord((SinkRecord) var2.next());
      log.info("task {}: write line to {}: {}", name, filename, record);
      lines.add(record);
    }
    if (lines.isEmpty()) {
      return;
    }

    try {
      Files.write(Paths.get(filename), lines, StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String parseRecord(SinkRecord record) {
    return String.format("key=%s, value=%s", parseSchema(record.keySchema(), record.key()), parseSchema(record.valueSchema(), record.value()));
  }

  private String parseSchema(Schema schema, Object value) {
    if (value == null) {
      return "";
    }

    if (schema == null || schema.type() != Type.STRUCT) {
      return value.toString();
    } else {
      Struct struct = (Struct) value;
      Iterator<Field> it = schema.fields().iterator();
      Field f = it.next();
      StringBuilder b = new StringBuilder(String.format("%s:%s", f.name(), struct.get(f)));
      while (it.hasNext()) {
        b.append(",").append(String.format("%s:%s", f.name(), struct.get(f)));
      }
      return b.toString();
    }
  }

  public void flush() {
    // no buffer, nothing to do
  }
}
