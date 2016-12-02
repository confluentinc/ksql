package io.confluent.ksql.physical;

import org.apache.kafka.connect.data.Schema;

public class Column {

  public final String name;
  public final Schema.Type type;

  public Column(String name, Schema.Type type) {
    this.name = name;
    this.type = type;
  }
}
