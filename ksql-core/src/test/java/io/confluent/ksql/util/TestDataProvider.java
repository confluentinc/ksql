/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public abstract class TestDataProvider {
  final String topicName;
  final String ksqlSchemaString;
  final String key;
  final Schema schema;
  final Map<String, GenericRow> data;
  final String kstreamName;

  public TestDataProvider(String namePrefix, String ksqlSchemaString, String key, Schema schema, Map<String, GenericRow> data) {
    this.topicName = namePrefix + "_TOPIC";
    this.kstreamName =  namePrefix + "_KSTREAM";
    this.ksqlSchemaString = ksqlSchemaString;
    this.key = key;
    this.schema = schema;
    this.data = data;
  }

  public String topicName() {
    return topicName;
  }

  public String ksqlSchemaString() {
    return ksqlSchemaString;
  }

  public String key() {
    return key;
  }

  public Schema schema() {
    return schema;
  }

  public Map<String, GenericRow> data() {
    return data;
  }

  public String kstreamName() {
    return kstreamName;
  }
}
