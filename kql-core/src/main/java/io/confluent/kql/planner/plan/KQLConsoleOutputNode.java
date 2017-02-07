/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLConsoleOutputNode extends OutputNode {

  @JsonCreator
  public KQLConsoleOutputNode(@JsonProperty("id") final PlanNodeId id,
                              @JsonProperty("source") final PlanNode source,
                              @JsonProperty("schema") final Schema schema) {
    super(id, source, schema);


  }

  public String getKafkaTopicName() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }
}
