package io.confluent.kql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLConsoleOutputNode extends OutputNode {

  @JsonCreator
  public KQLConsoleOutputNode(@JsonProperty("id") PlanNodeId id,
                              @JsonProperty("source") PlanNode source,
                              @JsonProperty("schema") Schema schema) {
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
