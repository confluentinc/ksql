/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public class KsqlBareOutputNode extends OutputNode {

  @JsonCreator
  public KsqlBareOutputNode(@JsonProperty("id") final PlanNodeId id,
                            @JsonProperty("source") final PlanNode source,
                            @JsonProperty("schema") final Schema schema,
                            @JsonProperty("limit") final Optional<Integer> limit) {
    super(id, source, schema, limit);


  }

  public String getKafkaTopicName() {
    return null;
  }

  @Override
  public Field getKeyField() {
    return null;
  }
}
