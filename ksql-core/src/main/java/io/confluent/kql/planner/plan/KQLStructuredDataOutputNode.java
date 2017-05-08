/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kql.metastore.KQLTopic;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KQLStructuredDataOutputNode extends OutputNode {

  final String kafkaTopicName;
  final KQLTopic kqlTopic;
  private final Field keyField;


  @JsonCreator
  public KQLStructuredDataOutputNode(@JsonProperty("id") final PlanNodeId id,
                                     @JsonProperty("source") final PlanNode source,
                                     @JsonProperty("schema") final Schema schema,
                                     @JsonProperty("kqlTopic") final KQLTopic kqlTopic,
                                     @JsonProperty("topicName") final String topicName) {
    super(id, source, schema);
    this.kafkaTopicName = topicName;
    this.keyField = source.getKeyField();
    this.kqlTopic = kqlTopic;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  public KQLTopic getKqlTopic() {
    return kqlTopic;
  }

}
