/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.KSQLTopic;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class KSQLStructuredDataOutputNode extends OutputNode {

  final String kafkaTopicName;
  final KSQLTopic ksqlTopic;
  private final Field keyField;
  final Map<String, Object> outputProperties;


  @JsonCreator
  public KSQLStructuredDataOutputNode(@JsonProperty("id") final PlanNodeId id,
                                      @JsonProperty("source") final PlanNode source,
                                      @JsonProperty("schema") final Schema schema,
                                      @JsonProperty("ksqlTopic") final KSQLTopic ksqlTopic,
                                      @JsonProperty("topicName") final String topicName,
                                      @JsonProperty("outputProperties") final Map<String, Object>
                                            outputProperties) {
    super(id, source, schema);
    this.kafkaTopicName = topicName;
    this.keyField = source.getKeyField();
    this.ksqlTopic = ksqlTopic;
    this.outputProperties = outputProperties;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  public KSQLTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Map<String, Object> getOutputProperties() {
    return outputProperties;
  }
}
