package io.confluent.ksql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KQLTopic;

import static java.util.Objects.requireNonNull;

public class KQLStructuredDataOutputNode extends OutputNode {

  final String kafkaTopicName;
  final KQLTopic kqlTopic;
  private final Field keyField;


  @JsonCreator
  public KQLStructuredDataOutputNode(@JsonProperty("id") PlanNodeId id,
                                     @JsonProperty("source") PlanNode source,
                                     @JsonProperty("schema") Schema schema,
                                     @JsonProperty("kqlTopic") KQLTopic kqlTopic,
                                     @JsonProperty("topicName") String topicName) {
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
