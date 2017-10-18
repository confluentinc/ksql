/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.KsqlTopicSerDe;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.Optional;

public class KsqlStructuredDataOutputNode extends OutputNode {

  final String kafkaTopicName;
  final KsqlTopic ksqlTopic;
  private final Field keyField;
  final Field timestampField;
  final Map<String, Object> outputProperties;


  @JsonCreator
  public KsqlStructuredDataOutputNode(@JsonProperty("id") final PlanNodeId id,
                                      @JsonProperty("source") final PlanNode source,
                                      @JsonProperty("schema") final Schema schema,
                                      @JsonProperty("timestamp") final Field timestampField,
                                      @JsonProperty("key") final Field keyField,
                                      @JsonProperty("ksqlTopic") final KsqlTopic ksqlTopic,
                                      @JsonProperty("topicName") final String topicName,
                                      @JsonProperty("outputProperties") final Map<String, Object>
                                            outputProperties,
                                      @JsonProperty("limit") final Optional<Integer> limit) {
    super(id, source, schema, limit);
    this.kafkaTopicName = topicName;
    this.keyField = keyField;
    this.timestampField = timestampField;
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

  public Field getTimestampField() {
    return timestampField;
  }

  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  public Map<String, Object> getOutputProperties() {
    return outputProperties;
  }

  public KsqlTopicSerDe getTopicSerde() {
    return ksqlTopic.getKsqlTopicSerDe();
  }

  public static class Builder {
    private PlanNodeId id;
    private PlanNode source;
    private Schema schema;
    private Field timestampField;
    private Field keyField;
    private KsqlTopic ksqlTopic;
    private String topicName;
    private Map<String, Object> outputProperties;
    private Optional<Integer> limit;

    public KsqlStructuredDataOutputNode build() {
      return new KsqlStructuredDataOutputNode(id,
          source,
          schema,
          timestampField,
          keyField,
          ksqlTopic,
          topicName,
          outputProperties,
          limit);
    }

    public static Builder from(final KsqlStructuredDataOutputNode original) {
      return new Builder()
          .withId(original.getId())
          .withSource(original.getSource())
          .withSchema(original.getSchema())
          .withTimestampField(original.getTimestampField())
          .withKeyField(original.getKeyField())
          .withKsqlTopic(original.getKsqlTopic())
          .withTopicName(original.getKafkaTopicName())
          .withOutputProperties(original.getOutputProperties())
          .withLimit(original.getLimit());
    }

    public Builder withLimit(final Optional<Integer> limit) {
      this.limit = limit;
      return this;
    }

    public Builder withOutputProperties(Map<String, Object> outputProperties) {
      this.outputProperties = outputProperties;
      return this;
    }

    public Builder withTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder withKsqlTopic(KsqlTopic ksqlTopic) {
      this.ksqlTopic = ksqlTopic;
      return this;
    }

    public Builder withKeyField(Field keyField) {
      this.keyField = keyField;
      return this;
    }

    public Builder withTimestampField(Field timestampField) {
      this.timestampField = timestampField;
      return this;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder withSource(PlanNode source) {
      this.source = source;
      return this;
    }

    public Builder withId(final PlanNodeId id) {
      this.id = id;
      return this;
    }

  }

}
