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

}
