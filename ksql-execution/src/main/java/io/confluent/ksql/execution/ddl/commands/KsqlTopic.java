/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.ddl.commands;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;

@Immutable
public class KsqlTopic {
  private static final String KAFKA_TOPIC_NAME = "kafkaTopicName";
  private static final String KEY_FORMAT = "keyFormat";
  private static final String VALUE_FORMAT = "valueFormat";
  private static final String IS_KSQL_SINK = "isKsqlSink";

  @JsonProperty(KAFKA_TOPIC_NAME)
  private final String kafkaTopicName;
  @JsonProperty(KEY_FORMAT)
  private final KeyFormat keyFormat;
  @JsonProperty(VALUE_FORMAT)
  private final ValueFormat valueFormat;
  @JsonProperty(IS_KSQL_SINK)
  private final boolean isKsqlSink;

  @JsonCreator
  public KsqlTopic(
      @JsonProperty(KAFKA_TOPIC_NAME) final String kafkaTopicName,
      @JsonProperty(KEY_FORMAT) final KeyFormat keyFormat,
      @JsonProperty(VALUE_FORMAT) final ValueFormat valueFormat,
      @JsonProperty(IS_KSQL_SINK) final boolean isKsqlSink
  ) {
    this.kafkaTopicName = requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = requireNonNull(valueFormat, "valueFormat");
    this.isKsqlSink = isKsqlSink;
  }

  public KeyFormat getKeyFormat() {
    return keyFormat;
  }

  public ValueFormat getValueFormat() {
    return valueFormat;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public boolean isKsqlSink() {
    return isKsqlSink;
  }
}
