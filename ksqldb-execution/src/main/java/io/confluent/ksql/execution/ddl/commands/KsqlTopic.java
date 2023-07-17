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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Objects;

@Immutable
public class KsqlTopic {
  private final String kafkaTopicName;
  private final KeyFormat keyFormat;
  private final ValueFormat valueFormat;

  public KsqlTopic(
      final String kafkaTopicName,
      final KeyFormat keyFormat,
      final ValueFormat valueFormat
  ) {
    this.kafkaTopicName = requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = requireNonNull(valueFormat, "valueFormat");
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlTopic ksqlTopic = (KsqlTopic) o;
    return Objects.equals(kafkaTopicName, ksqlTopic.kafkaTopicName)
        && Objects.equals(keyFormat, ksqlTopic.keyFormat)
        && Objects.equals(valueFormat, ksqlTopic.valueFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaTopicName, keyFormat, valueFormat);
  }

  @Override
  public String toString() {
    return "KsqlTopic{" + "kafkaTopicName='" + kafkaTopicName + '\''
        + ", keyFormat=" + keyFormat
        + ", valueFormat=" + valueFormat
        + '}';
  }
}
