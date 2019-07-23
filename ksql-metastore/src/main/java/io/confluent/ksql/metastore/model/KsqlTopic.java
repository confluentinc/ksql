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

package io.confluent.ksql.metastore.model;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.KsqlSerdeFactory;

@Immutable
public class KsqlTopic {

  private final String kafkaTopicName;
  private final KsqlSerdeFactory valueSerdeFactory;
  private final boolean isKsqlSink;

  public KsqlTopic(
      final String kafkaTopicName,
      final KsqlSerdeFactory valueSerdeFactory,
      final boolean isKsqlSink
  ) {
    this.kafkaTopicName = requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.valueSerdeFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
    this.isKsqlSink = isKsqlSink;
  }

  public KsqlSerdeFactory getValueSerdeFactory() {
    return valueSerdeFactory;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public boolean isKsqlSink() {
    return isKsqlSink;
  }
}
