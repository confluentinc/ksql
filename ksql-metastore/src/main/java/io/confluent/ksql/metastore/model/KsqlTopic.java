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
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;

@Immutable
public class KsqlTopic implements DataSource {

  private final String ksqlTopicName;
  private final String kafkaTopicName;
  private final KsqlTopicSerDe ksqlTopicSerDe;
  private final boolean isKsqlSink;

  public KsqlTopic(
      final String ksqlTopicName,
      final String kafkaTopicName,
      final KsqlTopicSerDe ksqlTopicSerDe,
      final boolean isKsqlSink
  ) {
    this.ksqlTopicName = requireNonNull(ksqlTopicName, "ksqlTopicName");
    this.kafkaTopicName = requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.ksqlTopicSerDe = requireNonNull(ksqlTopicSerDe, "ksqlTopicSerDe");
    this.isKsqlSink = isKsqlSink;
  }

  public KsqlTopicSerDe getKsqlTopicSerDe() {
    return ksqlTopicSerDe;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public String getKsqlTopicName() {
    return ksqlTopicName;
  }

  public boolean isKsqlSink() {
    return isKsqlSink;
  }

  @Override
  public String getName() {
    return ksqlTopicName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.KTOPIC;
  }
}
