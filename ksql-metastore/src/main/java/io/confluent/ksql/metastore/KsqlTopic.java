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

package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KsqlException;

import java.util.OptionalLong;

public class KsqlTopic implements DataSource {

  private final String topicName;
  private final String kafkaTopicName;
  private final KsqlTopicSerDe ksqlTopicSerDe;
  private final OptionalLong partitions;

  public KsqlTopic(final String topicName, final String kafkaTopicName,
                   final KsqlTopicSerDe ksqlTopicSerDe, final OptionalLong partitions) {
    this.topicName = topicName;
    this.kafkaTopicName = kafkaTopicName;
    this.ksqlTopicSerDe = ksqlTopicSerDe;
    this.partitions = partitions;
  }

  public KsqlTopic(final String topicName, final String kafkaTopicName,
                   final KsqlTopicSerDe ksqlTopicSerDe) {
    this(topicName, kafkaTopicName, ksqlTopicSerDe, OptionalLong.empty());
  }

  public KsqlTopicSerDe getKsqlTopicSerDe() {
    return ksqlTopicSerDe;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public String getName() {
    return topicName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.KTOPIC;
  }

  public Long getPartitions() {
    return partitions.orElseThrow(() -> new KsqlException(
        "KsqlTopic does not have partitions specified")
    );
  }
}
