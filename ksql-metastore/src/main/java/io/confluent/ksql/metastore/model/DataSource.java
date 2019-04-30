/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import org.apache.kafka.connect.data.Schema;

public interface DataSource<K> {

  String getName();

  DataSourceType getDataSourceType();

  Schema getSchema();

  KeyField getKeyField();

  KsqlTopic getKsqlTopic();

  SerdeFactory<K> getKeySerdeFactory();

  KsqlTopicSerDe getKsqlTopicSerde();

  TimestampExtractionPolicy getTimestampExtractionPolicy();

  String getKsqlTopicName();

  String getKafkaTopicName();

  String getSqlExpression();

  enum DataSourceType {

    KSTREAM("STREAM"),
    KTABLE("TABLE");

    private final String ksqlType;

    DataSourceType(final String ksqlType) {
      this.ksqlType = ksqlType;
    }

    public String getKsqlType() {
      return ksqlType;
    }
  }
}
