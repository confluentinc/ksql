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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;

public interface DataSource<K> {

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

  /**
   * @return the name of the data source.
   */
  String getName();

  /**
   * @return the type of the data source.
   */
  DataSourceType getDataSourceType();

  /**
   * Get the logical schema of the source.
   *
   * <p>The logical schema is the schema used by KSQL when building queries.
   *
   * @return the physical schema.
   */
  LogicalSchema getSchema();

  /**
   * Get the physical serde options of the source.
   *
   * <p>These options can be combined with the logical schema to build the {@link PhysicalSchema} of
   * the source.
   *
   * @return the source's serde options.
   */
  Set<SerdeOption> getSerdeOptions();

  /**
   * @return the key field of the source.
   */
  KeyField getKeyField();

  /**
   * @return the topic backing the source.
   */
  KsqlTopic getKsqlTopic();

  /**
   * @return the serde factory for the source.
   */
  SerdeFactory<K> getKeySerdeFactory();

  /**
   * @return the value format info.
   */
  KsqlSerdeFactory getValueSerdeFactory();

  /**
   * The timestamp extraction policy of the source.
   *
   * <p>This is controlled by the {@link io.confluent.ksql.ddl.DdlConfig#TIMESTAMP_NAME_PROPERTY}
   * and {@link io.confluent.ksql.ddl.DdlConfig#TIMESTAMP_FORMAT_PROPERTY} properties set in the
   * WITH clause.
   *
   * @return the timestamp extraction policy of the source.
   */
  TimestampExtractionPolicy getTimestampExtractionPolicy();

  /**
   * @return the name of the KSQL REGISTERED TOPIC backing this source.
   */
  String getKsqlTopicName();

  /**
   * @return the name of the KAFKA topic backing this source.
   */
  String getKafkaTopicName();

  /**
   * @return the SQL statement used to create this source.
   */
  String getSqlExpression();
}
