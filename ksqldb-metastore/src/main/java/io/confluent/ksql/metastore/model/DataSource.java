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

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Optional;

@EffectivelyImmutable
public interface DataSource {

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
  SourceName getName();

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
   * @return the topic backing the source.
   */
  KsqlTopic getKsqlTopic();

  /**
   * The timestamp extraction policy of the source.
   *
   * <p>This is controlled by the
   * {@link io.confluent.ksql.properties.with.CommonCreateConfigs#TIMESTAMP_NAME_PROPERTY}
   * and {@link io.confluent.ksql.properties.with.CommonCreateConfigs#TIMESTAMP_FORMAT_PROPERTY}
   * properties set in the WITH clause.
   *
   * @return the timestamp extraction policy of the source.
   */
  Optional<TimestampColumn> getTimestampColumn();

  /**
   * @return the name of the KAFKA topic backing this source.
   */
  String getKafkaTopicName();

  /**
   * @return the SQL statement used to create this source.
   */
  String getSqlExpression();

  /**
   * @return returns whether this stream/table was created by a C(T|S)AS
   */
  boolean isCasTarget();

  /**
   * @param other the other data source
   * @return an optional, empty if compatible or an explanation if incompatible
   */
  Optional<String> canUpgradeTo(DataSource other);

  /**
   * @param sql a sql statement to append to the current sql
   * @param schema a schema
   * @return a new DataSource object with all attributes the same as this, but with a new schema
   */
  DataSource with(String sql, LogicalSchema schema);

  /**
   * @return returns true if this source is read-only
   */
  boolean isSource();
}
