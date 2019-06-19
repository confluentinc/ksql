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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;

@Immutable
abstract class StructuredDataSource<K> implements DataSource<K> {

  private final String dataSourceName;
  private final DataSourceType dataSourceType;
  private final LogicalSchema schema;
  private final KeyField keyField;
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final SerdeFactory<K> keySerde;
  private final KsqlTopic ksqlTopic;
  private final String sqlExpression;
  private final ImmutableSet<SerdeOption> serdeOptions;

  StructuredDataSource(
      final String sqlExpression,
      final String dataSourceName,
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final KeyField keyField,
      final TimestampExtractionPolicy tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final KsqlTopic ksqlTopic,
      final SerdeFactory<K> keySerde
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = requireNonNull(schema, "schema");
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.timestampExtractionPolicy = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.keySerde = requireNonNull(keySerde, "keySerde");
    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));

    if (!schema.withImplicitAndKeyFieldsInValue().equals(schema)) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public String getName() {
    return this.dataSourceName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public KsqlTopic getKsqlTopic() {
    return ksqlTopic;
  }

  @Override
  public SerdeFactory<K> getKeySerdeFactory() {
    return keySerde;
  }

  @Override
  public KsqlSerdeFactory getValueSerdeFactory() {
    return ksqlTopic.getValueSerdeFactory();
  }

  @Override
  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  @Override
  public String getKsqlTopicName() {
    return ksqlTopic.getKsqlTopicName();
  }

  @Override
  public String getKafkaTopicName() {
    return ksqlTopic.getKafkaTopicName();
  }

  @Override
  public String getSqlExpression() {
    return sqlExpression;
  }
}
