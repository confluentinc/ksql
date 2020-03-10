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
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import java.util.Set;

@Immutable
abstract class StructuredDataSource<K> implements DataSource {

  private final SourceName dataSourceName;
  private final DataSourceType dataSourceType;
  private final LogicalSchema schema;
  private final KeyField keyField;
  private final Optional<TimestampColumn> timestampColumn;
  private final KsqlTopic ksqlTopic;
  private final String sqlExpression;
  private final ImmutableSet<SerdeOption> serdeOptions;
  private final boolean casTarget;

  StructuredDataSource(
      final String sqlExpression,
      final SourceName dataSourceName,
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final KeyField keyField,
      final Optional<TimestampColumn> tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final boolean casTarget,
      final KsqlTopic ksqlTopic
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = requireNonNull(schema, "schema");
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.timestampColumn = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.casTarget = casTarget;

    if (schema.valueContainsAny(SchemaUtil.systemColumnNames())) {
      throw new IllegalArgumentException("Schema contains system columns in value schema");
    }
  }

  @Override
  public SourceName getName() {
    return dataSourceName;
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
  public boolean isCasTarget() {
    return casTarget;
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  @Override
  public String getKafkaTopicName() {
    return ksqlTopic.getKafkaTopicName();
  }

  @Override
  public String getSqlExpression() {
    return sqlExpression;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }
}
