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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Immutable
abstract class StructuredDataSource<K> implements DataSource {

  private final SourceName dataSourceName;
  private final DataSourceType dataSourceType;
  private final LogicalSchema schema;
  private final Optional<TimestampColumn> timestampColumn;
  private final KsqlTopic ksqlTopic;
  private final String sqlExpression;
  private final boolean casTarget;
  private final boolean isSource;

  private static final ImmutableList<Property<?>> PROPERTIES = ImmutableList.of(
      new Property<>("name", DataSource::getName),
      new Property<>("type", DataSource::getDataSourceType),
      new Property<>("topic", DataSource::getKsqlTopic),
      new Property<>("timestampColumn", DataSource::getTimestampColumn)
  );
  private static final Property<LogicalSchema> SCHEMA_PROP =
      new Property<>("schema", DataSource::getSchema);

  StructuredDataSource(
      final String sqlExpression,
      final SourceName dataSourceName,
      final LogicalSchema schema,
      final Optional<TimestampColumn> tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final boolean casTarget,
      final KsqlTopic ksqlTopic,
      final boolean isSource
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = requireNonNull(schema, "schema");
    this.timestampColumn = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.ksqlTopic = requireNonNull(ksqlTopic, "ksqlTopic");
    this.casTarget = casTarget;
    this.isSource = isSource;

    final Set<ColumnName> keyNames = schema.key().stream()
        .map(Column::name)
        .collect(Collectors.toSet());

    if (schema.valueContainsAny(keyNames)) {
      throw new IllegalArgumentException("Schema contains duplicate column names");
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
  public boolean isSource() {
    return isSource;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }

  @Override
  public Optional<String> canUpgradeTo(final DataSource other) {
    final List<String> issues = PROPERTIES.stream()
        .filter(prop -> !prop.isCompatible(this, other))
        .map(prop -> getCompatMessage(other, prop))
        .collect(Collectors.toList());

    checkSchemas(getSchema(), other.getSchema())
        .map(s -> getCompatMessage(other, SCHEMA_PROP) + ". (" + s + ")")
        .ifPresent(issues::add);

    final String err = String.join("\n\tAND ", issues);
    return err.isEmpty() ? Optional.empty() : Optional.of(err);
  }

  private String getCompatMessage(
      final DataSource other,
      final Property<?> prop
  ) {
    return String.format(
        "DataSource '%s' has %s = %s which is not upgradeable to %s",
        getName(),
        prop.name,
        prop.getter.apply(this).toString(),
        prop.getter.apply(other).toString()
    );
  }

  @VisibleForTesting
  static Optional<String> checkSchemas(
      final LogicalSchema schema,
      final LogicalSchema other
  ) {
    final Optional<String> keyError = checkSchemas(schema.key(), other.key(), "key ")
        .map(msg -> "Key columns must be identical. " + msg);
    if (keyError.isPresent()) {
      return keyError;
    }

    return checkSchemas(schema.columns(), other.columns(), "");
  }

  private static Optional<String> checkSchemas(
      final List<Column> cols,
      final List<Column> otherCols,
      final String colType
  ) {
    // We compare sets of Column objects in case the Column objects themselves are reordered
    // (which is fine), but the actual columns themselves may not be reordered -- the set
    // comparison fails in that case as Column objects store the index of the column in the schema
    final ImmutableSet<Column> colA = ImmutableSet.copyOf(cols);
    final ImmutableSet<Column> colB = ImmutableSet.copyOf(otherCols);

    final SetView<Column> difference = Sets.difference(colA, colB);
    if (!difference.isEmpty()) {
      return Optional.of("The following " + colType + "columns are changed, missing or reordered: "
          + difference);
    }

    return Optional.empty();
  }

  @Immutable
  private static class Property<T> {

    final String name;
    @EffectivelyImmutable
    final Function<DataSource, T> getter;

    Property(final String name, final Function<DataSource, T> getter) {
      this.name = requireNonNull(name, "name");
      this.getter = requireNonNull(getter, "getter");
    }

    public boolean isCompatible(final DataSource source, final DataSource other) {
      return getter.apply(source).equals(getter.apply(other));
    }
  }
}
