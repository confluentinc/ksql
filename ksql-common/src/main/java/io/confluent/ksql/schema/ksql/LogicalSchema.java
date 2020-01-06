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

package io.confluent.ksql.schema.ksql;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Immutable KSQL logical schema.
 */
@Immutable
public final class LogicalSchema {

  private static final Column IMPLICIT_TIME_COLUMN = Column
      .of(Optional.empty(), SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT, Column.Namespace.META, 0);

  private static final Column IMPLICIT_KEY_COLUMN = Column
      .of(Optional.empty(), SchemaUtil.ROWKEY_NAME, SqlTypes.STRING, Column.Namespace.KEY, 0);

  private final ImmutableList<Column> columns;

  public static Builder builder() {
    return new Builder();
  }

  private LogicalSchema(final ImmutableList<Column> columns) {
    this.columns = Objects.requireNonNull(columns, "columns");
  }

  public ConnectSchema keyConnectSchema() {
    return toConnectSchema(key());
  }

  public ConnectSchema valueConnectSchema() {
    return toConnectSchema(value());
  }

  /**
   * @return the schema of the metadata.
   */
  public List<Column> metadata() {
    return byNamespace()
        .get(Namespace.META);
  }

  /**
   * @return the schema of the key.
   */
  public List<Column> key() {
    return byNamespace()
        .get(Namespace.KEY);
  }

  /**
   * @return the schema of the value.
   */
  public List<Column> value() {
    return byNamespace()
        .get(Namespace.VALUE);
  }

  /**
   * @return all columns in the schema.
   */
  public List<Column> columns() {
    return columns;
  }

  /**
   * Search for a column with the supplied {@code columnRef}.
   *
   * @param columnRef the column source and name to match.
   * @return the column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findColumn(final ColumnRef columnRef) {
    return findColumnMatching(withRef(columnRef));
  }

  /**
   * Search for a value column with the supplied {@code columnRef}.
   *
   * @param columnRef the column source and name to match.
   * @return the value column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findValueColumn(final ColumnRef columnRef) {
    return findColumnMatching(withNamespace(Namespace.VALUE).and(withRef(columnRef)));
  }

  /**
   * Add the supplied {@code alias} to each column.
   *
   * <p>If the columns are already aliased with this alias this is a no-op.
   *
   * <p>If the columns are already aliased with a different alias the column prefixed again.
   *
   * @param alias the alias to add.
   * @return the schema with the alias applied.
   */
  public LogicalSchema withAlias(final SourceName alias) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    columns.stream()
        .map(c -> c.withSource(alias))
        .forEach(builder::add);

    return new LogicalSchema(builder.build());
  }

  /**
   * Strip any alias from the column name.
   *
   * @return the schema without any aliases in the column name.
   */
  public LogicalSchema withoutAlias() {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    columns.stream()
        .map(Column::withoutSource)
        .forEach(builder::add);

    return new LogicalSchema(builder.build());
  }

  /**
   * Copies metadata and key columns to the value schema.
   *
   * <p>If the columns already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyColsInValue() {
    return rebuild(true);
  }

  /**
   * Remove metadata and key columns from the value schema.
   *
   * @return the new schema with the columns removed.
   */
  public LogicalSchema withoutMetaAndKeyColsInValue() {
    return rebuild(false);
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any metadata column.
   */
  public boolean isMetaColumn(final ColumnName columnName) {
    return findColumnMatching(withNamespace(Namespace.META).and(withName(columnName)))
        .isPresent();
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any key column.
   */
  public boolean isKeyColumn(final ColumnName columnName) {
    return findColumnMatching(withNamespace(Namespace.KEY).and(withName(columnName)))
        .isPresent();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LogicalSchema that = (LogicalSchema) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    // Meta columns deliberately excluded.

    return columns.stream()
        .filter(withNamespace(Namespace.META).negate())
        .map(c -> c.toString(formatOptions))
        .collect(Collectors.joining(", "));
  }

  private Optional<Column> findColumnMatching(final Predicate<Column> predicate) {
    // At the moment, it's possible for some column names to have multiple matches, e.g.
    // ROWKEY and ROWTIME. Order of preference on namespace is KEY, VALUE then META,
    // as per Namespace enum ordinal.

    return columns.stream()
        .filter(predicate)
        .min(Comparator.comparingInt(c -> c.namespace().ordinal()));
  }

  private Map<Namespace, List<Column>> byNamespace() {
    final Map<Namespace, List<Column>> byNamespace = columns.stream()
        .collect(Collectors.groupingBy(Column::namespace));

    Arrays.stream(Namespace.values())
        .forEach(ns -> byNamespace.putIfAbsent(ns, ImmutableList.of()));

    return byNamespace;
  }

  private LogicalSchema rebuild(final boolean withMetaAndKeyColsInValue) {
    final Map<Namespace, List<Column>> byNamespace = byNamespace();

    final List<Column> metadata = byNamespace.get(Namespace.META);
    final List<Column> key = byNamespace.get(Namespace.KEY);
    final List<Column> value = byNamespace.get(Namespace.VALUE);

    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    builder.addAll(metadata);
    builder.addAll(key);

    int valueIndex = 0;
    if (withMetaAndKeyColsInValue) {
      for (final Column c : metadata) {
        builder.add(Column.of(c.source(), c.name(), c.type(), Namespace.VALUE, valueIndex++));
      }

      for (final Column c : key) {
        builder.add(Column.of(c.source(), c.name(), c.type(), Namespace.VALUE, valueIndex++));
      }
    }

    for (final Column c : value) {
      if (findColumnMatching(
          (withNamespace(Namespace.META).or(withNamespace(Namespace.KEY))
              .and(withRef(c.ref()))
          )).isPresent()) {
        continue;
      }

      builder.add(Column.of(c.source(), c.name(), c.type(), Namespace.VALUE, valueIndex++));
    }

    return new LogicalSchema(builder.build());
  }

  private static Predicate<Column> withRef(final ColumnRef ref) {
    return c -> c.ref().equals(ref);
  }

  private static Predicate<Column> withName(final ColumnName name) {
    return c -> c.name().equals(name);
  }

  private static Predicate<Column> withNamespace(final Namespace ns) {
    return c -> c.namespace() == ns;
  }

  private static ConnectSchema toConnectSchema(
      final List<Column> columns
  ) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final Column column : columns) {
      final Schema colSchema = converter.toConnectSchema(column.type());
      builder.field(column.ref().aliasedFieldName(), colSchema);
    }

    return (ConnectSchema) builder.build();
  }

  public static class Builder {

    private final ImmutableList.Builder<Column> explicitColumns = ImmutableList.builder();

    private final Set<ColumnRef> seenKeys = new HashSet<>();
    private final Set<ColumnRef> seenValues = new HashSet<>();

    private boolean addImplicitRowKey = true;
    private boolean addImplicitRowTime = true;

    public Builder noImplicitColumns() {
      addImplicitRowKey = false;
      addImplicitRowTime = false;
      return this;
    }

    public Builder keyColumns(final Iterable<? extends SimpleColumn> columns) {
      columns.forEach(this::keyColumn);
      return this;
    }

    public Builder keyColumn(final ColumnName columnName, final SqlType type) {
      keyColumn(Optional.empty(), columnName, type);
      return this;
    }

    public Builder keyColumn(final SourceName source, final ColumnName name, final SqlType type) {
      keyColumn(Optional.of(source), name, type);
      return this;
    }

    public Builder keyColumn(final SimpleColumn col) {
      return keyColumn(col.ref().source(), col.ref().name(), col.type());
    }

    private Builder keyColumn(
        final Optional<SourceName> source,
        final ColumnName name,
        final SqlType type
    ) {
      addColumn(Column.of(source, name, type, Column.Namespace.KEY, seenKeys.size()));
      return this;
    }

    public Builder valueColumns(final Iterable<? extends SimpleColumn> column) {
      column.forEach(this::valueColumn);
      return this;
    }

    public Builder valueColumn(final ColumnName columnName, final SqlType type) {
      valueColumn(Optional.empty(), columnName, type);
      return this;
    }

    public Builder valueColumn(final SourceName source, final ColumnName name, final SqlType type) {
      valueColumn(Optional.of(source), name, type);
      return this;
    }

    public Builder valueColumn(final SimpleColumn col) {
      return valueColumn(col.ref().source(), col.ref().name(), col.type());
    }

    private Builder valueColumn(
        final Optional<SourceName> source,
        final ColumnName name,
        final SqlType type
    ) {
      addColumn(Column.of(source, name, type, Column.Namespace.VALUE, seenValues.size()));
      return this;
    }

    public LogicalSchema build() {
      final ImmutableList.Builder<Column> allColumns = ImmutableList.builder();

      if (addImplicitRowTime) {
        allColumns.add(IMPLICIT_TIME_COLUMN);
      }

      if (addImplicitRowKey) {
        allColumns.add(IMPLICIT_KEY_COLUMN);
      }

      allColumns.addAll(explicitColumns.build());

      return new LogicalSchema(allColumns.build());
    }

    private void addColumn(final Column column) {
      switch (column.namespace()) {
        case KEY:
          if (!seenKeys.add(column.ref())) {
            throw new KsqlException("Duplicate keys found in schema: " + column);
          }
          addImplicitRowKey = false;
          break;

        case VALUE:
          if (!seenValues.add(column.ref())) {
            throw new KsqlException("Duplicate values found in schema: " + column);
          }
          break;

        default:
          break;
      }

      explicitColumns.add(column);
    }
  }
}