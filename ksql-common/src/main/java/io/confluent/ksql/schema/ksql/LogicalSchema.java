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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
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

  private static final NamespacedColumn IMPLICIT_TIME_COLUMN = NamespacedColumn.of(
      Column.of(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT),
      Namespace.META
  );

  private static final NamespacedColumn IMPLICIT_KEY_COLUMN = NamespacedColumn.of(
      Column.of(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING),
      Namespace.KEY
  );

  private final ImmutableList<NamespacedColumn> columns;

  public static Builder builder() {
    return new Builder();
  }

  private LogicalSchema(final ImmutableList<NamespacedColumn> columns) {
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
    return byNamespace(NamespacedColumn::column)
        .get(Namespace.META);
  }

  /**
   * @return the schema of the key.
   */
  public List<Column> key() {
    return byNamespace(NamespacedColumn::column)
        .get(Namespace.KEY);
  }

  /**
   * @return the schema of the value.
   */
  public List<Column> value() {
    return byNamespace(NamespacedColumn::column)
        .get(Namespace.VALUE);
  }

  /**
   * @return all columns in the schema.
   */
  public List<Column> columns() {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    columns.forEach(c -> builder.add(c.column()));
    return builder.build();
  }

  /**
   * Search for a column with the supplied {@code columnRef}.
   *
   * @param columnRef the column source and name to match.
   * @return the column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findColumn(final ColumnRef columnRef) {
    return findNamespacedColumn(withRef(columnRef))
        .map(NamespacedColumn::column);
  }

  /**
   * Search for a value column with the supplied {@code columnRef}.
   *
   * @param columnRef the column source and name to match.
   * @return the value column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findValueColumn(final ColumnRef columnRef) {
    return findNamespacedColumn(withNamespace(Namespace.VALUE).and(withRef(columnRef)))
        .map(NamespacedColumn::column);
  }

  /**
   * Find the index of the column with the supplied exact {@code target}.
   *
   * @param target the exact name of the column to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueColumnIndex(final ColumnRef target) {
    int idx = 0;
    for (final Column column : value()) {
      if (column.ref().equals(target)) {
        return OptionalInt.of(idx);
      }
      ++idx;
    }

    return OptionalInt.empty();
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
    if (isAliased()) {
      throw new IllegalStateException("Already aliased");
    }

    final ImmutableList.Builder<NamespacedColumn> builder = ImmutableList.builder();
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
    if (!isAliased()) {
      throw new IllegalStateException("Not aliased");
    }

    final ImmutableList.Builder<NamespacedColumn> builder = ImmutableList.builder();
    columns.stream()
        .map(NamespacedColumn::noSource)
        .forEach(builder::add);

    return new LogicalSchema(builder.build());
  }

  /**
   * Returns a copy of this schema with not meta columns.
   *
   * <p>The order of columns is maintained
   *
   * @return the new schema.
   */
  public LogicalSchema withoutMetaColumns() {
    final Builder builder = builder()
        .noImplicitColumns();

    columns.stream()
        .filter(col -> col.namespace() != Namespace.META)
        .forEachOrdered(builder::addColumn);

    return builder.build();
  }

  /**
   * @return {@code true} is aliased, {@code false} otherwise.
   */
  public boolean isAliased() {
    // Either all columns are aliased, or none:
    return columns.get(0).column().source().isPresent();
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
    return findNamespacedColumn(withNamespace(Namespace.META).and(withName(columnName)))
        .isPresent();
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any key column.
   */
  public boolean isKeyColumn(final ColumnName columnName) {
    return findNamespacedColumn(withNamespace(Namespace.KEY).and(withName(columnName)))
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
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private Optional<NamespacedColumn> findNamespacedColumn(
      final Predicate<NamespacedColumn> predicate
  ) {
    return columns.stream()
        .filter(predicate)
        .findFirst();
  }

  private <T> Map<Namespace, List<T>> byNamespace(final Function<NamespacedColumn, T> mapper) {
    final Map<Namespace, List<T>> byNamespace = columns.stream()
        .collect(Collectors.groupingBy(
            NamespacedColumn::namespace,
            Collectors.mapping(mapper, Collectors.toList())
        ));

    Arrays.stream(Namespace.values())
        .forEach(ns -> byNamespace.putIfAbsent(ns, ImmutableList.of()));

    return byNamespace;
  }

  private LogicalSchema rebuild(final boolean withMetaAndKeyColsInValue) {
    final Map<Namespace, List<NamespacedColumn>> byNamespace = byNamespace(Function.identity());

    final List<NamespacedColumn> metadata = byNamespace.get(Namespace.META);
    final List<NamespacedColumn> key = byNamespace.get(Namespace.KEY);
    final List<NamespacedColumn> value = byNamespace.get(Namespace.VALUE);

    final ImmutableList.Builder<NamespacedColumn> builder = ImmutableList.builder();

    builder.addAll(metadata);
    builder.addAll(key);

    if (withMetaAndKeyColsInValue) {
      metadata.stream()
          .map(c -> NamespacedColumn.of(c.column(), Namespace.VALUE))
          .forEach(builder::add);

      key.stream()
          .map(c -> NamespacedColumn.of(c.column(), Namespace.VALUE))
          .forEach(builder::add);
    }

    value.stream()
        .filter(c -> !findNamespacedColumn(
            (withNamespace(Namespace.META).or(withNamespace(Namespace.KEY))
                .and(withRef(c.column().ref()))
            )).isPresent())
        .forEach(builder::add);

    return new LogicalSchema(builder.build());
  }

  private static Predicate<NamespacedColumn> withRef(final ColumnRef ref) {
    return c -> c.column().ref().equals(ref);
  }

  private static Predicate<NamespacedColumn> withName(final ColumnName name) {
    return c -> c.column().name().equals(name);
  }

  private static Predicate<NamespacedColumn> withNamespace(final Namespace ns) {
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

    private final ImmutableList.Builder<NamespacedColumn> explicitColumns = ImmutableList.builder();

    private final Set<ColumnRef> seenKeys = new HashSet<>();
    private final Set<ColumnRef> seenValues = new HashSet<>();

    private boolean addImplicitRowKey = true;
    private boolean addImplicitRowTime = true;

    public Builder noImplicitColumns() {
      addImplicitRowKey = false;
      addImplicitRowTime = false;
      return this;
    }

    public Builder keyColumn(final ColumnName columnName, final SqlType type) {
      keyColumn(Column.of(columnName, type));
      return this;
    }

    public Builder keyColumn(final Column column) {
      addColumn(NamespacedColumn.of(column, Namespace.KEY));
      return this;
    }

    public Builder keyColumns(final Iterable<? extends Column> columns) {
      columns.forEach(this::keyColumn);
      return this;
    }

    public Builder valueColumn(final ColumnName columnName, final SqlType type) {
      valueColumn(Column.of(columnName, type));
      return this;
    }

    public Builder valueColumn(final SourceName source, final ColumnName name, final SqlType type) {
      valueColumn(Column.of(source, name, type));
      return this;
    }

    public Builder valueColumn(final Column column) {
      addColumn(NamespacedColumn.of(column, Namespace.VALUE));
      return this;
    }

    public Builder valueColumns(final Iterable<? extends Column> column) {
      column.forEach(this::valueColumn);
      return this;
    }

    public LogicalSchema build() {
      final ImmutableList.Builder<NamespacedColumn> allColumns = ImmutableList.builder();

      if (addImplicitRowTime) {
        allColumns.add(IMPLICIT_TIME_COLUMN);
      }

      if (addImplicitRowKey) {
        allColumns.add(IMPLICIT_KEY_COLUMN);
      }

      allColumns.addAll(explicitColumns.build());

      return new LogicalSchema(allColumns.build());
    }

    private void addColumn(final NamespacedColumn column) {
      switch (column.namespace()) {
        case KEY:
          if (!seenKeys.add(column.column().ref())) {
            throw new KsqlException("Duplicate keys found in schema: " + column);
          }
          addImplicitRowKey = false;
          break;

        case VALUE:
          if (!seenValues.add(column.column().ref())) {
            throw new KsqlException("Duplicate values found in schema: " + column);
          }
          break;

        default:
          break;
      }

      explicitColumns.add(column);
    }
  }

  private enum Namespace {
    META,
    KEY,
    VALUE
  }

  @Immutable
  private static final class NamespacedColumn {

    private final Column column;
    private final Namespace namespace;

    static NamespacedColumn of(
        final Column column,
        final Namespace namespace
    ) {
      return new NamespacedColumn(column, namespace);
    }

    private NamespacedColumn(
        final Column column,
        final Namespace namespace
    ) {
      this.column = requireNonNull(column, "column");
      this.namespace = requireNonNull(namespace, "namespace");
    }

    Column column() {
      return column;
    }

    Namespace namespace() {
      return namespace;
    }

    NamespacedColumn withSource(final SourceName sourceName) {
      return NamespacedColumn.of(column.withSource(sourceName), namespace);
    }

    NamespacedColumn noSource() {
      return NamespacedColumn.of(Column.of(column.name(), column.type()), namespace);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final NamespacedColumn that = (NamespacedColumn) o;
      return Objects.equals(column, that.column)
          && namespace == that.namespace;
    }

    @Override
    public int hashCode() {
      return Objects.hash(column, namespace);
    }

    @Override
    public String toString() {
      return toString(FormatOptions.none());
    }

    public String toString(final FormatOptions formatOptions) {
      final String postFix = namespace == Namespace.VALUE
          ? ""
          : " " + namespace;

      return column.toString(formatOptions) + postFix;
    }
  }
}