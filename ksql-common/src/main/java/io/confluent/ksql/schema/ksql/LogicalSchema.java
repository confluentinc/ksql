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
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Immutable KSQL logical schema.
 */
@Immutable
public final class LogicalSchema {

  private static final String KEY_KEYWORD = "KEY";

  private static final List<Column> METADATA_SCHEMA = ImmutableList.<Column>builder()
      .add(Column.of(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT))
      .build();

  private static final List<Column> IMPLICIT_KEY_SCHEMA = ImmutableList.<Column>builder()
      .add(Column.of(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING))
      .build();

  private final List<Column> metadata;
  private final List<Column> key;
  private final List<Column> value;

  public static Builder builder() {
    return new Builder();
  }

  private LogicalSchema(
      final List<Column> metadata,
      final List<Column> key,
      final List<Column> value
  ) {
    this.metadata = requireNonNull(metadata, "metadata");
    this.key = requireNonNull(key, "key");
    this.value = requireNonNull(value, "value");
  }

  public ConnectSchema keyConnectSchema() {
    return toConnectSchema(key);
  }

  public ConnectSchema valueConnectSchema() {
    return toConnectSchema(value);
  }

  /**
   * @return the schema of the metadata.
   */
  public List<Column> metadata() {
    return metadata;
  }

  /**
   * @return the schema of the key.
   */
  public List<Column> key() {
    return key;
  }

  /**
   * @return the schema of the value.
   */
  public List<Column> value() {
    return value;
  }

  /**
   * @return all columns in the schema.
   */
  public List<Column> columns() {
    return ImmutableList.<Column>builder()
        .addAll(metadata)
        .addAll(key)
        .addAll(value)
        .build();
  }

  /**
   * Search for a column with the supplied {@code columnName}.
   *
   * <p>If the columnName and the name of a column are an exact match, it will return that column.
   *
   * <p>If not exact match is found, any alias is stripped from the supplied  {@code columnName}
   * before attempting to find a match again.
   *
   * <p>Search order if meta, then key and then value columns.
   *
   * @param columnName the column name, where any alias is ignored.
   * @return the column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findColumn(final String columnName) {
    Optional<Column> found = doFindColumn(columnName, metadata);
    if (found.isPresent()) {
      return found;
    }

    found = doFindColumn(columnName, key);
    if (found.isPresent()) {
      return found;
    }

    return doFindColumn(columnName, value);
  }

  /**
   * Search for a value column with the supplied {@code columnName}.
   *
   * <p>If the columnName and the name of a column are an exact match, it will return that column.
   *
   * <p>If not exact match is found, any alias is stripped from the supplied  {@code columnName}
   * before attempting to find a match again.
   *
   * @param columnName the column name, where any alias is ignored.
   * @return the value column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findValueColumn(final String columnName) {
    return doFindColumn(columnName, value);
  }

  /**
   * Find the index of the column with the supplied exact {@code fullColumnName}.
   *
   * @param fullColumnName the exact name of the column to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueColumnIndex(final String fullColumnName) {
    int idx = 0;
    for (final Column column : value) {
      if (column.fullName().equals(fullColumnName)) {
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
  public LogicalSchema withAlias(final String alias) {
    if (isAliased()) {
      throw new IllegalStateException("Already aliased");
    }

    return new LogicalSchema(
        addAlias(alias, metadata),
        addAlias(alias, key),
        addAlias(alias, value)
    );
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

    return new LogicalSchema(
        removeAlias(metadata),
        removeAlias(key),
        removeAlias(value)
    );
  }

  /**
   * @return {@code true} is aliased, {@code false} otherwise.
   */
  public boolean isAliased() {
    // Either all columns are aliased, or none:
    return metadata.get(0).source().isPresent();
  }

  /**
   * Copies metadata and key columns to the value schema.
   *
   * <p>If the columns already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyColsInValue() {
    final List<Column> newValueColumns = new ArrayList<>(
        metadata.size()
            + key.size()
            + value.size());

    newValueColumns.addAll(metadata);
    newValueColumns.addAll(key);

    value.forEach(f -> {
      if (!doFindColumn(f.name(), newValueColumns).isPresent()) {
        newValueColumns.add(f);
      }
    });

    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    newValueColumns.forEach(builder::add);

    return new LogicalSchema(
        metadata,
        key,
        builder.build()
    );
  }

  /**
   * Remove metadata and key columns from the value schema.
   *
   * @return the new schema with the columns removed.
   */
  public LogicalSchema withoutMetaAndKeyColsInValue() {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    final Set<String> excluded = metaAndKeyColumnNames();

    value.stream()
        .filter(f -> !excluded.contains(f.name()))
        .forEach(builder::add);

    return new LogicalSchema(
        metadata,
        key,
        builder.build()
    );
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any metadata column.
   */
  public boolean isMetaColumn(final String columnName) {
    return metaColumnNames().contains(columnName);
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any key column.
   */
  public boolean isKeyColumn(final String columnName) {
    return keyColumnNames().contains(columnName);
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
    // Meta Columns deliberately excluded.
    return Objects.equals(key, that.key)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    // Meta columns deliberately excluded.

    final String keys = key.stream()
        .map(f -> f.toString(formatOptions) + " " + KEY_KEYWORD)
        .collect(Collectors.joining(", "));

    final String values = value.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", "));

    final String join = keys.isEmpty() || values.isEmpty()
        ? ""
        : ", ";

    return "[" + keys + join + values + "]";
  }

  private Set<String> metaColumnNames() {
    return columnNames(metadata);
  }

  private Set<String> keyColumnNames() {
    return columnNames(key);
  }

  private Set<String> metaAndKeyColumnNames() {
    final Set<String> names = metaColumnNames();
    names.addAll(keyColumnNames());
    return names;
  }

  private static Set<String> columnNames(final List<Column> struct) {
    return struct.stream()
        .map(Column::name)
        .collect(Collectors.toSet());
  }

  private static List<Column> addAlias(final String alias, final List<Column> columns) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    for (final Column col : columns) {
      builder.add(col.withSource(alias));
    }
    return builder.build();
  }

  private static List<Column> removeAlias(final List<Column> columns) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    for (final Column col : columns) {
      builder.add(Column.of(col.name(), col.type()));
    }
    return builder.build();
  }

  private static Optional<Column> doFindColumn(final String column, final List<Column> columns) {
    return columns
        .stream()
        .filter(f -> SchemaUtil.isFieldName(column, f.fullName()))
        .findFirst();
  }

  private static ConnectSchema toConnectSchema(
      final List<Column> columns
  ) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final Column column : columns) {
      final Schema colSchema = converter.toConnectSchema(column.type());
      builder.field(column.fullName(), colSchema);
    }

    return (ConnectSchema) builder.build();
  }

  public static class Builder {

    private final ImmutableList.Builder<Column> keyBuilder = ImmutableList.builder();
    private final ImmutableList.Builder<Column> valueBuilder = ImmutableList.builder();

    private final Set<String> seenKeys = new HashSet<>();
    private final Set<String> seenValues = new HashSet<>();

    public Builder keyColumn(final String columnName, final SqlType type) {
      keyColumn(Column.of(columnName, type));
      return this;
    }

    public Builder keyColumn(final Column column) {
      if (!seenKeys.add(column.fullName())) {
        throw new KsqlException("Duplicate keys found in schema: " + column);
      }
      keyBuilder.add(column);
      return this;
    }

    public Builder keyColumns(final Iterable<? extends Column> columns) {
      columns.forEach(this::keyColumn);
      return this;
    }

    public Builder valueColumn(final String columnName, final SqlType type) {
      valueColumn(Column.of(columnName, type));
      return this;
    }

    public Builder valueColumn(final String source, final String name, final SqlType type) {
      valueColumn(Column.of(source, name, type));
      return this;
    }

    public Builder valueColumn(final Column column) {
      if (!seenValues.add(column.fullName())) {
        throw new KsqlException("Duplicate values found in schema: " + column);
      }
      valueBuilder.add(column);
      return this;
    }

    public Builder valueColumns(final Iterable<? extends Column> column) {
      column.forEach(this::valueColumn);
      return this;
    }

    public LogicalSchema build() {
      final List<Column> suppliedKey = keyBuilder.build();

      final List<Column> key = suppliedKey.isEmpty()
          ? IMPLICIT_KEY_SCHEMA
          : suppliedKey;

      return new LogicalSchema(
          METADATA_SCHEMA,
          key,
          valueBuilder.build()
      );
    }
  }
}