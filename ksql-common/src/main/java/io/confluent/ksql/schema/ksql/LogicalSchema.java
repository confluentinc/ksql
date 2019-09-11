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
   * @return all fields in the schema.
   */
  public List<Column> fields() {
    return ImmutableList.<Column>builder()
        .addAll(metadata)
        .addAll(key)
        .addAll(value)
        .build();
  }

  /**
   * Search for a field with the supplied {@code fieldName}.
   *
   * <p>If the fieldName and the name of a field are an exact match, it will return that field.
   *
   * <p>If not exact match is found, any alias is stripped from the supplied  {@code fieldName}
   * before attempting to find a match again.
   *
   * <p>Search order if meta, then key and then value fields.
   *
   * @param fieldName the field name, where any alias is ignored.
   * @return the field if found, else {@code Optiona.empty()}.
   */
  public Optional<Column> findField(final String fieldName) {
    Optional<Column> found = doFindColumn(fieldName, metadata);
    if (found.isPresent()) {
      return found;
    }

    found = doFindColumn(fieldName, key);
    if (found.isPresent()) {
      return found;
    }

    return doFindColumn(fieldName, value);
  }

  /**
   * Search for a value field with the supplied {@code fieldName}.
   *
   * <p>If the fieldName and the name of a field are an exact match, it will return that field.
   *
   * <p>If not exact match is found, any alias is stripped from the supplied  {@code fieldName}
   * before attempting to find a match again.
   *
   * @param fieldName the field name, where any alias is ignored.
   * @return the value field if found, else {@code Optiona.empty()}.
   */
  public Optional<Column> findValueField(final String fieldName) {
    return doFindColumn(fieldName, value);
  }

  /**
   * Find the index of the field with the supplied exact {@code fullColumnName}.
   *
   * @param fullColumnName the exact name of the field to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueFieldIndex(final String fullColumnName) {
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
   * Add the supplied {@code alias} to each field.
   *
   * <p>If the fields are already aliased with this alias this is a no-op.
   *
   * <p>If the fields are already aliased with a different alias the field prefixed again.
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
   * Strip any alias from the field name.
   *
   * @return the schema without any aliases in the field name.
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
    // Either all fields are aliased, or none:
    return metadata.get(0).source().isPresent();
  }

  /**
   * Copies metadata and key fields to the value schema.
   *
   * <p>If the fields already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyFieldsInValue() {
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
   * Remove metadata and key fields from the value schema.
   *
   * @return the new schema with the fields removed.
   */
  public LogicalSchema withoutMetaAndKeyFieldsInValue() {
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
   * @param fieldName the field name to check
   * @return {@code true} if the field matches the name of any metadata field.
   */
  public boolean isMetaField(final String fieldName) {
    return metaColumnNames().contains(fieldName);
  }

  /**
   * @param fieldName the field name to check
   * @return {@code true} if the field matches the name of any key field.
   */
  public boolean isKeyField(final String fieldName) {
    return keyColumnNames().contains(fieldName);
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
    // Meta fields deliberately excluded.
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
    // Meta fields deliberately excluded.

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
    return fieldNames(metadata);
  }

  private Set<String> keyColumnNames() {
    return fieldNames(key);
  }

  private Set<String> metaAndKeyColumnNames() {
    final Set<String> names = metaColumnNames();
    names.addAll(keyColumnNames());
    return names;
  }

  private static Set<String> fieldNames(final List<Column> struct) {
    return struct.stream()
        .map(Column::name)
        .collect(Collectors.toSet());
  }

  private static List<Column> addAlias(final String alias, final List<Column> struct) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    for (final Column field : struct) {
      builder.add(field.withSource(alias));
    }
    return builder.build();
  }

  private static List<Column> removeAlias(final List<Column> struct) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    for (final Column field : struct) {
      builder.add(Column.of(field.name(), field.type()));
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
      final Schema fieldSchema = converter.toConnectSchema(column.type());
      builder.field(column.fullName(), fieldSchema);
    }

    return (ConnectSchema) builder.build();
  }

  public static class Builder {

    private final ImmutableList.Builder<Column> keyBuilder = ImmutableList.builder();
    private final ImmutableList.Builder<Column> valueBuilder = ImmutableList.builder();

    private final Set<String> seenKeys = new HashSet<>();
    private final Set<String> seenValues = new HashSet<>();

    public Builder keyField(final String fieldName, final SqlType fieldType) {
      keyField(Column.of(fieldName, fieldType));
      return this;
    }

    public Builder keyField(final Column field) {
      if (!seenKeys.add(field.fullName())) {
        throw new KsqlException("Duplicate keys found in schema: " + field);
      }
      keyBuilder.add(field);
      return this;
    }

    public Builder keyFields(final Iterable<? extends Column> fields) {
      fields.forEach(this::keyField);
      return this;
    }

    public Builder valueField(final String fieldName, final SqlType fieldType) {
      valueField(Column.of(fieldName, fieldType));
      return this;
    }

    public Builder valueField(final String source, final String name, final SqlType type) {
      valueField(Column.of(source, name, type));
      return this;
    }

    public Builder valueField(final Column field) {
      if (!seenValues.add(field.fullName())) {
        throw new KsqlException("Duplicate values found in schema: " + field);
      }
      valueBuilder.add(field);
      return this;
    }

    public Builder valueFields(final Iterable<? extends Column> fields) {
      fields.forEach(this::valueField);
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