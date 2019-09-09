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
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
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

  private static final SqlStruct METADATA_SCHEMA = SqlTypes.struct()
      .field(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
      .build();

  private static final SqlStruct IMPLICIT_KEY_SCHEMA = SqlTypes.struct()
      .field(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
      .build();

  private final SqlStruct metadata;
  private final SqlStruct key;
  private final SqlStruct value;

  public static Builder builder() {
    return new Builder();
  }

  private LogicalSchema(
      final SqlStruct metadata,
      final SqlStruct key,
      final SqlStruct value
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
  public SqlStruct metadata() {
    return metadata;
  }

  /**
   * @return the schema of the key.
   */
  public SqlStruct key() {
    return key;
  }

  /**
   * @return the schema of the value.
   */
  public SqlStruct value() {
    return value;
  }

  /**
   * @return all fields in the schema.
   */
  public List<Field> fields() {
    return ImmutableList.<Field>builder()
        .addAll(metadata.fields())
        .addAll(key.fields())
        .addAll(value.fields())
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
  public Optional<Field> findField(final String fieldName) {
    Optional<Field> found = doFindField(fieldName, metadata.fields());
    if (found.isPresent()) {
      return found;
    }

    found = doFindField(fieldName, key.fields());
    if (found.isPresent()) {
      return found;
    }

    return doFindField(fieldName, value.fields());
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
  public Optional<Field> findValueField(final String fieldName) {
    return doFindField(fieldName, value.fields());
  }

  /**
   * Find the index of the field with the supplied exact {@code fullFieldName}.
   *
   * @param fullFieldName the exact name of the field to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueFieldIndex(final String fullFieldName) {
    int idx = 0;
    for (final Field field : value.fields()) {
      if (field.fullName().equals(fullFieldName)) {
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
    return metadata.fields().get(0).fieldName().source().isPresent();
  }

  /**
   * Copies metadata and key fields to the value schema.
   *
   * <p>If the fields already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyFieldsInValue() {
    final List<Field> newValueFields = new ArrayList<>(
        metadata.fields().size()
            + key.fields().size()
            + value.fields().size());

    newValueFields.addAll(metadata.fields());
    newValueFields.addAll(key.fields());

    value.fields().forEach(f -> {
      if (!doFindField(f.name(), newValueFields).isPresent()) {
        newValueFields.add(f);
      }
    });

    final SqlStruct.Builder builder = SqlTypes.struct();
    newValueFields.forEach(builder::field);

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
    final SqlStruct.Builder builder = SqlTypes.struct();

    final Set<String> excluded = metaAndKeyFieldNames();

    value.fields().stream()
        .filter(f -> !excluded.contains(f.name()))
        .forEach(builder::field);

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
    return metaFieldNames().contains(fieldName);
  }

  /**
   * @param fieldName the field name to check
   * @return {@code true} if the field matches the name of any key field.
   */
  public boolean isKeyField(final String fieldName) {
    return keyFieldNames().contains(fieldName);
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

    final String keys = key.fields().stream()
        .map(f -> f.toString(formatOptions) + " " + KEY_KEYWORD)
        .collect(Collectors.joining(", "));

    final String values = value.fields().stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", "));

    final String join = keys.isEmpty() || values.isEmpty()
        ? ""
        : ", ";

    return "[" + keys + join + values + "]";
  }

  private Set<String> metaFieldNames() {
    return fieldNames(metadata);
  }

  private Set<String> keyFieldNames() {
    return fieldNames(key);
  }

  private Set<String> metaAndKeyFieldNames() {
    final Set<String> names = metaFieldNames();
    names.addAll(keyFieldNames());
    return names;
  }

  private static Set<String> fieldNames(final SqlStruct struct) {
    return struct.fields().stream()
        .map(Field::name)
        .collect(Collectors.toSet());
  }

  private static SqlStruct addAlias(final String alias, final SqlStruct struct) {
    final SqlStruct.Builder builder = SqlTypes.struct();

    for (final Field field : struct.fields()) {
      builder.field(field.withSource(alias));
    }
    return builder.build();
  }

  private static SqlStruct removeAlias(final SqlStruct struct) {
    final SqlStruct.Builder builder = SqlTypes.struct();

    for (final Field field : struct.fields()) {
      builder.field(Field.of(field.name(), field.type()));
    }
    return builder.build();
  }

  private static Optional<Field> doFindField(final String fieldName, final List<Field> fields) {
    return fields
        .stream()
        .filter(f -> SchemaUtil.isFieldName(fieldName, f.fullName()))
        .findFirst();
  }

  private static ConnectSchema toConnectSchema(
      final SqlStruct struct
  ) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final Field field : struct.fields()) {

      final Schema fieldSchema = converter.toConnectSchema(field.type());
      builder.field(field.fullName(), fieldSchema);
    }

    return (ConnectSchema) builder.build();
  }

  public static class Builder {

    private final SqlStruct.Builder keyBuilder = SqlTypes.struct();
    private final SqlStruct.Builder valueBuilder = SqlTypes.struct();

    public Builder keyField(final String fieldName, final SqlType fieldType) {
      keyField(Field.of(fieldName, fieldType));
      return this;
    }

    public Builder keyField(final Field field) {
      keyBuilder.field(field);
      return this;
    }

    public Builder keyFields(final Iterable<? extends Field> fields) {
      fields.forEach(this::keyField);
      return this;
    }

    public Builder valueField(final String fieldName, final SqlType fieldType) {
      valueField(Field.of(fieldName, fieldType));
      return this;
    }

    public Builder valueField(final Field field) {
      valueBuilder.field(field);
      return this;
    }

    public Builder valueFields(final Iterable<? extends Field> fields) {
      fields.forEach(this::valueField);
      return this;
    }

    public LogicalSchema build() {
      final SqlStruct suppliedKey = keyBuilder.build();

      final SqlStruct key = suppliedKey.fields().isEmpty()
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