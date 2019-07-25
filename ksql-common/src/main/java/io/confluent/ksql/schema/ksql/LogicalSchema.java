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
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToConnectTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Immutable KSQL logical schema.
 */
@Immutable
public final class LogicalSchema {

  private static final String KEY_KEYWORD = "KEY";

  private static final ImmutableList<Field> META_FIELDS = ImmutableList.of(
      Field.of(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
  );

  private static final ImmutableList<Field> IMPLICIT_KEY_FIELDS = ImmutableList.of(
      Field.of(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
  );

  private final List<Field> metaFields;
  private final List<Field> keyFields;
  private final List<Field> valueFields;

  public static Builder builder() {
    return new Builder();
  }

  public static LogicalSchema of(final Schema valueSchema) {
    final List<Field> valueFields = fromConnectSchema(valueSchema);
    return new LogicalSchema(META_FIELDS, IMPLICIT_KEY_FIELDS, valueFields);
  }

  public static LogicalSchema of(
      final Schema keySchema,
      final Schema valueSchema
  ) {
    final List<Field> keyFields = fromConnectSchema(keySchema);
    final List<Field> valueFields = fromConnectSchema(valueSchema);
    return new LogicalSchema(META_FIELDS, keyFields, valueFields);
  }

  public LogicalSchema(
      final List<Field> metaFields,
      final List<Field> keyFields,
      final List<Field> valueFields
  ) {
    this.metaFields = ImmutableList.copyOf(requireNonNull(metaFields, "metaFields"));
    this.keyFields = ImmutableList.copyOf(requireNonNull(keyFields, "keyFields"));
    this.valueFields = ImmutableList.copyOf(requireNonNull(valueFields, "valueFields"));
  }

  public ConnectSchema keySchema() {
    return toConnectSchema(keyFields);
  }

  public ConnectSchema valueSchema() {
    return toConnectSchema(valueFields);
  }

  /**
   * @return the metadata fields in the schema.
   */
  public List<Field> metaFields() {
    return metaFields;
  }

  /**
   * @return the key fields in the schema.
   */
  public List<Field> keyFields() {
    return keyFields;
  }

  /**
   * @return the value fields in the schema.
   */
  public List<Field> valueFields() {
    return valueFields;
  }

  /**
   * @return all fields in the schema.
   */
  public List<Field> fields() {
    return ImmutableList.<Field>builder()
        .addAll(metaFields)
        .addAll(keyFields)
        .addAll(valueFields)
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
    Optional<Field> found = doFindField(fieldName, metaFields);
    if (found.isPresent()) {
      return found;
    }

    found = doFindField(fieldName, keyFields);
    if (found.isPresent()) {
      return found;
    }

    return doFindField(fieldName, valueFields);
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
    return doFindField(fieldName, valueFields);
  }

  /**
   * Find the index of the field with the supplied exact {@code fullFieldName}.
   *
   * @param fullFieldName the exact name of the field to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueFieldIndex(final String fullFieldName) {
    int idx = 0;
    for (final Field field : valueFields) {
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
        addAlias(alias, metaFields),
        addAlias(alias, keyFields),
        addAlias(alias, valueFields)
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
        removeAlias(metaFields),
        removeAlias(keyFields),
        removeAlias(valueFields)
    );
  }

  /**
   * @return {@code true} is aliased, {@code false} otherwise.
   */
  public boolean isAliased() {
    return metaFields.get(0).source().isPresent();
  }

  /**
   * Copies metadata and key fields to the value schema.
   *
   * <p>If the fields already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyFieldsInValue() {
    final List<Field> newValueFields =
        new ArrayList<>(metaFields.size() + keyFields.size() + valueFields.size());

    newValueFields.addAll(metaFields);
    newValueFields.addAll(keyFields);

    valueFields().forEach(f -> {
      if (!doFindField(f.name(), newValueFields).isPresent()) {
        newValueFields.add(f);
      }
    });

    return new LogicalSchema(
        metaFields,
        keyFields,
        newValueFields
    );
  }

  /**
   * Remove metadata and key fields from the value schema.
   *
   * @return the new schema with the fields removed.
   */
  public LogicalSchema withoutMetaAndKeyFieldsInValue() {
    final ImmutableList.Builder<Field> builder = ImmutableList.builder();

    final Set<String> excluded = metaAndKeyFieldNames();

    valueFields.stream()
        .filter(f -> !excluded.contains(f.name()))
        .forEach(builder::add);

    return new LogicalSchema(
        metaFields,
        keyFields,
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
    return Objects.equals(keyFields, that.keyFields)
        && Objects.equals(valueFields, that.valueFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFields, valueFields);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    // Meta fields deliberately excluded.

    final String keys = keyFields.stream()
        .map(f -> f.toString(formatOptions) + " " + KEY_KEYWORD)
        .collect(Collectors.joining(", "));

    final String values = valueFields.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", "));

    final String join = keys.isEmpty() || values.isEmpty()
        ? ""
        : ", ";

    return "[" + keys + join + values + "]";
  }

  private Set<String> metaFieldNames() {
    return fieldNames(metaFields);
  }

  private Set<String> keyFieldNames() {
    return fieldNames(keyFields);
  }

  private Set<String> metaAndKeyFieldNames() {
    final Set<String> names = metaFieldNames();
    names.addAll(keyFieldNames());
    return names;
  }

  private static Set<String> fieldNames(final Collection<Field> fields) {
    return fields.stream()
        .map(Field::name)
        .collect(Collectors.toSet());
  }

  private static List<Field> addAlias(final String alias, final List<Field> fields) {
    final ImmutableList.Builder<Field> builder = ImmutableList.builder();

    for (final Field field : fields) {
      builder.add(field.withSource(alias));
    }
    return builder.build();
  }

  private static List<Field> removeAlias(final List<Field> fields) {
    final ImmutableList.Builder<Field> builder = ImmutableList.builder();

    for (final Field field : fields) {
      builder.add(Field.of(field.name(), field.type()));
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
      final List<Field> fields
  ) {
    final SqlToConnectTypeConverter converter = SchemaConverters.sqlToConnectConverter();

    final SchemaBuilder builder = SchemaBuilder.struct();
    for (final Field field : fields) {

      final Schema fieldSchema = converter.toConnectSchema(field.type());
      builder.field(field.fullName(), fieldSchema);
    }

    return (ConnectSchema) builder.build();
  }

  private static List<Field> fromConnectSchema(final Schema schema) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("Top level schema must be STRUCT");
    }

    final ConnectToSqlTypeConverter converter = SchemaConverters.connectToSqlConverter();

    final ImmutableList.Builder<Field> builder = ImmutableList.builder();

    for (final org.apache.kafka.connect.data.Field field : schema.fields()) {
      final Optional<String> source = SchemaUtil.getFieldNameAlias(field.name());
      final String fieldName = SchemaUtil.getFieldNameWithNoAlias(field.name());
      final SqlType fieldType = converter.toSqlType(field.schema());

      builder.add(Field.of(source, fieldName, fieldType));
    }

    return builder.build();
  }

  public static class Builder {

    private final ImmutableList.Builder<Field> keyFields = ImmutableList.builder();
    private final ImmutableList.Builder<Field> valueFields = ImmutableList.builder();

    private final Set<String> keyFieldNames = new HashSet<>();
    private final Set<String> valueFieldNames = new HashSet<>();

    public Builder keyField(final String fieldName, final SqlType fieldType) {
      keyField(Field.of(fieldName, fieldType));
      return this;
    }

    public Builder keyField(final Field field) {
      throwOnDuplicateName(field.fullName(), keyFieldNames);
      keyFields.add(field);
      keyFieldNames.add(field.fullName());
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
      throwOnDuplicateName(field.fullName(), valueFieldNames);
      valueFields.add(field);
      valueFieldNames.add(field.fullName());
      return this;
    }

    public Builder valueFields(final Iterable<? extends Field> fields) {
      fields.forEach(this::valueField);
      return this;
    }

    public LogicalSchema build() {
      final ImmutableList<Field> suppliedKeys = this.keyFields.build();

      final ImmutableList<Field> keys = suppliedKeys.isEmpty()
          ? IMPLICIT_KEY_FIELDS
          : suppliedKeys;

      return new LogicalSchema(
          META_FIELDS,
          keys,
          valueFields.build()
      );
    }

    private static void throwOnDuplicateName(final String fullName, final Set<String> existing) {
      if (existing.contains(fullName)) {
        throw new IllegalArgumentException("Duplicate field name: " + fullName);
      }
    }
  }
}