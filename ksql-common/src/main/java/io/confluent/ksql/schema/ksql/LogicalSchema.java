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
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Immutable KSQL logical schema.
 *
 * <p>KSQL's logical schema internal uses the Connect {@link org.apache.kafka.connect.data.Schema}
 * interface. The interface has two main implementations: a mutable {@link
 * org.apache.kafka.connect.data.SchemaBuilder} and an immutable {@link
 * org.apache.kafka.connect.data.ConnectSchema}.
 *
 * <p>The purpose of this class is two fold:
 * <ul>
 * <li>First, to ensure the schemas used to hold the KSQL logical model are always immutable</li>
 * <li>Second, to provide a KSQL specific immutable schema type, rather than have the code
 * use {@code ConnectSchema}, which can be confusing as {@code ConnectSchema} is also used in the
 * serde code.</li>
 * </ul>
 */
@Immutable
public final class LogicalSchema {

  private static final Schema METADATA_SCHEMA = SchemaBuilder
      .struct()
      .field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  private static final Schema KEY_SCHEMA = SchemaBuilder
      .struct()
      .field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .build();

  private static final Consumer<Schema> NO_ADDITIONAL_VALIDATION = schema -> {
  };

  private static final Map<Type, Consumer<Schema>> VALIDATORS =
      ImmutableMap.<Type, Consumer<Schema>>builder()
          .put(Type.BOOLEAN, NO_ADDITIONAL_VALIDATION)
          .put(Type.INT32, NO_ADDITIONAL_VALIDATION)
          .put(Type.INT64, NO_ADDITIONAL_VALIDATION)
          .put(Type.FLOAT64, NO_ADDITIONAL_VALIDATION)
          .put(Type.STRING, NO_ADDITIONAL_VALIDATION)
          .put(Type.ARRAY, LogicalSchema::validateArray)
          .put(Type.MAP, LogicalSchema::validateMap)
          .put(Type.STRUCT, LogicalSchema::validateStruct)
          .put(Type.BYTES, DecimalUtil::requireDecimal)
          .build();

  private final Optional<String> alias;
  private final ConnectSchema metaSchema;
  private final ConnectSchema keySchema;
  private final ConnectSchema valueSchema;

  public static LogicalSchema of(final Schema valueSchema) {
    return new LogicalSchema(METADATA_SCHEMA, KEY_SCHEMA, valueSchema, Optional.empty());
  }

  private LogicalSchema(
      final Schema metaSchema,
      final Schema keySchema,
      final Schema valueSchema,
      final Optional<String> alias
  ) {
    this.metaSchema = validate(requireNonNull(metaSchema, "metaSchema"), true);
    this.keySchema = validate(requireNonNull(keySchema, "keySchema"), true);
    this.valueSchema = validate(requireNonNull(valueSchema, "valueSchema"), true);
    this.alias = requireNonNull(alias, "alias");
  }

  public ConnectSchema valueSchema() {
    return valueSchema;
  }

  /**
   * @return the metadata fields in the schema.
   */
  public List<Field> metaFields() {
    return ImmutableList.copyOf(metaSchema.fields());
  }

  /**
   * @return the key fields in the schema.
   */
  public List<Field> keyFields() {
    return ImmutableList.copyOf(keySchema.fields());
  }

  /**
   * @return the value fields in the schema.
   */
  public List<Field> valueFields() {
    return ImmutableList.copyOf(valueSchema.fields());
  }

  /**
   * @return all fields in the schema.
   */
  public List<Field> fields() {
    return ImmutableList.<Field>builder()
        .addAll(metaFields())
        .addAll(keyFields())
        .addAll(valueFields())
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
   * @param fieldName the field name, where any alias is ignored.
   * @return the field if found, else {@code Optiona.empty()}.
   */
  public Optional<Field> findField(final String fieldName) {
    Optional<Field> found = findSchemaField(fieldName, metaSchema);
    if (found.isPresent()) {
      return found;
    }

    found = findSchemaField(fieldName, keySchema);
    if (found.isPresent()) {
      return found;
    }

    return findSchemaField(fieldName, valueSchema);
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
    return findSchemaField(fieldName, valueSchema);
  }

  /**
   * Find the index of the field with the supplied exact {@code fieldName}.
   *
   * @param fieldName the exact name of the field to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt valueFieldIndex(final String fieldName) {
    final Field field = valueSchema.field(fieldName);
    if (field == null) {
      return OptionalInt.empty();
    }

    return OptionalInt.of(field.index());
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
    if (this.alias.isPresent()) {
      throw new IllegalStateException("Already aliased");
    }

    return new LogicalSchema(
        addAlias(alias, metaSchema),
        addAlias(alias, keySchema),
        addAlias(alias, valueSchema),
        Optional.of(alias)
    );
  }

  /**
   * Strip any alias from the field name.
   *
   * @return the schema without any aliases in the field name.
   */
  public LogicalSchema withoutAlias() {
    if (!alias.isPresent()) {
      throw new IllegalStateException("Not aliased");
    }

    return new LogicalSchema(
        removeAlias(metaSchema),
        removeAlias(keySchema),
        removeAlias(valueSchema),
        Optional.empty()
    );
  }

  /**
   * Copies metadata and key fields to the value schema.
   *
   * <p>If the fields already exist in the value schema the function returns the same schema.
   *
   * @return the new schema.
   */
  public LogicalSchema withMetaAndKeyFieldsInValue() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    metaFields().forEach(f -> schemaBuilder.field(f.name(), f.schema()));

    keyFields().forEach(f -> schemaBuilder.field(f.name(), f.schema()));

    valueFields().forEach(f -> {
      if (!findSchemaField(f.name(), schemaBuilder).isPresent()) {
        schemaBuilder.field(f.name(), f.schema());
      }
    });

    return new LogicalSchema(
        metaSchema,
        keySchema,
        schemaBuilder.build(),
        alias);
  }

  /**
   * Remove metadata and key fields from the value schema.
   *
   * @return the new schema with the fields removed.
   */
  public LogicalSchema withoutMetaAndKeyFieldsInValue() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    final Set<String> excluded = metaAndKeyFieldNames();

    valueSchema.fields().stream()
        .filter(f -> !excluded.contains(f.name()))
        .forEach(f -> schemaBuilder.field(f.name(), f.schema()));

    return new LogicalSchema(
        metaSchema,
        keySchema,
        schemaBuilder.build(),
        alias);
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
    return schemasAreEqual(valueSchema, that.valueSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueSchema);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    final SqlSchemaFormatter formatter = new SqlSchemaFormatter(
        formatOptions::isReservedWord, Option.AS_COLUMN_LIST);

    return "[" + formatter.format(valueSchema) + "]";
  }

  private Set<String> metaFieldNames() {
    return fieldNames(metaFields());
  }

  private Set<String> keyFieldNames() {
    return fieldNames(keyFields());
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

  private static Schema addAlias(final String alias, final ConnectSchema schema) {
    final SchemaBuilder newSchema = SchemaBuilder
        .struct()
        .name(schema.name());

    for (final Field field : schema.fields()) {
      final String aliased = SchemaUtil.buildAliasedFieldName(alias, field.name());
      newSchema.field(aliased, field.schema());
    }
    return newSchema.build();
  }

  private static Schema removeAlias(final Schema schema) {
    final SchemaBuilder newSchema = SchemaBuilder
        .struct()
        .name(schema.name());

    for (final Field field : schema.fields()) {
      final String unaliased = SchemaUtil.getFieldNameWithNoAlias(field.name());
      newSchema.field(unaliased, field.schema());
    }
    return newSchema.build();
  }

  private static Optional<Field> findSchemaField(final String fieldName, final Schema schema) {
    return schema.fields()
        .stream()
        .filter(f -> SchemaUtil.matchFieldName(f, fieldName))
        .findFirst();
  }

  private static ConnectSchema validate(final Schema schema, final boolean topLevel) {
    if (topLevel && schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("Top level schema must be STRUCT. schema: " + schema);
    }

    if (!(schema instanceof ConnectSchema)) {
      throw new IllegalArgumentException("Mutable schema found: " + schema);
    }

    if (!topLevel && !schema.isOptional()) {
      throw new IllegalArgumentException("Non-optional field found: " + schema);
    }

    final Consumer<Schema> validator = VALIDATORS.get(schema.type());
    if (validator == null) {
      throw new IllegalArgumentException("Unsupported schema type: " + schema);
    }

    validator.accept(schema);
    return (ConnectSchema) schema;
  }

  private static void validateArray(final Schema schema) {
    validate(schema.valueSchema(), false);
  }

  private static void validateMap(final Schema schema) {
    if (schema.keySchema().type() != Type.STRING) {
      throw new IllegalArgumentException("MAP only supports STRING keys");
    }

    validate(schema.keySchema(), false);
    validate(schema.valueSchema(), false);
  }

  private static void validateStruct(final Schema schema) {
    for (int idx = 0; idx != schema.fields().size(); ++idx) {
      final Field field = schema.fields().get(idx);
      validate(field.schema(), false);
    }
  }

  private static boolean schemasAreEqual(final Schema schema1, final Schema schema2) {
    if (schema1.fields().size() != schema2.fields().size()) {
      return false;
    }

    for (int i = 0; i < schema1.fields().size(); i++) {
      final Field f1 = schema1.fields().get(i);
      final Field f2 = schema2.fields().get(i);
      if (!f1.equals(f2)) {
        return false;
      }
    }

    return true;
  }
}

