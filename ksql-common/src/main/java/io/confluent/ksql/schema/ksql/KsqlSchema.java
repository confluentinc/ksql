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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.util.SchemaUtil;
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
public final class KsqlSchema {

  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(Option.AS_COLUMN_LIST);

  private static final Set<String> IMPLICIT_FIELD_NAMES = ImmutableSet.of(
      SchemaUtil.ROWTIME_NAME,
      SchemaUtil.ROWKEY_NAME
  );

  private static final Consumer<Schema> NO_ADDITIONAL_VALIDATION = schema -> {
  };

  private static final Map<Type, Consumer<Schema>> VALIDATORS =
      ImmutableMap.<Type, Consumer<Schema>>builder()
          .put(Type.BOOLEAN, NO_ADDITIONAL_VALIDATION)
          .put(Type.INT32, NO_ADDITIONAL_VALIDATION)
          .put(Type.INT64, NO_ADDITIONAL_VALIDATION)
          .put(Type.FLOAT64, NO_ADDITIONAL_VALIDATION)
          .put(Type.STRING, NO_ADDITIONAL_VALIDATION)
          .put(Type.ARRAY, KsqlSchema::validateArray)
          .put(Type.MAP, KsqlSchema::validateMap)
          .put(Type.STRUCT, KsqlSchema::validateStruct)
          .build();

  private final ConnectSchema schema;

  public static KsqlSchema of(final Schema schema) {
    return new KsqlSchema(schema);
  }

  private KsqlSchema(final Schema schema) {
    this.schema = validate(Objects.requireNonNull(schema, "schema"), true);
  }

  public ConnectSchema getSchema() {
    return schema;
  }

  /**
   * Get all the fields in the schema.
   *
   * @return all the fields in the schema.
   */
  public List<Field> fields() {
    return schema.fields();
  }

  /**
   * Get the set of field indexes for the implicit fields, if any.
   *
   * @return the set of indexes to the implicit fields.
   */
  public Set<Integer> implicitColumnIndexes() {
    return IMPLICIT_FIELD_NAMES.stream()
        .map(schema::field)
        .filter(Objects::nonNull)
        .map(Field::index)
        .collect(Collectors.toSet());
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
   */
  public Optional<Field> findField(final String fieldName) {
    return schema.fields()
        .stream()
        .filter(f -> SchemaUtil.matchFieldName(f, fieldName))
        .findFirst();
  }

  /**
   * Find the index of the field with the supplied exact {@code fieldName}.
   *
   * @param fieldName the exact name of the field to get the index of.
   * @return the index if it exists or else {@code empty()}.
   */
  public OptionalInt fieldIndex(final String fieldName) {
    final Field field = schema.field(fieldName);
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
  public KsqlSchema withAlias(final String alias) {
    final SchemaBuilder newSchema = SchemaBuilder
        .struct()
        .name(schema.name());

    for (final Field field : schema.fields()) {
      final String aliased = SchemaUtil.buildAliasedFieldName(alias, field.name());
      newSchema.field(aliased, field.schema());
    }

    return KsqlSchema.of(newSchema.build());
  }

  /**
   * Strip any alias from the field name.
   *
   * @return the schema without any aliases in the field name.
   */
  public KsqlSchema withoutAlias() {
    final SchemaBuilder newSchema = SchemaBuilder
        .struct()
        .name(schema.name());

    for (final Field field : schema.fields()) {
      final String unaliased = SchemaUtil.getFieldNameWithNoAlias(field.name());
      newSchema.field(unaliased, field.schema());
    }

    return KsqlSchema.of(newSchema.build());
  }

  /**
   * Add implicit fields to the schema.
   *
   * <p>Implicit fields are:
   * <ol>
   * <li>{@link SchemaUtil#ROWTIME_NAME}</li>
   * <li>{@link SchemaUtil#ROWKEY_NAME}</li>
   * </ol>
   *
   * <p>If the implicit fields already exist, the function returns the same schema.
   *
   * <p><b>NOTE:</b> the function does NOT take any aliases in the fields into account
   *
   * @return the new schema with the (unaliased) implicit fields added.
   */
  public KsqlSchema withImplicitFields() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA);
    for (final Field field : ((Schema) schema).fields()) {
      if (!isImplicitColumnName(field.name())) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return KsqlSchema.of(schemaBuilder.build());
  }

  /**
   * Remove implicit fields to the schema.
   *
   * <p>Implicit fields are:
   * <ol>
   * <li>{@link SchemaUtil#ROWTIME_NAME}</li>
   * <li>{@link SchemaUtil#ROWKEY_NAME}</li>
   * </ol>
   *
   * <p><b>NOTE:</b> the function DOES take any aliases in the fields into account.
   *
   * @return the new schema with the implicit fields removed.
   */
  public KsqlSchema withoutImplicitFields() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (final Field field : schema.fields()) {
      final String fieldName = SchemaUtil.getFieldNameWithNoAlias(field.name());
      if (!isImplicitColumnName(fieldName)) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }

    return KsqlSchema.of(schemaBuilder.build());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlSchema that = (KsqlSchema) o;
    return schemasAreEqual(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }

  @Override
  public String toString() {
    return "[" + FORMATTER.format(schema) + "]";
  }

  public static boolean isImplicitColumnName(final String fieldName) {
    return IMPLICIT_FIELD_NAMES.contains(fieldName.toUpperCase());
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

