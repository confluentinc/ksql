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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilTest {

  private static final ConnectSchema STRUCT_SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
      .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build();

  private static final ConnectSchema SCHEMA = (ConnectSchema) SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("ARRAYCOL", SchemaBuilder
          .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field("MAPCOL", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build())
      .field("RAW_STRUCT", STRUCT_SCHEMA)
      .field("ARRAY_OF_STRUCTS", SchemaBuilder
          .array(STRUCT_SCHEMA)
          .optional()
          .build())
      .field("MAP-OF-STRUCTS", SchemaBuilder
          .map(Schema.OPTIONAL_STRING_SCHEMA, STRUCT_SCHEMA)
          .optional()
          .build())
      .field("NESTED.STRUCTS", SchemaBuilder
          .struct()
          .field("s0", STRUCT_SCHEMA)
          .field("s1", SchemaBuilder
              .struct()
              .field("ss0", STRUCT_SCHEMA))
          .build())
      .build();

  @Test
  public void shouldGetCorrectJavaClassForBoolean() {
    final Class<?> booleanClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    assertThat(booleanClazz, equalTo(Boolean.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForInt() {
    final Class<?> intClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_INT32_SCHEMA);
    assertThat(intClazz, equalTo(Integer.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForBigInt() {
    final Class<?> longClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_INT64_SCHEMA);
    assertThat(longClazz, equalTo(Long.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForDouble() {
    final Class<?> doubleClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_FLOAT64_SCHEMA);
    assertThat(doubleClazz, equalTo(Double.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForString() {
    final Class<?> StringClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_STRING_SCHEMA);
    assertThat(StringClazz, equalTo(String.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForArray() {
    final Class<?> arrayClazz = SchemaUtil
        .getJavaType(SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build());
    assertThat(arrayClazz, equalTo(List.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForMap() {
    final Class<?> mapClazz = SchemaUtil.getJavaType(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
            .build());
    assertThat(mapClazz, equalTo(Map.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForBytes() {
    final Class<?> decClazz = SchemaUtil.getJavaType(DecimalUtil.builder(2, 1).build());
    assertThat(decClazz, equalTo(BigDecimal.class));
  }

  @Test
  public void shouldCreateCorrectAvroSchemaWithNullableFields() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder.struct()
        .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("itemid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("arraycol", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("mapcol",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA))
        .optional()
        .build();

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil
        .buildAvroSchema(PersistenceSchema.from(schema, false), "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo(
        "{\"type\":\"record\",\"name\":\"orders\",\"namespace\":\"ksql\",\"fields\":"
            + "[{\"name\":\"ordertime\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":"
            + "\"orderid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"itemid\","
            + "\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"orderunits\",\"type\":"
            + "[\"null\",\"double\"],\"default\":null},{\"name\":\"arraycol\",\"type\":[\"null\","
            + "{\"type\":\"array\",\"items\":[\"null\",\"double\"]}],\"default\":null},{\"name\":"
            + "\"mapcol\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"double\"]}]"
            + ",\"default\":null}]}"));
  }

  @Test
  public void shouldSupportAvroStructs() {
    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil
        .buildAvroSchema(PersistenceSchema.from(SCHEMA, false), "bob");

    // Then:
    final org.apache.avro.Schema.Field rawStruct = avroSchema.getField("RAW_STRUCT");
    assertThat(rawStruct, is(notNullValue()));
    assertThat(rawStruct.schema().getType(), is(org.apache.avro.Schema.Type.UNION));
    assertThat(rawStruct.schema().getTypes().get(0).getType(),
        is(org.apache.avro.Schema.Type.NULL));
    assertThat(rawStruct.schema().getTypes().get(1).toString(), is(
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"RAW_STRUCT\","
            + "\"namespace\":\"ksql.bob\","
            + "\"fields\":["
            + "{\"name\":\"f0\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
            + "]}"
    ));
  }

  @Test
  public void shouldSupportAvroArrayOfStructs() {
    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil
        .buildAvroSchema(PersistenceSchema.from(SCHEMA, false), "bob");

    // Then:
    final org.apache.avro.Schema.Field rawStruct = avroSchema.getField("ARRAY_OF_STRUCTS");
    assertThat(rawStruct, is(notNullValue()));
    assertThat(rawStruct.schema().getType(), is(org.apache.avro.Schema.Type.UNION));
    assertThat(rawStruct.schema().getTypes().get(0).getType(),
        is(org.apache.avro.Schema.Type.NULL));
    assertThat(rawStruct.schema().getTypes().get(1).toString(), is(
        "{"
            + "\"type\":\"array\","
            + "\"items\":["
            + "\"null\","
            + "{\"type\":\"record\","
            + "\"name\":\"ARRAY_OF_STRUCTS\","
            + "\"namespace\":\"ksql.bob\","
            + "\"fields\":["
            + "{\"name\":\"f0\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
            + "]}]}"
    ));
  }

  @Test
  public void shouldSupportAvroMapOfStructs() {
    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil
        .buildAvroSchema(PersistenceSchema.from(SCHEMA, false), "bob");

    // Then:
    final org.apache.avro.Schema.Field rawStruct = avroSchema.getField("MAP_OF_STRUCTS");
    assertThat(rawStruct, is(notNullValue()));
    assertThat(rawStruct.schema().getType(), is(org.apache.avro.Schema.Type.UNION));
    assertThat(rawStruct.schema().getTypes().get(0).getType(),
        is(org.apache.avro.Schema.Type.NULL));
    assertThat(rawStruct.schema().getTypes().get(1).toString(), is(
        "{"
            + "\"type\":\"map\","
            + "\"values\":["
            + "\"null\","
            + "{\"type\":\"record\","
            + "\"name\":\"MAP_OF_STRUCTS\","
            + "\"namespace\":\"ksql.bob\","
            + "\"fields\":["
            + "{\"name\":\"f0\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
            + "]}]}"
    ));
  }

  @Test
  public void shouldSupportAvroNestedStructs() {
    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil
        .buildAvroSchema(PersistenceSchema.from(SCHEMA, false), "bob");

    // Then:
    final org.apache.avro.Schema.Field rawStruct = avroSchema.getField("NESTED_STRUCTS");
    assertThat(rawStruct, is(notNullValue()));
    assertThat(rawStruct.schema().getType(), is(org.apache.avro.Schema.Type.UNION));
    assertThat(rawStruct.schema().getTypes().get(0).getType(),
        is(org.apache.avro.Schema.Type.NULL));

    final String s0Schema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"s0\","
        + "\"namespace\":\"ksql.bob.NESTED_STRUCTS\","
        + "\"fields\":["
        + "{\"name\":\"f0\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
        + "]}";

    final String ss0Schema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"ss0\","
        + "\"namespace\":\"ksql.bob.NESTED_STRUCTS.s1\","
        + "\"fields\":["
        + "{\"name\":\"f0\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"],\"default\":null}"
        + "]}";

    final String s1Schema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"s1\","
        + "\"namespace\":\"ksql.bob.NESTED_STRUCTS\","
        + "\"fields\":["
        + "{\"name\":\"ss0\",\"type\":[\"null\"," + ss0Schema + "],\"default\":null}"
        + "]}";

    assertThat(rawStruct.schema().getTypes().get(1).toString(), is(
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"NESTED_STRUCTS\","
            + "\"namespace\":\"ksql.bob\","
            + "\"fields\":["
            + "{\"name\":\"s0\",\"type\":[\"null\"," + s0Schema + "],\"default\":null},"
            + "{\"name\":\"s1\",\"type\":[\"null\"," + s1Schema + "],\"default\":null}"
            + "]}"
    ));
  }

  @Test
  public void shouldCreateAvroSchemaForBoolean() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("\"boolean\""));
  }

  @Test
  public void shouldCreateAvroSchemaForInt() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(Schema.OPTIONAL_INT32_SCHEMA);

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("\"int\""));
  }

  @Test
  public void shouldCreateAvroSchemaForBigInt() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(Schema.OPTIONAL_INT64_SCHEMA);

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("\"long\""));
  }

  @Test
  public void shouldCreateAvroSchemaForDouble() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("\"double\""));
  }

  @Test
  public void shouldCreateAvroSchemaForString() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("\"string\""));
  }

  @Test
  public void shouldCreateAvroSchemaForArray() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(
        SchemaBuilder
            .array(Schema.OPTIONAL_INT64_SCHEMA)
            .build()
    );

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("{"
        + "\"type\":\"array\","
        + "\"items\":[\"null\",\"long\"]"
        + "}"));
  }

  @Test
  public void shouldCreateAvroSchemaForMap() {
    // Given:
    final PersistenceSchema schema = unwrappedPersistenceSchema(
        SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.BOOLEAN_SCHEMA)
            .build()
    );

    // When:
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "orders");

    // Then:
    assertThat(avroSchema.toString(), equalTo("{"
        + "\"type\":\"map\","
        + "\"values\":[\"null\",\"boolean\"]"
        + "}"));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForBoolean() {
    final Schema schema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Boolean.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForInt() {
    final Schema schema = Schema.OPTIONAL_INT32_SCHEMA;
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Integer.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForLong() {
    final Schema schema = Schema.OPTIONAL_INT64_SCHEMA;
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Long.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForDouble() {
    final Schema schema = Schema.OPTIONAL_FLOAT64_SCHEMA;
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Double.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForString() {
    final Schema schema = Schema.OPTIONAL_STRING_SCHEMA;
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(String.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForArray() {
    final Schema schema = SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(List.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForMap() {
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Class<?> javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Map.class));
  }

  @Test
  public void shouldFailForCorrectJavaType() {
    try {
      SchemaUtil.getJavaType(Schema.BYTES_SCHEMA);
      Assert.fail();
    } catch (final KsqlException ksqlException) {
      assertThat("Invalid type retured.", ksqlException.getMessage(), equalTo("Type is not "
          + "supported: BYTES"));
    }
  }

  @Test
  public void shouldMatchName() {
    final Field field = new Field("foo", 0, Schema.INT32_SCHEMA);
    assertThat(SchemaUtil.matchFieldName(field, "foo"), is(true));
  }

  @Test
  public void shouldNotMatchDifferentName() {
    final Field field = new Field("foo", 0, Schema.INT32_SCHEMA);
    assertThat(SchemaUtil.matchFieldName(field, "bar"), is(false));
  }

  @Test
  public void shouldMatchNameWithAlias() {
    final Field field = new Field("foo", 0, Schema.INT32_SCHEMA);
    assertThat(SchemaUtil.matchFieldName(field, "bar.foo"), is(true));
  }

  @Test
  public void shouldMatchFieldNameOnExactMatch() {
    assertThat(SchemaUtil.isFieldName("bob", "bob"), is(true));
    assertThat(SchemaUtil.isFieldName("aliased.bob", "aliased.bob"), is(true));
  }

  @Test
  public void shouldMatchFieldNameEvenIfActualAliased() {
    assertThat(SchemaUtil.isFieldName("aliased.bob", "bob"), is(true));
  }

  @Test
  public void shouldNotMatchFieldNamesOnMismatch() {
    assertThat(SchemaUtil.isFieldName("different", "bob"), is(false));
    assertThat(SchemaUtil.isFieldName("aliased.different", "bob"), is(false));
  }

  @Test
  public void shouldNotMatchFieldNamesIfRequiredIsAliased() {
    assertThat(SchemaUtil.isFieldName("bob", "aliased.bob"), is(false));
  }

  @Test
  public void shouldGetTheCorrectJavaCastClass() {
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.OPTIONAL_BOOLEAN_SCHEMA),
        equalTo("(Boolean)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.OPTIONAL_INT32_SCHEMA),
        equalTo("(Integer)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.OPTIONAL_INT64_SCHEMA),
        equalTo("(Long)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.OPTIONAL_FLOAT64_SCHEMA),
        equalTo("(Double)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.OPTIONAL_STRING_SCHEMA),
        equalTo("(String)"));
  }

  @Test
  public void shouldStripAliasFromField() {
    // Given:
    final Field field = new Field("alias.some-field-name", 1, Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final String result = SchemaUtil.getFieldNameWithNoAlias(field);

    // Then:
    assertThat(result, is("some-field-name"));
  }

  @Test
  public void shouldReturnFieldWithoutAliasAsIs() {
    // Given:
    final Field field = new Field("some-field-name", 1, Schema.OPTIONAL_STRING_SCHEMA);

    // When:
    final String result = SchemaUtil.getFieldNameWithNoAlias(field);

    // Then:
    assertThat(result, is("some-field-name"));
  }

  @Test
  public void shouldStripAliasFromFieldName() {
    // When:
    final String result = SchemaUtil.getFieldNameWithNoAlias("some-alias.some-field-name");

    // Then:
    assertThat(result, is("some-field-name"));
  }

  @Test
  public void shouldReturnFieldNameWithoutAliasAsIs() {
    // When:
    final String result = SchemaUtil.getFieldNameWithNoAlias("some-field-name");

    // Then:
    assertThat(result, is("some-field-name"));
  }

  @Test
  public void shouldReturnNoAlias() {
    assertThat(SchemaUtil.getFieldNameAlias("not-aliased"), is(Optional.empty()));
  }

  @Test
  public void shouldReturnAlias() {
    assertThat(SchemaUtil.getFieldNameAlias("is.aliased"), is(Optional.of("is")));
  }

  @Test
  public void shouldResolveIntAndLongSchemaToLong() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.INT64_SCHEMA, Schema.INT32_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.INT64));
  }

  @Test
  public void shouldResolveIntAndIntSchemaToInt() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.INT32));
  }

  @Test
  public void shouldResolveFloat64AndAnyNumberTypeToFloat() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.INT32_SCHEMA, Schema.FLOAT64_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.FLOAT64));
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.FLOAT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.FLOAT64));
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.FLOAT32_SCHEMA, Schema.FLOAT64_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.FLOAT64));
  }

  @Test
  public void shouldResolveDecimalAddition() {
    final Map<Pair<PrecisionScale, PrecisionScale>, PrecisionScale> inputToExpected =
        ImmutableMap.<Pair<PrecisionScale, PrecisionScale>, PrecisionScale>builder()
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 1)), PrecisionScale.of(3, 1))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(2, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(3, 2)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(3, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(4, 2))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.left.precision, in.left.scale).build();
      final Schema d2 = DecimalUtil.builder(in.right.precision, in.right.scale).build();

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.ADD);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalSubtraction() {
    final Map<Pair<PrecisionScale, PrecisionScale>, PrecisionScale> inputToExpected =
        ImmutableMap.<Pair<PrecisionScale, PrecisionScale>, PrecisionScale>builder()
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 1)), PrecisionScale.of(3, 1))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(2, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(3, 2)), PrecisionScale.of(4, 2))
            .put(Pair.of(PrecisionScale.of(3, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(4, 2))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.left.precision, in.left.scale).build();
      final Schema d2 = DecimalUtil.builder(in.right.precision, in.right.scale).build();

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.SUBTRACT);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalMultiply() {
    final Map<Pair<PrecisionScale, PrecisionScale>, PrecisionScale> inputToExpected =
        ImmutableMap.<Pair<PrecisionScale, PrecisionScale>, PrecisionScale>builder()
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 1)), PrecisionScale.of(5, 2))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(5, 3))
            .put(Pair.of(PrecisionScale.of(2, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(5, 3))
            .put(Pair.of(PrecisionScale.of(3, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(6, 3))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.left.precision, in.left.scale).build();
      final Schema d2 = DecimalUtil.builder(in.right.precision, in.right.scale).build();

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.MULTIPLY);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalDivide() {
    final Map<Pair<PrecisionScale, PrecisionScale>, PrecisionScale> inputToExpected =
        ImmutableMap.<Pair<PrecisionScale, PrecisionScale>, PrecisionScale>builder()
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 1)), PrecisionScale.of(8, 6))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(9, 6))
            .put(Pair.of(PrecisionScale.of(2, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(7, 6))
            .put(Pair.of(PrecisionScale.of(3, 3), PrecisionScale.of(3, 3)), PrecisionScale.of(10, 7))
            .put(Pair.of(PrecisionScale.of(3, 3), PrecisionScale.of(3, 2)), PrecisionScale.of(9, 7))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.left.precision, in.left.scale).build();
      final Schema d2 = DecimalUtil.builder(in.right.precision, in.right.scale).build();

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.DIVIDE);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalMod() {
    final Map<Pair<PrecisionScale, PrecisionScale>, PrecisionScale> inputToExpected =
        ImmutableMap.<Pair<PrecisionScale, PrecisionScale>, PrecisionScale>builder()
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 1)), PrecisionScale.of(2, 1))
            .put(Pair.of(PrecisionScale.of(2, 2), PrecisionScale.of(2, 1)), PrecisionScale.of(2, 2))
            .put(Pair.of(PrecisionScale.of(2, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(2, 2))
            .put(Pair.of(PrecisionScale.of(3, 1), PrecisionScale.of(2, 2)), PrecisionScale.of(2, 2))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.left.precision, in.left.scale).build();
      final Schema d2 = DecimalUtil.builder(in.right.precision, in.right.scale).build();

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.MODULUS);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalLongAdd() {
    final Map<PrecisionScale, PrecisionScale> inputToExpected =
        ImmutableMap.<PrecisionScale, PrecisionScale>builder()
            .put(PrecisionScale.of(2, 1), PrecisionScale.of(21, 1))
            .put(PrecisionScale.of(3, 3), PrecisionScale.of(23, 3))
            .put(PrecisionScale.of(23, 0), PrecisionScale.of(24, 0))
            .build();

    inputToExpected.forEach((in, out) -> {
      // Given:
      final Schema d1 = DecimalUtil.builder(in.precision, in.scale).build();
      final Schema d2 = Schema.OPTIONAL_INT64_SCHEMA;

      // When:
      final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.ADD);

      // Then:
      assertThat(String.format("precision: %s", in), DecimalUtil.precision(result), is(out.precision));
      assertThat(String.format("scale: %s", in), DecimalUtil.scale(result), is(out.scale));
    });
  }

  @Test
  public void shouldResolveDecimalDoubleMath() {
    // Given:
    final Schema d1 = DecimalUtil.builder(15, 10).build();
    final Schema d2 = Schema.OPTIONAL_FLOAT64_SCHEMA;

    // When:
    final Schema result = SchemaUtil.resolveBinaryOperatorResultType(d1, d2, Operator.ADD);

    // Then:
    assertThat(result, is(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  private static class PrecisionScale {
    final int precision;
    final int scale;

    static PrecisionScale of(final int precision, final int scale) {
      return new PrecisionScale(precision, scale);
    }

    private PrecisionScale(final int precision, final int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public String toString() {
      return "PrecisionScale{"
          + "precision=" + precision
          + ", scale=" + scale
          + '}';
    }
  }

  @Test
  public void shouldResolveStringAndStringToString() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, Operator.ADD).type(),
        equalTo(Schema.Type.STRING));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionWhenResolvingStringWithAnythingElse() {
    SchemaUtil.resolveBinaryOperatorResultType(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA, Operator.ADD);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionWhenResolvingUnkonwnType() {
    SchemaUtil.resolveBinaryOperatorResultType(Schema.BOOLEAN_SCHEMA, Schema.FLOAT64_SCHEMA, Operator.ADD);
  }


  @Test
  public void shouldPassIsNumberForInt() {
    assertThat(SchemaUtil.isNumber(Schema.Type.INT32), is(true));
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.INT32_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForBigint() {
    assertThat(SchemaUtil.isNumber(Schema.Type.INT64), is(true));
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.INT64_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForDouble() {
    assertThat(SchemaUtil.isNumber(Schema.Type.FLOAT64), is(true));
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.FLOAT64_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForDecimal() {
    assertThat(SchemaUtil.isNumber(DecimalUtil.builder(2, 1)), is(true));
  }

  @Test
  public void shouldFailIsNumberForBoolean() {
    assertThat(SchemaUtil.isNumber(Schema.Type.BOOLEAN), is(false));
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_BOOLEAN_SCHEMA), is(false));
    assertThat(SchemaUtil.isNumber(Schema.BOOLEAN_SCHEMA), is(false));
  }

  @Test
  public void shouldFailIsNumberForString() {
    assertThat(SchemaUtil.isNumber(Schema.Type.STRING), is(false));
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_STRING_SCHEMA), is(false));
    assertThat(SchemaUtil.isNumber(Schema.STRING_SCHEMA), is(false));
  }

  @Test
  public void shouldBuildAliasedFieldName() {
    // When:
    final String result = SchemaUtil.buildAliasedFieldName("SomeAlias", "SomeFieldName");

    // Then:
    assertThat(result, is("SomeAlias.SomeFieldName"));
  }

  @Test
  public void shouldBuildAliasedFieldNameThatIsAlreadyAliased() {
    // When:
    final String result = SchemaUtil.buildAliasedFieldName("SomeAlias", "SomeAlias.SomeFieldName");

    // Then:
    assertThat(result, is("SomeAlias.SomeFieldName"));
  }

  @Test
  public void shouldBuildAliasedField() {
    // Given:
    final Field field = new Field("col0", 1, Schema.INT64_SCHEMA);

    // When:
    final Field result = SchemaUtil
        .buildAliasedField("TheAlias", field);

    // Then:
    assertThat(result, is(new Field("TheAlias.col0", 1, Schema.INT64_SCHEMA)));
  }

  @Test
  public void shouldBuildAliasedFieldThatIsAlreadyAliased() {
    // Given:
    final Field field = new Field("TheAlias.col0", 1, Schema.INT64_SCHEMA);

    // When:
    final Field result = SchemaUtil.buildAliasedField("TheAlias", field);

    // Then:
    assertThat(result, is(field));
  }

  @Test
  public void shouldEnsureDeepOptional() {
    // Given:
    final Schema optionalSchema = SchemaBuilder
        .struct()
        .field("struct", SchemaBuilder
            .struct()
            .field("prim", Schema.FLOAT64_SCHEMA)
            .field("array", SchemaBuilder
                .array(Schema.STRING_SCHEMA)
                .build())
            .field("map", SchemaBuilder
                .map(Schema.INT64_SCHEMA, Schema.BOOLEAN_SCHEMA)
                .build())
            .build())
        .build();

    // When:
    final Schema result = SchemaUtil.ensureOptional(optionalSchema);

    // Then:
    assertThat(result, is(SchemaBuilder
        .struct()
        .field("struct", SchemaBuilder
            .struct()
            .field("prim", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("array", SchemaBuilder
                .array(Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build())
            .field("map", SchemaBuilder
                .map(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build())
            .optional()
            .build())
        .optional()
        .build()
    ));
  }

  private static PersistenceSchema unwrappedPersistenceSchema(final Schema fieldSchema) {
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", fieldSchema)
        .build();

    return PersistenceSchema.from(connectSchema, true);
  }
}

