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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchemaUtilTest {

  private Schema schema;

  @Before
  public void init() {
    final Schema structSchema = SchemaBuilder.struct()
        .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    schema = SchemaBuilder.struct()
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("ARRAYCOL", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("MAPCOL",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .optional().build())
        .field("RAW_STRUCT", structSchema)
        .field("ARRAY_OF_STRUCTS", SchemaBuilder.array(structSchema).optional().build())
        .field("MAP-OF-STRUCTS",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, structSchema).optional().build())
        .field("NESTED.STRUCTS", SchemaBuilder.struct()
            .field("s0", structSchema)
            .field("s1", SchemaBuilder.struct().field("ss0", structSchema))
            .build())
        .build();
  }

  @Test
  public void shouldGetCorrectJavaClassForBoolean() {
    final Class booleanClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    assertThat(booleanClazz, equalTo(Boolean.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForInt() {
    final Class intClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_INT32_SCHEMA);
    assertThat(intClazz, equalTo(Integer.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForBigInt() {
    final Class longClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_INT64_SCHEMA);
    assertThat(longClazz, equalTo(Long.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForDouble() {
    final Class doubleClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_FLOAT64_SCHEMA);
    assertThat(doubleClazz, equalTo(Double.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForString() {
    final Class StringClazz = SchemaUtil.getJavaType(Schema.OPTIONAL_STRING_SCHEMA);
    assertThat(StringClazz, equalTo(String.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForArray() {
    final Class arrayClazz = SchemaUtil
        .getJavaType(SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build());
    assertThat(arrayClazz, equalTo(List.class));
  }

  @Test
  public void shouldGetCorrectJavaClassForMap() {
    final Class mapClazz = SchemaUtil.getJavaType(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
            .build());
    assertThat(mapClazz, equalTo(Map.class));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForBoolean() {
    assertThat(SchemaUtil.getSqlTypeName(Schema.OPTIONAL_BOOLEAN_SCHEMA), equalTo("BOOLEAN"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForInt() {
    assertThat(SchemaUtil.getSqlTypeName(Schema.OPTIONAL_INT32_SCHEMA), equalTo("INT"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForBigint() {
    assertThat(SchemaUtil.getSqlTypeName(Schema.OPTIONAL_INT64_SCHEMA), equalTo("BIGINT"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForDouble() {
    assertThat(SchemaUtil.getSqlTypeName(Schema.OPTIONAL_FLOAT64_SCHEMA), equalTo("DOUBLE"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForArray() {
    assertThat(SchemaUtil
            .getSqlTypeName(SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build()),
        equalTo("ARRAY<DOUBLE>"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForMap() {
    assertThat(SchemaUtil.getSqlTypeName(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
            .build()),
        equalTo("MAP<VARCHAR,DOUBLE>"));
  }

  @Test
  public void shouldGetCorrectSqlTypeNameForStruct() {
    final Schema structSchema = SchemaBuilder.struct()
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_INT32_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("COL5",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
                .optional().build())
        .build();
    assertThat(SchemaUtil.getSqlTypeName(structSchema),
        equalTo(
            "STRUCT<COL1 VARCHAR, COL2 INT, COL3 DOUBLE, COL4 ARRAY<DOUBLE>, COL5 MAP<VARCHAR,DOUBLE>>"));
  }


  @Test
  public void shouldCreateCorrectAvroSchemaWithNullableFields() {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder
        .field("ordertime", Schema.OPTIONAL_INT64_SCHEMA)
        .field("orderid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("itemid", Schema.OPTIONAL_STRING_SCHEMA)
        .field("orderunits", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("arraycol", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("mapcol",
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA))
        .optional().build();
    final String avroSchemaString = SchemaUtil.buildAvroSchema(schemaBuilder.build(), "orders")
        .toString();
    assertThat(avroSchemaString, equalTo(
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
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "bob");

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
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "bob");

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
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "bob");

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
    final org.apache.avro.Schema avroSchema = SchemaUtil.buildAvroSchema(schema, "bob");

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
  public void shouldGetTheCorrectJavaTypeForBoolean() {
    final Schema schema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Boolean.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForInt() {
    final Schema schema = Schema.OPTIONAL_INT32_SCHEMA;
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Integer.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForLong() {
    final Schema schema = Schema.OPTIONAL_INT64_SCHEMA;
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Long.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForDouble() {
    final Schema schema = Schema.OPTIONAL_FLOAT64_SCHEMA;
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Double.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForString() {
    final Schema schema = Schema.OPTIONAL_STRING_SCHEMA;
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(String.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForArray() {
    final Schema schema = SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(List.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForMap() {
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build();
    final Class javaClass = SchemaUtil.getJavaType(schema);
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
  public void shouldGetTheCorrectFieldName() {
    final Optional<Field> field = SchemaUtil.getFieldByName(schema, "orderid".toUpperCase());
    Assert.assertTrue(field.isPresent());
    assertThat(field.get().schema(), sameInstance(Schema.OPTIONAL_INT64_SCHEMA));
    assertThat("", field.get().name().toLowerCase(), equalTo("orderid"));

    final Optional<Field> field1 = SchemaUtil.getFieldByName(schema, "orderid");
    Assert.assertFalse(field1.isPresent());
  }

  @Test
  public void shouldGetTheCorrectFieldIndex() {
    final int index1 = SchemaUtil.getFieldIndexByName(schema, "orderid".toUpperCase());
    final int index2 = SchemaUtil.getFieldIndexByName(schema, "itemid".toUpperCase());
    final int index3 = SchemaUtil.getFieldIndexByName(schema, "mapcol".toUpperCase());

    assertThat("Incorrect index.", index1, equalTo(1));
    assertThat("Incorrect index.", index2, equalTo(2));
    assertThat("Incorrect index.", index3, equalTo(5));

  }

  @Test
  public void shouldHandleInvalidFieldIndexCorrectly() {
    final int index = SchemaUtil.getFieldIndexByName(schema, "mapcol1".toUpperCase());
    assertThat("Incorrect index.", index, equalTo(-1));
  }

  @Test
  public void shouldBuildTheCorrectSchemaWithAndWithoutAlias() {
    final String alias = "Hello";
    final Schema schemaWithAlias = SchemaUtil.buildSchemaWithAlias(schema, alias);
    assertThat("Incorrect schema field count.", schemaWithAlias.fields().size() == schema.fields()
        .size());
    for (int i = 0; i < schemaWithAlias.fields().size(); i++) {
      final Field fieldWithAlias = schemaWithAlias.fields().get(i);
      final Field field = schema.fields().get(i);
      assertThat(fieldWithAlias.name(), equalTo(alias + "." + field.name()));
    }
    final Schema schemaWithoutAlias = SchemaUtil.getSchemaWithNoAlias(schemaWithAlias);
    assertThat("Incorrect schema field count.",
        schemaWithAlias.fields().size() == schema.fields().size());
    for (int i = 0; i < schemaWithoutAlias.fields().size(); i++) {
      final Field fieldWithAlias = schemaWithoutAlias.fields().get(i);
      final Field field = schema.fields().get(i);
      assertThat("Incorrect field name.", fieldWithAlias.name().equals(field.name()));
    }
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
  public void shouldAddAndRemoveImplicitColumns() {
    // Given:
    final int initialFieldCount = schema.fields().size();

    // When:
    final Schema withImplicit = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);

    // Then:
    assertThat("Invalid field count.", withImplicit.fields(), hasSize(initialFieldCount + 2));
    assertThat("Field name should be ROWTIME.", withImplicit.fields().get(0).name(),
        equalTo(SchemaUtil.ROWTIME_NAME));
    assertThat("Field name should ne ROWKEY.", withImplicit.fields().get(1).name(),
        equalTo(SchemaUtil.ROWKEY_NAME));

    // When:
    final Schema withoutImplicit = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(withImplicit);

    // Then:
    assertThat("Invalid field count.", withoutImplicit.fields(), hasSize(initialFieldCount));
    assertThat("Invalid field name.", withoutImplicit.fields().get(0).name(), equalTo("ORDERTIME"));
    assertThat("Invalid field name.", withoutImplicit.fields().get(1).name(), equalTo("ORDERID"));
  }

  @Test
  public void shouldGetTheSchemaDefString() {
    final String schemaDef = SchemaUtil.getSchemaDefinitionString(schema);
    assertThat("Invalid schema def.", schemaDef, equalTo("[ORDERTIME : BIGINT, "
        + "ORDERID : BIGINT, "
        + "ITEMID : VARCHAR, "
        + "ORDERUNITS : DOUBLE, "
        + "ARRAYCOL : ARRAY<DOUBLE>, "
        + "MAPCOL : MAP<VARCHAR,DOUBLE>, "
        + "RAW_STRUCT : STRUCT<f0 BIGINT, f1 BOOLEAN>, "
        + "ARRAY_OF_STRUCTS : ARRAY<STRUCT<f0 BIGINT, f1 BOOLEAN>>, "
        + "MAP-OF-STRUCTS : MAP<VARCHAR,STRUCT<f0 BIGINT, f1 BOOLEAN>>, "
        + "NESTED.STRUCTS : STRUCT<s0 STRUCT<f0 BIGINT, f1 BOOLEAN>, s1 STRUCT<ss0 STRUCT<f0 BIGINT, f1 BOOLEAN>>>]"));
  }

  @Test
  public void shouldGetCorrectSqlType() {
    final String sqlType1 = SchemaUtil.getSqlTypeName(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    final String sqlType2 = SchemaUtil.getSqlTypeName(Schema.OPTIONAL_INT32_SCHEMA);
    final String sqlType3 = SchemaUtil.getSqlTypeName(Schema.OPTIONAL_INT64_SCHEMA);
    final String sqlType4 = SchemaUtil.getSqlTypeName(Schema.OPTIONAL_FLOAT64_SCHEMA);
    final String sqlType5 = SchemaUtil.getSqlTypeName(Schema.OPTIONAL_STRING_SCHEMA);
    final String sqlType6 = SchemaUtil
        .getSqlTypeName(SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build());
    final String sqlType7 = SchemaUtil.getSqlTypeName(
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
            .build());

    assertThat("Invalid SQL type.", sqlType1, equalTo("BOOLEAN"));
    assertThat("Invalid SQL type.", sqlType2, equalTo("INT"));
    assertThat("Invalid SQL type.", sqlType3, equalTo("BIGINT"));
    assertThat("Invalid SQL type.", sqlType4, equalTo("DOUBLE"));
    assertThat("Invalid SQL type.", sqlType5, equalTo("VARCHAR"));
    assertThat("Invalid SQL type.", sqlType6, equalTo("ARRAY<DOUBLE>"));
    assertThat("Invalid SQL type.", sqlType7, equalTo("MAP<VARCHAR,DOUBLE>"));
  }

  @Test
  public void shouldGetCorrectSqlTypeFromSchemaType() {
    assertThat(SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.STRING), is("VARCHAR(STRING)"));
    assertThat(SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.INT64), is("BIGINT"));
    assertThat(SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.INT32), is("INTEGER"));
    assertThat(SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.FLOAT64), is("DOUBLE"));
    assertThat(SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.BOOLEAN), is("BOOLEAN"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnknownSchemaType() {
    SchemaUtil.getSchemaTypeAsSqlType(Schema.Type.BYTES);
  }

  @Test
  public void shouldStripAliasFromFieldName() {
    final Schema schemaWithAlias = SchemaUtil.buildSchemaWithAlias(schema, "alias");
    assertThat("Invalid field name",
        SchemaUtil.getFieldNameWithNoAlias(schemaWithAlias.fields().get(0)),
        equalTo(schema.fields().get(0).name()));
  }

  @Test
  public void shouldReturnFieldNameWithoutAliasAsIs() {
    assertThat("Invalid field name", SchemaUtil.getFieldNameWithNoAlias(schema.fields().get(0)),
        equalTo(schema.fields().get(0).name()));
  }

  @Test
  public void shouldResolveIntAndLongSchemaToLong() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.INT64, Schema.Type.INT32).type(),
        equalTo(Schema.Type.INT64));
  }

  @Test
  public void shouldResolveIntAndIntSchemaToInt() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.INT32, Schema.Type.INT32).type(),
        equalTo(Schema.Type.INT32));
  }

  @Test
  public void shouldResolveFloat64AndAnyNumberTypeToFloat() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.INT32, Schema.Type.FLOAT64).type(),
        equalTo(Schema.Type.FLOAT64));
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.FLOAT64, Schema.Type.INT64).type(),
        equalTo(Schema.Type.FLOAT64));
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.FLOAT32, Schema.Type.FLOAT64).type(),
        equalTo(Schema.Type.FLOAT64));
  }

  @Test
  public void shouldResolveStringAndStringToString() {
    assertThat(
        SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.STRING, Schema.Type.STRING).type(),
        equalTo(Schema.Type.STRING));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionWhenResolvingStringWithAnythingElse() {
    SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.STRING, Schema.Type.FLOAT64);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionWhenResolvingUnkonwnType() {
    SchemaUtil.resolveBinaryOperatorResultType(Schema.Type.BOOLEAN, Schema.Type.FLOAT64);
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanClass() {
    assertThat(SchemaUtil.getSchemaFromType(Boolean.class),
        equalTo(Schema.OPTIONAL_BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanPrimitiveClass() {
    assertThat(SchemaUtil.getSchemaFromType(boolean.class),
        equalTo(Schema.BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldGetIntSchemaForIntegerClass() {
    assertThat(SchemaUtil.getSchemaFromType(Integer.class),
        equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldGetIntegerSchemaForIntPrimitiveClass() {
    assertThat(SchemaUtil.getSchemaFromType(int.class),
        equalTo(Schema.INT32_SCHEMA));
  }

  @Test
  public void shouldGetLongSchemaForLongClass() {
    assertThat(SchemaUtil.getSchemaFromType(Long.class),
        equalTo(Schema.OPTIONAL_INT64_SCHEMA));
  }

  @Test
  public void shouldGetLongSchemaForLongPrimitiveClass() {
    assertThat(SchemaUtil.getSchemaFromType(long.class),
        equalTo(Schema.INT64_SCHEMA));
  }

  @Test
  public void shouldGetFloatSchemaForDoubleClass() {
    assertThat(SchemaUtil.getSchemaFromType(Double.class),
        equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetFloatSchemaForDoublePrimitiveClass() {
    assertThat(SchemaUtil.getSchemaFromType(double.class),
        equalTo(Schema.FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetMapSchemaFromMapClass() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("mapType", Map.class)
        .getGenericParameterTypes()[0];
    final Schema schema = SchemaUtil.getSchemaFromType(type);
    assertThat(schema.type(), equalTo(Schema.Type.MAP));
    assertThat(schema.keySchema(), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(schema.valueSchema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldGetArraySchemaFromListClass() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("listType", List.class)
        .getGenericParameterTypes()[0];
    final Schema schema = SchemaUtil.getSchemaFromType(type);
    assertThat(schema.type(), equalTo(Schema.Type.ARRAY));
    assertThat(schema.valueSchema(), equalTo(Schema.OPTIONAL_FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetStringSchemaFromStringClass() {
    assertThat(SchemaUtil.getSchemaFromType(String.class),
        equalTo(Schema.OPTIONAL_STRING_SCHEMA));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfClassDoesntMapToSchema() {
    SchemaUtil.getSchemaFromType(System.class);
  }

  @Test
  public void shouldDefaultToNoNameOnGetSchemaFromType() {
    assertThat(SchemaUtil.getSchemaFromType(Double.class).name(), is(nullValue()));
  }

  @Test
  public void shouldDefaultToNoDocOnGetSchemaFromType() {
    assertThat(SchemaUtil.getSchemaFromType(Double.class).doc(), is(nullValue()));
  }

  @Test
  public void shouldSetNameOnGetSchemaFromType() {
    assertThat(SchemaUtil.getSchemaFromType(Double.class, "name", "").name(), is("name"));
  }

  @Test
  public void shouldSetDocOnGetSchemaFromType() {
    assertThat(SchemaUtil.getSchemaFromType(Double.class, "", "doc").doc(), is("doc"));
  }

  @Test
  public void shouldPassIsNumberForInt() {
    assertThat(SchemaUtil.isNumber(Schema.Type.INT32), is(true));
  }

  @Test
  public void shouldPassIsNumberForBigint() {
    assertThat(SchemaUtil.isNumber(Schema.Type.INT64), is(true));
  }

  @Test
  public void shouldPassIsNumberForDouble() {
    assertThat(SchemaUtil.isNumber(Schema.Type.FLOAT64), is(true));
  }

  @Test
  public void shouldFailIsNumberForBoolean() {
    assertThat(SchemaUtil.isNumber(Schema.Type.BOOLEAN), is(false));
  }

  @Test
  public void shouldFailIsNumberForString() {
    assertThat(SchemaUtil.isNumber(Schema.Type.STRING), is(false));
  }


  // Following methods not invoked but used to test conversion from Type -> Schema
  @SuppressWarnings("unused")
  private void mapType(final Map<String, Integer> map) {
  }

  @SuppressWarnings("unused")
  private void listType(final List<Double> list) {
  }
}

