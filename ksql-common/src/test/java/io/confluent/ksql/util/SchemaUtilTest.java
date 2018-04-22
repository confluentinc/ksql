/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Optional;

public class SchemaUtilTest {

  Schema schema;

  @Before
  public void init() {
    schema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .field("arraycol".toUpperCase(), SchemaBuilder.array(org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .field("mapcol".toUpperCase(), SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA))
        .build();
  }

  @Test
  public void shouldCreateCorrectAvroSchemaWithNullableFields() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder
        .field("ordertime", Schema.INT64_SCHEMA)
        .field("orderid", Schema.STRING_SCHEMA)
        .field("itemid", Schema.STRING_SCHEMA)
        .field("orderunits", Schema.FLOAT64_SCHEMA)
        .field("arraycol", SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("mapcol", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));
    String avroSchemaString = SchemaUtil.buildAvroSchema(schemaBuilder.build(), "orders");
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
  public void shouldGetTheCorrectJavaTypeForBoolean() {
    Schema schema = Schema.BOOLEAN_SCHEMA;
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Boolean.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForInt() {
    Schema schema = Schema.INT32_SCHEMA;
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Integer.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForLong() {
    Schema schema = Schema.INT64_SCHEMA;
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Long.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForDouble() {
    Schema schema = Schema.FLOAT64_SCHEMA;
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(Double.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForString() {
    Schema schema = Schema.STRING_SCHEMA;
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(String.class));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForArray() {
    Schema schema = SchemaBuilder.array(Schema.FLOAT64_SCHEMA);
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(new Double[]{}.getClass()));
  }

  @Test
  public void shouldGetTheCorrectJavaTypeForMap() {
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA);
    Class javaClass = SchemaUtil.getJavaType(schema);
    assertThat(javaClass, equalTo(HashMap.class));
  }

  @Test
  public void shouldFailForCorrectJavaType() {

    try {
      Class class8 = SchemaUtil.getJavaType(Schema.BYTES_SCHEMA);
      Assert.fail();
    } catch (KsqlException ksqlException) {
      assertThat("Invalid type retured.",ksqlException.getMessage(), equalTo("Type is not "
                                                                          + "supported: BYTES"));
    }
  }

  @Test
  public void shouldGetTheCorrectFieldName() {
    Optional<Field> field = SchemaUtil.getFieldByName(schema, "orderid".toUpperCase());
    Assert.assertTrue(field.isPresent());
    assertThat(field.get().schema(), sameInstance(Schema.INT64_SCHEMA));
    assertThat("", field.get().name().toLowerCase(), equalTo("orderid"));

    Optional<Field> field1 = SchemaUtil.getFieldByName(schema, "orderid");
    Assert.assertFalse(field1.isPresent());
  }

  @Test
  public void shouldGetTheCorrectSchemaForBoolean() {
    Schema schema = SchemaUtil.getTypeSchema("BOOLEAN");
    assertThat(schema,  sameInstance(Schema.BOOLEAN_SCHEMA));
  }

  @Test
  public void shouldGetTheCorrectSchemaForInt() {
    Schema schema = SchemaUtil.getTypeSchema("INT");
    assertThat(schema, sameInstance(Schema.INT32_SCHEMA));
  }

  @Test
  public void shouldGetTheCorrectSchemaForLong() {
    Schema schema = SchemaUtil.getTypeSchema("BIGINT");
    assertThat(schema, sameInstance(Schema.INT64_SCHEMA));
  }

  @Test
  public void shouldGetTheCorrectSchemaForDouble() {
    Schema schema = SchemaUtil.getTypeSchema("DOUBLE");
    assertThat(schema, sameInstance(Schema.FLOAT64_SCHEMA));
  }

  @Test
  public void shouldGetTheCorrectSchemaForString() {
    Schema schema = SchemaUtil.getTypeSchema("VARCHAR");
    assertThat(schema, sameInstance(Schema.STRING_SCHEMA));
  }

  @Test
  public void shouldGetTheCorrectSchemaForArray() {
    Schema schema = SchemaUtil.getTypeSchema("ARRAY<DOUBLE>");
    assertThat(schema.type(), sameInstance(Schema.Type.ARRAY));
  }

  @Test
  public void shouldGetTheCorrectSchemaForMap() {
    Schema schema = SchemaUtil.getTypeSchema("MAP<VARCHAR, DOUBLE>");
    assertThat(schema.type(), sameInstance(Schema.Type.MAP));
  }

  @Test
  public void shouldFailForIncorrectSchema() {

    try {
      Schema schema8 = SchemaUtil.getTypeSchema("BYTES");
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), equalTo("Unsupported type: BYTES"));
    }
  }

  @Test
  public void shouldGetTheCorrectFieldIndex() {
    int index1 = SchemaUtil.getFieldIndexByName(schema, "orderid".toUpperCase());
    int index2 = SchemaUtil.getFieldIndexByName(schema, "itemid".toUpperCase());
    int index3 = SchemaUtil.getFieldIndexByName(schema, "mapcol".toUpperCase());


    assertThat("Incorrect index.", index1, equalTo(1));
    assertThat("Incorrect index.", index2, equalTo(2));
    assertThat("Incorrect index.", index3, equalTo(5));

  }

  @Test
  public void shouldHandleInvalidFieldIndexCorrectly() {
    int index = SchemaUtil.getFieldIndexByName(schema, "mapcol1".toUpperCase());
    assertThat("Incorrect index.", index, equalTo(-1));
  }

  @Test
  public void shouldBuildTheCorrectSchemaWithAndWithoutAlias() {
    String alias = "Hello";
    Schema schemaWithAlias = SchemaUtil.buildSchemaWithAlias(schema, alias);
    assertThat("Incorrect schema field count.", schemaWithAlias.fields().size() == schema.fields()
        .size());
    for (int i = 0; i < schemaWithAlias.fields().size(); i++) {
      Field fieldWithAlias = schemaWithAlias.fields().get(i);
      Field field = schema.fields().get(i);
      assertThat(fieldWithAlias.name(), equalTo(alias + "." + field.name()));
    }
    Schema schemaWithoutAlias = SchemaUtil.getSchemaWithNoAlias(schemaWithAlias);
    assertThat("Incorrect schema field count.", schemaWithAlias.fields().size() == schema.fields().size());
    for (int i = 0; i < schemaWithoutAlias.fields().size(); i++) {
      Field fieldWithAlias = schemaWithoutAlias.fields().get(i);
      Field field = schema.fields().get(i);
      assertThat("Incorrect field name.", fieldWithAlias.name().equals(field.name()));
    }
  }

  @Test
  public void shouldGetTheCorrectJavaCastClass() {
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.BOOLEAN_SCHEMA), equalTo("(Boolean)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.INT32_SCHEMA), equalTo("(Integer)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.INT64_SCHEMA), equalTo("(Long)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.FLOAT64_SCHEMA), equalTo("(Double)"));
    assertThat("Incorrect class.", SchemaUtil.getJavaCastString(Schema.STRING_SCHEMA), equalTo("(String)"));
  }

  @Test
  public void shouldAddAndRemoveImplicitColumns() {
    Schema withImplicit = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);

    assertThat("Invalid field count.", withImplicit.fields().size() == 8);
    assertThat("Field name should be ROWTIME.", withImplicit.fields().get(0).name(), equalTo(SchemaUtil.ROWTIME_NAME));
    assertThat("Field name should ne ROWKEY.", withImplicit.fields().get(1).name(), equalTo(SchemaUtil.ROWKEY_NAME));

    Schema withoutImplicit = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(withImplicit);
    assertThat("Invalid field count.", withoutImplicit.fields().size() == 6);
    assertThat("Invalid field name.", withoutImplicit.fields().get(0).name(), equalTo("ORDERTIME"));
    assertThat("Invalid field name.", withoutImplicit.fields().get(1).name(), equalTo("ORDERID"));
  }

  @Test
  public void shouldGetTheSchemaDefString() {
    String schemaDef = SchemaUtil.getSchemaDefinitionString(schema);
    assertThat("Invalid schema def.", schemaDef.equals("[ORDERTIME : INT64 , ORDERID : INT64 , "
                                                   + "ITEMID : STRING , ORDERUNITS : "
                       + "FLOAT64 , ARRAYCOL : ARRAY , MAPCOL : MAP]"));
  }

  @Test
  public void shouldGetCorrectSqlType() {
    String sqlType1 = SchemaUtil.getSqlTypeName(Schema.BOOLEAN_SCHEMA);
    String sqlType2 = SchemaUtil.getSqlTypeName(Schema.INT32_SCHEMA);
    String sqlType3 = SchemaUtil.getSqlTypeName(Schema.INT64_SCHEMA);
    String sqlType4 = SchemaUtil.getSqlTypeName(Schema.FLOAT64_SCHEMA);
    String sqlType5 = SchemaUtil.getSqlTypeName(Schema.STRING_SCHEMA);
    String sqlType6 = SchemaUtil.getSqlTypeName(SchemaBuilder.array(Schema.FLOAT64_SCHEMA));
    String sqlType7 = SchemaUtil.getSqlTypeName(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));

    assertThat("Invalid SQL type.", sqlType1, equalTo("BOOLEAN"));
    assertThat("Invalid SQL type.", sqlType2, equalTo("INT"));
    assertThat("Invalid SQL type.", sqlType3, equalTo("BIGINT"));
    assertThat("Invalid SQL type.", sqlType4, equalTo("DOUBLE"));
    assertThat("Invalid SQL type.", sqlType5, equalTo("VARCHAR"));
    assertThat("Invalid SQL type.", sqlType6, equalTo("ARRAY<DOUBLE>"));
    assertThat("Invalid SQL type.", sqlType7, equalTo("MAP<VARCHAR,DOUBLE>"));
  }

  @Test
  public void shouldStripAliasFromFieldName() {
    Schema schemaWithAlias = SchemaUtil.buildSchemaWithAlias(schema, "alias");
    assertThat("Invalid field name", SchemaUtil.getFieldNameWithNoAlias(schemaWithAlias.fields().get(0)),
        equalTo(schema.fields().get(0).name()));
  }

  @Test
  public void shouldReturnFieldNameWithoutAliasAsIs() {
    assertThat("Invalid field name", SchemaUtil.getFieldNameWithNoAlias(schema.fields().get(0)),
        equalTo(schema.fields().get(0).name()));
  }
}
