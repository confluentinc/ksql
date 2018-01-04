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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;
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
  public void shouldCreateCorrectAvroSchema() throws IOException {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder
        .field("ordertime", Schema.INT64_SCHEMA)
        .field("orderid", Schema.STRING_SCHEMA)
        .field("itemid", Schema.STRING_SCHEMA)
        .field("orderunits", Schema.FLOAT64_SCHEMA)
        .field("arraycol", SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("mapcol", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));
    String avroSchemaString = SchemaUtil.buildAvroSchema(schemaBuilder.build(), "orders");
    assertThat("", avroSchemaString.equals("{\"type\":\"record\",\"name\":\"orders\",\"namespace\":\"ksql\",\"fields\":[{\"name\":\"ordertime\",\"type\":\"long\"},{\"name\":\"orderid\",\"type\":\"string\"},{\"name\":\"itemid\",\"type\":\"string\"},{\"name\":\"orderunits\",\"type\":\"double\"},{\"name\":\"arraycol\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"mapcol\",\"type\":{\"type\":\"map\",\"values\":\"double\"}}]}"));
  }


  @Test
  public void shouldGetTheCorrectJavaType() {
    Schema schema1 = Schema.BOOLEAN_SCHEMA;
    Schema schema2 = Schema.INT32_SCHEMA;
    Schema schema3 = Schema.INT64_SCHEMA;
    Schema schema4 = Schema.FLOAT64_SCHEMA;
    Schema schema5 = Schema.STRING_SCHEMA;
    Schema schema6 = SchemaBuilder.array(Schema.FLOAT64_SCHEMA);
    Schema schema7 = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA);

    Class class1 = SchemaUtil.getJavaType(schema1);
    Class class2 = SchemaUtil.getJavaType(schema2);
    Class class3 = SchemaUtil.getJavaType(schema3);
    Class class4 = SchemaUtil.getJavaType(schema4);
    Class class5 = SchemaUtil.getJavaType(schema5);
    Class class6 = SchemaUtil.getJavaType(schema6);
    Class class7 = SchemaUtil.getJavaType(schema7);

    assertThat(class1, equalTo(Boolean.class));
    assertThat(class2, equalTo(Integer.class));
    assertThat(class3, equalTo(Long.class));
    assertThat(class4, equalTo(Double.class));
    assertThat(class5, equalTo(String.class));
    assertThat(class6, equalTo(new Double[]{}.getClass()));
    assertThat(class7, equalTo(HashMap.class));

    try {
      Class class8 = SchemaUtil.getJavaType(Schema.BYTES_SCHEMA);
    } catch (KsqlException ksqlException) {
      Assert.assertTrue(ksqlException.getMessage().equals("Type is not supported: BYTES"));
      return;
    }
    Assert.fail();
  }

  @Test
  public void shouldGetTheCorrectFieldName() {
    Optional<Field> field = SchemaUtil.getFieldByName(schema, "orderid".toUpperCase());
    Assert.assertTrue(field.isPresent());
    Assert.assertTrue(field.get().schema() == Schema.INT64_SCHEMA);
    Assert.assertTrue(field.get().name().equalsIgnoreCase("orderid"));

    Optional<Field> field1 = SchemaUtil.getFieldByName(schema, "orderid");
    Assert.assertFalse(field1.isPresent());
  }

  @Test
  public void shouldGetTheCorrectSchema() {
    Schema schema1 = SchemaUtil.getTypeSchema("VARCHAR");
    Schema schema2 = SchemaUtil.getTypeSchema("INT");
    Schema schema3 = SchemaUtil.getTypeSchema("BIGINT");
    Schema schema4 = SchemaUtil.getTypeSchema("DOUBLE");
    Schema schema5 = SchemaUtil.getTypeSchema("BOOLEAN");
    Schema schema6 = SchemaUtil.getTypeSchema("ARRAY<DOUBLE>");
    Schema schema7 = SchemaUtil.getTypeSchema("MAP<VARCHAR, DOUBLE>");

    Assert.assertTrue(schema1 == Schema.STRING_SCHEMA);
    Assert.assertTrue(schema2 == Schema.INT32_SCHEMA);
    Assert.assertTrue(schema3 == Schema.INT64_SCHEMA);
    Assert.assertTrue(schema4 == Schema.FLOAT64_SCHEMA);
    Assert.assertTrue(schema5 == Schema.BOOLEAN_SCHEMA);
    Assert.assertTrue(schema6.type() == Schema.Type.ARRAY);
    Assert.assertTrue(schema7.type() == Schema.Type.MAP);

    try {
      Schema schema8 = SchemaUtil.getTypeSchema("BYTES");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().equals("Unsupported type: BYTES"));
      return;
    }
    Assert.fail();
  }

  @Test
  public void shouldGetTheCorrectFieldIndex() {
    int index1 = SchemaUtil.getFieldIndexByName(schema, "orderid".toUpperCase());
    int index2 = SchemaUtil.getFieldIndexByName(schema, "itemid".toUpperCase());
    int index3 = SchemaUtil.getFieldIndexByName(schema, "mapcol".toUpperCase());
    int index4 = SchemaUtil.getFieldIndexByName(schema, "mapcol1".toUpperCase());

    assertThat("Incorrect index.", index1, equalTo(1));
    assertThat("Incorrect index.", index2, equalTo(2));
    assertThat("Incorrect index.", index3, equalTo(5));
    assertThat("Incorrect index.", index4, equalTo(-1));
  }

  @Test
  public void shouldBuildTheCorrectSchemaWithAndWithoutAlias() {
    String alias = "Hello";
    Schema schemaWithAlias = SchemaUtil.buildSchemaWithAlias(schema, alias);
    Assert.assertTrue(schemaWithAlias.fields().size() == schema.fields().size());
    for (int i = 0; i < schemaWithAlias.fields().size(); i++) {
      Field fieldWithAlias = schemaWithAlias.fields().get(i);
      Field field = schema.fields().get(i);
      Assert.assertTrue(fieldWithAlias.name().equals(alias + "." + field.name()));
    }
    Schema schemaWithoutAlias = SchemaUtil.getSchemaWithNoAlias(schemaWithAlias);
    Assert.assertTrue(schemaWithAlias.fields().size() == schema.fields().size());
    for (int i = 0; i < schemaWithoutAlias.fields().size(); i++) {
      Field fieldWithAlias = schemaWithoutAlias.fields().get(i);
      Field field = schema.fields().get(i);
      Assert.assertTrue(fieldWithAlias.name().equals(field.name()));
    }
  }

  @Test
  public void shouldGetTheCorrectJavaCastClass() {
    Assert.assertTrue(SchemaUtil.getJavaCastString(Schema.BOOLEAN_SCHEMA).equals("(Boolean)"));
    Assert.assertTrue(SchemaUtil.getJavaCastString(Schema.INT32_SCHEMA).equals("(Integer)"));
    Assert.assertTrue(SchemaUtil.getJavaCastString(Schema.INT64_SCHEMA).equals("(Long)"));
    Assert.assertTrue(SchemaUtil.getJavaCastString(Schema.FLOAT64_SCHEMA).equals("(Double)"));
    Assert.assertTrue(SchemaUtil.getJavaCastString(Schema.STRING_SCHEMA).equals("(String)"));
  }

  @Test
  public void shouldAddAndRemoveImplicitColumns() {
    Schema withImplicit = SchemaUtil.addImplicitRowTimeRowKeyToSchema(schema);

    Assert.assertTrue(withImplicit.fields().size() == 8);
    Assert.assertTrue(withImplicit.fields().get(0).name().equals(SchemaUtil.ROWTIME_NAME));
    Assert.assertTrue(withImplicit.fields().get(1).name().equals(SchemaUtil.ROWKEY_NAME));

    Schema withoutImplicit = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(withImplicit);
    Assert.assertTrue(withoutImplicit.fields().size() == 6);
    Assert.assertTrue(withoutImplicit.fields().get(0).name().equals("ORDERTIME"));
    Assert.assertTrue(withoutImplicit.fields().get(1).name().equals("ORDERID"));
  }

  @Test
  public void shouldGetTheSchemaDefString() {
    String schemaDef = SchemaUtil.getSchemaDefinitionString(schema);
    Assert.assertTrue(schemaDef.equals("[ORDERTIME : INT64 , ORDERID : INT64 , ITEMID : STRING , ORDERUNITS : FLOAT64 , ARRAYCOL : ARRAY , MAPCOL : MAP]"));
  }

  @Test
  public void shouldGetCorrectSqlType() {
    String sqlType1 = SchemaUtil.getSQLTypeName(Schema.BOOLEAN_SCHEMA);
    String sqlType2 = SchemaUtil.getSQLTypeName(Schema.INT32_SCHEMA);
    String sqlType3 = SchemaUtil.getSQLTypeName(Schema.INT64_SCHEMA);
    String sqlType4 = SchemaUtil.getSQLTypeName(Schema.FLOAT64_SCHEMA);
    String sqlType5 = SchemaUtil.getSQLTypeName(Schema.STRING_SCHEMA);
    String sqlType6 = SchemaUtil.getSQLTypeName(SchemaBuilder.array(Schema.FLOAT64_SCHEMA));
    String sqlType7 = SchemaUtil.getSQLTypeName(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA));

    Assert.assertTrue(sqlType1.equals("BOOLEAN"));
    Assert.assertTrue(sqlType2.equals("INT"));
    Assert.assertTrue(sqlType3.equals("BIGINT"));
    Assert.assertTrue(sqlType4.equals("DOUBLE"));
    Assert.assertTrue(sqlType5.equals("VARCHAR"));
    Assert.assertTrue(sqlType6.equals("ARRAY<DOUBLE>"));
    Assert.assertTrue(sqlType7.equals("MAP<VARCHAR,DOUBLE>"));
  }

}
