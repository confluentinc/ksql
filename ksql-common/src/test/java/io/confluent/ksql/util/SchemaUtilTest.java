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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
  public void shouldFailForGetJavaTypeOnUnsuppportedConnectType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unexpected schema type: Schema{INT8}");

    // When:
    SchemaUtil.getJavaType(Schema.INT8_SCHEMA);
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
  public void shouldPassIsNumberForInt() {
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.INT32_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForBigint() {
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.INT64_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForDouble() {
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(SchemaUtil.isNumber(Schema.FLOAT64_SCHEMA), is(true));
  }

  @Test
  public void shouldPassIsNumberForDecimal() {
    assertThat(SchemaUtil.isNumber(DecimalUtil.builder(2, 1)), is(true));
  }

  @Test
  public void shouldFailIsNumberForBoolean() {
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_BOOLEAN_SCHEMA), is(false));
    assertThat(SchemaUtil.isNumber(Schema.BOOLEAN_SCHEMA), is(false));
  }

  @Test
  public void shouldFailIsNumberForString() {
    assertThat(SchemaUtil.isNumber(Schema.OPTIONAL_STRING_SCHEMA), is(false));
    assertThat(SchemaUtil.isNumber(Schema.STRING_SCHEMA), is(false));
  }

  @Test
  public void shouldFailINonCompatibleSchemas() {
    assertThat(SchemaUtil.areCompatible(SqlTypes.STRING, ParamTypes.INTEGER), is(false));

    assertThat(SchemaUtil.areCompatible(SqlTypes.STRING, GenericType.of("T")), is(false));

    assertThat(
        SchemaUtil.areCompatible(SqlTypes.array(SqlTypes.INTEGER), ArrayType.of(ParamTypes.STRING)),
        is(false));

    assertThat(SchemaUtil.areCompatible(
        SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build(),
        StructType.builder().field("a", ParamTypes.DOUBLE).build()),
        is(false));

    assertThat(SchemaUtil.areCompatible(
        SqlTypes.map(SqlTypes.decimal(1, 1)),
        MapType.of(ParamTypes.INTEGER)),
        is(false));
  }

  @Test
  public void shouldPassCompatibleSchemas() {
    assertThat(SchemaUtil.areCompatible(SqlTypes.STRING, ParamTypes.STRING),
        is(true));

    assertThat(
        SchemaUtil.areCompatible(SqlTypes.array(SqlTypes.INTEGER), ArrayType.of(ParamTypes.INTEGER)),
        is(true));

    assertThat(SchemaUtil.areCompatible(
        SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build(),
        StructType.builder().field("a", ParamTypes.DECIMAL).build()),
        is(true));

    assertThat(SchemaUtil.areCompatible(
        SqlTypes.map(SqlTypes.decimal(1, 1)),
        MapType.of(ParamTypes.DECIMAL)),
        is(true));

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

