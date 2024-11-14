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

import static io.confluent.connect.json.JsonSchemaData.JSON_TYPE_ONE_OF;
import static io.confluent.ksql.schema.ksql.SchemaConverters.javaToSqlConverter;
import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToConnectConverter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class SchemaConvertersTest {

  private static final Schema CONNECT_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
  private static final Schema CONNECT_INTEGER_SCHEMA = SchemaBuilder.int32().optional().build();
  private static final Schema CONNECT_BIGINT_SCHEMA = SchemaBuilder.int64().optional().build();
  private static final Schema CONNECT_DOUBLE_SCHEMA = SchemaBuilder.float64().optional().build();
  private static final Schema CONNECT_STRING_SCHEMA = SchemaBuilder.string().optional().build();
  private static final Schema CONNECT_BYTES_SCHEMA = SchemaBuilder.bytes().optional().build();
  private static final Schema CONNECT_TIME_SCHEMA =
      org.apache.kafka.connect.data.Time.builder().optional().schema();
  private static final Schema CONNECT_DATE_SCHEMA =
      org.apache.kafka.connect.data.Date.builder().optional().schema();
  private static final Schema CONNECT_TIMESTAMP_SCHEMA =
      org.apache.kafka.connect.data.Timestamp.builder().optional().schema();

  private static final BiMap<SqlType, Schema> SQL_TO_LOGICAL = ImmutableBiMap.<SqlType, Schema>builder()
      .put(SqlTypes.BOOLEAN, CONNECT_BOOLEAN_SCHEMA)
      .put(SqlTypes.INTEGER, CONNECT_INTEGER_SCHEMA)
      .put(SqlTypes.BIGINT, CONNECT_BIGINT_SCHEMA)
      .put(SqlTypes.DOUBLE, CONNECT_DOUBLE_SCHEMA)
      .put(SqlTypes.STRING, CONNECT_STRING_SCHEMA)
      .put(SqlTypes.TIME, CONNECT_TIME_SCHEMA)
      .put(SqlTypes.DATE, CONNECT_DATE_SCHEMA)
      .put(SqlTypes.TIMESTAMP, CONNECT_TIMESTAMP_SCHEMA)
      .put(SqlTypes.BYTES, CONNECT_BYTES_SCHEMA)
      .put(SqlArray.of(SqlTypes.INTEGER), SchemaBuilder
          .array(Schema.OPTIONAL_INT32_SCHEMA)
          .optional()
          .build()
      )
      .put(SqlDecimal.of(2, 1), DecimalUtil.builder(2, 1).build())
      .put(SqlMap.of(SqlTypes.BIGINT, SqlTypes.INTEGER), SchemaBuilder
          .map(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
          .optional()
          .build()
      )
      .put(SqlStruct.builder()
              .field("f0", SqlTypes.INTEGER)
              .build(),
          SchemaBuilder.struct()
              .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
              .optional()
              .build())
      .build();

  private static final BiMap<SqlBaseType, Class<?>> SQL_TO_JAVA = ImmutableBiMap
      .<SqlBaseType, Class<?>>builder()
      .put(SqlBaseType.BOOLEAN, Boolean.class)
      .put(SqlBaseType.INTEGER, Integer.class)
      .put(SqlBaseType.BIGINT, Long.class)
      .put(SqlBaseType.DOUBLE, Double.class)
      .put(SqlBaseType.STRING, String.class)
      .put(SqlBaseType.DECIMAL, BigDecimal.class)
      .put(SqlBaseType.ARRAY, List.class)
      .put(SqlBaseType.MAP, Map.class)
      .put(SqlBaseType.STRUCT, Struct.class)
      .put(SqlBaseType.TIME, Time.class)
      .put(SqlBaseType.DATE, Date.class)
      .put(SqlBaseType.TIMESTAMP, Timestamp.class)
      .put(SqlBaseType.BYTES, ByteBuffer.class)
      .build();

  private static final BiMap<SqlType, ParamType> SQL_TO_FUNCTION = ImmutableBiMap
      .<SqlType, ParamType>builder()
      .put(SqlTypes.BOOLEAN, ParamTypes.BOOLEAN)
      .put(SqlTypes.INTEGER, ParamTypes.INTEGER)
      .put(SqlTypes.BIGINT, ParamTypes.LONG)
      .put(SqlTypes.DOUBLE, ParamTypes.DOUBLE)
      .put(SqlTypes.STRING, ParamTypes.STRING)
      .put(SqlTypes.TIME, ParamTypes.TIME)
      .put(SqlTypes.DATE, ParamTypes.DATE)
      .put(SqlTypes.TIMESTAMP, ParamTypes.TIMESTAMP)
      .put(SqlTypes.BYTES, ParamTypes.BYTES)
      .put(SqlArray.of(SqlTypes.INTEGER), ArrayType.of(ParamTypes.INTEGER))
      .put(SqlDecimal.of(2, 1), ParamTypes.DECIMAL)
      .put(
          SqlMap.of(SqlTypes.BIGINT, SqlTypes.INTEGER),
          MapType.of(ParamTypes.LONG, ParamTypes.INTEGER)
      )
      .put(SqlStruct.builder()
              .field("f0", SqlTypes.INTEGER)
              .build(),
          StructType.builder()
              .field("f0", ParamTypes.INTEGER)
              .build())
      .build();

  private static final Set<ParamType> REQUIRES_SCHEMA_SPEC = ImmutableSet.of(
      ParamTypes.DECIMAL
  );

  private static final Schema STRUCT_LOGICAL_TYPE = SchemaBuilder.struct()
      .field("F0", SchemaBuilder.int32().optional().build())
      .optional()
      .build();

  private static final Schema NESTED_LOGICAL_TYPE = SchemaBuilder.struct()
      .field("ARRAY", SchemaBuilder.array(STRUCT_LOGICAL_TYPE).optional().build())
      .field("MAP",
          SchemaBuilder.map(CONNECT_DOUBLE_SCHEMA, STRUCT_LOGICAL_TYPE).optional().build())
      .field("STRUCT", STRUCT_LOGICAL_TYPE)
      .optional()
      .build();

  private static final SqlType STRUCT_SQL_TYPE = SqlStruct.builder()
      .field("F0", SqlTypes.INTEGER)
      .build();

  private static final SqlType NESTED_SQL_TYPE = SqlStruct.builder()
      .field("ARRAY", SqlArray.of(STRUCT_SQL_TYPE))
      .field("MAP", SqlMap.of(SqlTypes.DOUBLE, STRUCT_SQL_TYPE))
      .field("STRUCT", STRUCT_SQL_TYPE)
      .build();

  @Test
  public void shouldHaveConnectTestsForAllSqlTypes() {
    final Set<SqlBaseType> tested = SQL_TO_LOGICAL.keySet().stream()
        .map(SqlType::baseType)
        .collect(Collectors.toSet());

    final ImmutableSet<SqlBaseType> allTypes = ImmutableSet.copyOf(SqlBaseType.values());

    assertThat("If this test fails then there has been a new SQL type added and this test "
        + "file needs updating to cover that new type", tested, is(allTypes));
  }

  @Test
  public void shouldGetLogicalForEverySqlType() {
    for (final Entry<SqlType, Schema> entry : SQL_TO_LOGICAL.entrySet()) {
      final SqlType sqlType = entry.getKey();
      final Schema logical = entry.getValue();
      final Schema result = SchemaConverters.sqlToConnectConverter().toConnectSchema(sqlType);
      assertThat(result, is(logical));
    }
  }

  @Test
  public void shouldGetSqlTypeForEveryLogicalType() {
    SQL_TO_LOGICAL.inverse().forEach((logical, sqlType) -> {
      assertThat(SchemaConverters.connectToSqlConverter().toSqlType(logical), is(sqlType));
    });
  }

  @Test
  public void shouldHaveJavaTestsForAllSqlTypes() {
    final Set<SqlBaseType> tested = new HashSet<>(SQL_TO_JAVA.keySet());

    final ImmutableSet<SqlBaseType> allTypes = ImmutableSet.copyOf(SqlBaseType.values());

    assertThat("If this test fails then there has been a new SQL type added and this test "
        + "file needs updating to cover that new type", tested, is(allTypes));
  }

  @Test
  public void shouldGetJavaTypesForAllSqlTypes() {
    for (final Entry<SqlBaseType, Class<?>> entry : SQL_TO_JAVA.entrySet()) {
      final SqlBaseType sqlType = entry.getKey();
      final Class<?> javaType = entry.getValue();
      final Class<?> result = SchemaConverters.sqlToJavaConverter().toJavaType(sqlType);
      assertThat(result, equalTo(javaType));
    }
  }

  @Test
  public void shouldGetSqlTypeForAllJavaTypes() {
    SQL_TO_JAVA.inverse().forEach((java, sqlType) -> {
      assertThat(javaToSqlConverter().toSqlType(java), is(sqlType));
    });
  }

  @Test
  public void shouldGetSqArrayForImplementationsOfJavaList() {
    ImmutableList.<Class<?>>of(
        ArrayList.class,
        ImmutableList.class
    ).forEach(javaType -> {
      assertThat(javaToSqlConverter().toSqlType(javaType), is(SqlBaseType.ARRAY));
    });
  }

  @Test
  public void shouldGetSqlMapForImplementationsOfJavaMap() {
    ImmutableList.<Class<?>>of(
        HashMap.class,
        ImmutableMap.class
    ).forEach(javaType -> {
      assertThat(javaToSqlConverter().toSqlType(javaType), is(SqlBaseType.MAP));
    });
  }

  @Test
  public void shouldConvertNestedComplexToSql() {
    assertThat(SchemaConverters.connectToSqlConverter().toSqlType(NESTED_LOGICAL_TYPE), is(NESTED_SQL_TYPE));
  }

  @Test
  public void shouldConvertNestedComplexFromSql() {
    assertThat(SchemaConverters.sqlToConnectConverter().toConnectSchema(NESTED_SQL_TYPE), is(NESTED_LOGICAL_TYPE));
  }

  @Test
  public void shouldThrowOnUnsupportedConnectSchemaType() {
    // Given:
    final Schema unsupported = SchemaBuilder.int8().build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> SchemaConverters.connectToSqlConverter().toSqlType(unsupported)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unexpected schema type: Schema{INT8}"));
  }

  @Test
  public void shouldConvertJavaBooleanToSqlBoolean() {
    assertThat(javaToSqlConverter().toSqlType(Boolean.class),
               is(SqlBaseType.BOOLEAN));
  }

  @Test
  public void shouldConvertJavaIntegerToSqlInteger() {
    assertThat(javaToSqlConverter().toSqlType(Integer.class),
               is(SqlBaseType.INTEGER));
  }

  @Test
  public void shouldConvertJavaLongToSqlBigInt() {
    assertThat(javaToSqlConverter().toSqlType(Long.class), is(SqlBaseType.BIGINT));
  }

  @Test
  public void shouldConvertJavaDoubleToSqlDouble() {
    assertThat(javaToSqlConverter().toSqlType(Double.class),
               is(SqlBaseType.DOUBLE));
  }

  @Test
  public void shouldConvertJavaStringToSqlString() {
    assertThat(javaToSqlConverter().toSqlType(String.class),
               is(SqlBaseType.STRING));
  }

  @Test
  public void shouldConvertJavaStringToSqlTimestamp() {
    assertThat(javaToSqlConverter().toSqlType(Timestamp.class),
        is(SqlBaseType.TIMESTAMP));
  }

  @Test
  public void shouldConvertOneOfUnionTypeFromSqlToConnect() {
    // Given:
    SqlStruct sqlStruct = SqlStruct.builder()
        .field("io.confluent.connect.json.OneOf.field.0", SqlPrimitiveType.of(SqlBaseType.STRING))
        .field("io.confluent.connect.json.OneOf.field.1", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("io.confluent.connect.json.OneOf.field.2", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .build();

    // When:
    Schema connectSchema = sqlToConnectConverter().toConnectSchema(sqlStruct);

    // Then:
    assertThat(connectSchema.type(), is(Schema.Type.STRUCT));
    assertEquals(JSON_TYPE_ONE_OF, connectSchema.schema().name());
  }

  @Test
  public void shouldConvertGeneralizedUnionTypeFromSqlToConnect() {
    // Given:
    SqlStruct sqlStruct = SqlStruct.builder()
        .field("connect_union_field_0", SqlPrimitiveType.of(SqlBaseType.STRING))
        .field("connect_union_field_1", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("connect_union_field_2", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .build();

    // When:
    Schema connectSchema = sqlToConnectConverter().toConnectSchema(sqlStruct);

    // Then:
    assertThat(connectSchema.type(), is(Schema.Type.STRUCT));
    assertFalse(connectSchema.schema().parameters().isEmpty());
    assertTrue(connectSchema.schema().parameters().containsKey(JsonSchemaData.GENERALIZED_TYPE_UNION));
  }

  @Test
  public void shouldConvertNestedUnionTypeFromSqlToConnect() {
    // Given:
    SqlStruct inner = SqlStruct.builder()
        .field("io.confluent.connect.json.OneOf.field.0", SqlPrimitiveType.of(SqlBaseType.STRING))
        .field("io.confluent.connect.json.OneOf.field.1", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("io.confluent.connect.json.OneOf.field.2", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .build();

    SqlStruct outer = SqlStruct.builder()
        .field("foo", inner)
        .field("bar", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("baz", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .build();

    // When:
    Schema connectSchema = sqlToConnectConverter().toConnectSchema(outer);
    Schema fooSchema = connectSchema.field("foo").schema();

    // Then:
    assertThat(connectSchema.type(), is(Schema.Type.STRUCT));
    assertNull(connectSchema.schema().name());
    assertThat(fooSchema.type(), is(Schema.Type.STRUCT));
    assertEquals(JSON_TYPE_ONE_OF, fooSchema.schema().name());
  }

  @Test
  public void shouldConvertRegularStructFromSqlToConnect() {
    // Given:
    SqlStruct sqlStruct = SqlStruct.builder()
        .field("foo", SqlPrimitiveType.of(SqlBaseType.STRING))
        .field("bar", SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .field("baz", SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .build();

    // When:
    Schema connectSchema = sqlToConnectConverter().toConnectSchema(sqlStruct);

    // Then:
    assertThat(connectSchema.type(), is(Schema.Type.STRUCT));
    assertNull(connectSchema.schema().name());
  }

  @Test
  public void shouldThrowOnUnknownJavaType() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> javaToSqlConverter().toSqlType(double.class)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unexpected java type: " + double.class));
  }

  @Test
  public void shouldCoverAllSqlToFunction() {
    final Set<SqlBaseType> tested = SQL_TO_FUNCTION.keySet().stream()
        .map(SqlType::baseType)
        .collect(Collectors.toSet());

    final ImmutableSet<SqlBaseType> allTypes = ImmutableSet.copyOf(SqlBaseType.values());

    assertThat("If this test fails then there has been a new SQL type added and this test "
        + "file needs updating to cover that new type", tested, is(allTypes));
  }

  @Test
  public void shouldGetParamTypesForAllSqlTypes() {
    for (final Entry<SqlType, ParamType> entry : SQL_TO_FUNCTION.entrySet()) {
      final SqlType sqlType = entry.getKey();
      final ParamType javaType = entry.getValue();
      final ParamType result = SchemaConverters.sqlToFunctionConverter().toFunctionType(sqlType);
      assertThat(result, equalTo(javaType));
    }
  }

  @Test
  public void shouldGetSqlTypeForAllParamTypes() {
    for (Entry<ParamType, SqlType> entry : SQL_TO_FUNCTION.inverse().entrySet()) {
      ParamType param = entry.getKey();
      if (REQUIRES_SCHEMA_SPEC.contains(param)) {
        continue;
      }

      SqlType sqlType = entry.getValue();
      assertThat(SchemaConverters.functionToSqlConverter().toSqlType(param), is(sqlType));
    }
  }

}
