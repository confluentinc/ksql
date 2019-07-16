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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SchemaConvertersTest {

  private static final Schema LOGICAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
  private static final Schema LOGICAL_INT_SCHEMA = SchemaBuilder.int32().optional().build();
  private static final Schema LOGICAL_BIGINT_SCHEMA = SchemaBuilder.int64().optional().build();
  private static final Schema LOGICAL_DOUBLE_SCHEMA = SchemaBuilder.float64().optional().build();
  private static final Schema LOGICAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();

  private static final BiMap<SqlType, Schema> SQL_TO_LOGICAL = ImmutableBiMap.<SqlType, Schema>builder()
      .put(SqlTypes.BOOLEAN, LOGICAL_BOOLEAN_SCHEMA)
      .put(SqlTypes.INTEGER, LOGICAL_INT_SCHEMA)
      .put(SqlTypes.BIGINT, LOGICAL_BIGINT_SCHEMA)
      .put(SqlTypes.DOUBLE, LOGICAL_DOUBLE_SCHEMA)
      .put(SqlTypes.STRING, LOGICAL_STRING_SCHEMA)
      .put(SqlArray.of(SqlTypes.INTEGER),
          SchemaBuilder.array(SchemaConverters.INTEGER).optional().build())
      .put(SqlDecimal.of(2, 1), DecimalUtil.builder(2, 1).build())
      .put(SqlMap.of(SqlTypes.INTEGER),
          SchemaBuilder.map(SchemaConverters.STRING, SchemaConverters.INTEGER).optional().build())
      .put(SqlStruct.builder()
              .field("f0", SqlTypes.INTEGER)
              .build(),
          SchemaBuilder.struct().field("f0", SchemaConverters.INTEGER).optional().build())
      .build();

  private static final Schema STRUCT_LOGICAL_TYPE = SchemaBuilder.struct()
      .field("F0", SchemaBuilder.int32().optional().build())
      .optional()
      .build();

  private static final Schema NESTED_LOGICAL_TYPE = SchemaBuilder.struct()
      .field("ARRAY", SchemaBuilder.array(STRUCT_LOGICAL_TYPE).optional().build())
      .field("MAP",
          SchemaBuilder.map(LOGICAL_STRING_SCHEMA, STRUCT_LOGICAL_TYPE).optional().build())
      .field("STRUCT", STRUCT_LOGICAL_TYPE)
      .optional()
      .build();

  private static final SqlType STRUCT_SQL_TYPE = SqlStruct.builder()
      .field("F0", SqlTypes.INTEGER)
      .build();

  private static final SqlType NESTED_SQL_TYPE = SqlStruct.builder()
      .field("ARRAY", SqlArray.of(STRUCT_SQL_TYPE))
      .field("MAP", SqlMap.of(STRUCT_SQL_TYPE))
      .field("STRUCT", STRUCT_SQL_TYPE)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldHaveTestsForAllTypes() {
    final Set<SqlBaseType> tested = SQL_TO_LOGICAL.keySet().stream()
        .map(SqlType::baseType)
        .collect(Collectors.toSet());

    final ImmutableSet<SqlBaseType> allTypes = ImmutableSet.copyOf(SqlBaseType.values());

    assertThat("If this test fails then there has been a new SQL type added and this test "
        + "file needs updating to cover that new type", tested, is(allTypes));
  }

  @Test
  public void shouldGetLogicalForEverySqlType() {
    for (Entry<SqlType, Schema> entry : SQL_TO_LOGICAL.entrySet()) {
      final SqlType sqlType = entry.getKey();
      final Schema logical = entry.getValue();
      final Schema result = SchemaConverters.sqlToLogicalConverter().fromSqlType(sqlType);
      assertThat(result, is(logical));
    }
  }

  @Test
  public void shouldGetSqlTypeForEveryLogicalType() {
    SQL_TO_LOGICAL.inverse().forEach((logical, sqlType) -> {
      if (!(sqlType instanceof SqlDecimal)) {
        assertThat(SchemaConverters.logicalToSqlConverter().toSqlType(logical), is(sqlType));
      }
    });
  }

  @Test
  public void shouldConvertNestedComplexToSql() {
    assertThat(SchemaConverters.logicalToSqlConverter().toSqlType(NESTED_LOGICAL_TYPE), is(NESTED_SQL_TYPE));
  }

  @Test
  public void shouldConvertNestedComplexFromSql() {
    assertThat(SchemaConverters.sqlToLogicalConverter().fromSqlType(NESTED_SQL_TYPE), is(NESTED_LOGICAL_TYPE));
  }

  @Test
  public void shouldThrowOnNonStringKeyedMap() {
    // Given:
    final Schema mapSchema = SchemaBuilder
        .map(LOGICAL_BIGINT_SCHEMA, LOGICAL_DOUBLE_SCHEMA).optional()
        .build();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unsupported map key type: Schema{INT64}");

    // When:
    SchemaConverters.logicalToSqlConverter().toSqlType(mapSchema);
  }

  @Test
  public void shouldThrowOnUnsupportedConnectSchemaType() {
    // Given:
    final Schema unsupported = SchemaBuilder.int8().build();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unexpected logical type: Schema{INT8}");

    // When:
    SchemaConverters.logicalToSqlConverter().toSqlType(unsupported);
  }

  @Test
  public void shouldConvertJavaBooleanToSqlBoolean() {
    assertThat(SchemaConverters.javaToSqlConverter().toSqlType(Boolean.class),
        is(SqlBaseType.BOOLEAN));
  }

  @Test
  public void shouldConvertJavaIntegerToSqlInteger() {
    assertThat(SchemaConverters.javaToSqlConverter().toSqlType(Integer.class),
        is(SqlBaseType.INTEGER));
  }

  @Test
  public void shouldConvertJavaLongToSqlBigInt() {
    assertThat(SchemaConverters.javaToSqlConverter().toSqlType(Long.class), is(SqlBaseType.BIGINT));
  }

  @Test
  public void shouldConvertJavaDoubleToSqlDouble() {
    assertThat(SchemaConverters.javaToSqlConverter().toSqlType(Double.class),
        is(SqlBaseType.DOUBLE));
  }

  @Test
  public void shouldConvertJavaStringToSqlString() {
    assertThat(SchemaConverters.javaToSqlConverter().toSqlType(String.class),
        is(SqlBaseType.STRING));
  }

  @Test
  public void shouldThrowOnUnknownJavaType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unexpected java type: " + double.class);

    // When:
    SchemaConverters.javaToSqlConverter().toSqlType(double.class);
  }
}
