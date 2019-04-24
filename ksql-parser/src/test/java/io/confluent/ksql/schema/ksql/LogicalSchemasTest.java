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
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LogicalSchemasTest {

  private static final Schema LOGICAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
  private static final Schema LOGICAL_INT_SCHEMA = SchemaBuilder.int32().optional().build();
  private static final Schema LOGICAL_BIGINT_SCHEMA = SchemaBuilder.int64().optional().build();
  private static final Schema LOGICAL_DOUBLE_SCHEMA = SchemaBuilder.float64().optional().build();
  private static final Schema LOGICAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();

  private static final BiMap<Type, Schema> SQL_TO_LOGICAL = ImmutableBiMap.<Type, Schema>builder()
      .put(PrimitiveType.of(SqlType.BOOLEAN), LOGICAL_BOOLEAN_SCHEMA)
      .put(PrimitiveType.of(SqlType.INTEGER), LOGICAL_INT_SCHEMA)
      .put(PrimitiveType.of(SqlType.BIGINT), LOGICAL_BIGINT_SCHEMA)
      .put(PrimitiveType.of(SqlType.DOUBLE), LOGICAL_DOUBLE_SCHEMA)
      .put(PrimitiveType.of(SqlType.STRING), LOGICAL_STRING_SCHEMA)
      .put(io.confluent.ksql.parser.tree.Array.of(PrimitiveType.of(SqlType.INTEGER)),
          SchemaBuilder.array(LogicalSchemas.INTEGER).optional().build())
      .put(io.confluent.ksql.parser.tree.Map.of(PrimitiveType.of(SqlType.INTEGER)),
          SchemaBuilder.map(LogicalSchemas.STRING, LogicalSchemas.INTEGER).optional().build())
      .put(io.confluent.ksql.parser.tree.Struct.builder()
              .addField("f0", PrimitiveType.of(SqlType.INTEGER))
              .build(),
          SchemaBuilder.struct().field("f0", LogicalSchemas.INTEGER).optional().build())
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

  private static final Type STRUCT_SQL_TYPE = io.confluent.ksql.parser.tree.Struct.builder()
      .addField("F0", PrimitiveType.of(SqlType.INTEGER))
      .build();

  private static final Type NESTED_SQL_TYPE = io.confluent.ksql.parser.tree.Struct.builder()
      .addField("ARRAY", io.confluent.ksql.parser.tree.Array.of(STRUCT_SQL_TYPE))
      .addField("MAP", io.confluent.ksql.parser.tree.Map.of(STRUCT_SQL_TYPE))
      .addField("STRUCT", STRUCT_SQL_TYPE)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldHaveTestsForAllTypes() {
    final Set<SqlType> tested = SQL_TO_LOGICAL.keySet().stream()
        .map(Type::getSqlType)
        .collect(Collectors.toSet());

    final ImmutableSet<SqlType> allTypes = ImmutableSet.copyOf(SqlType.values());

    assertThat("If this test fails then there has been a new SQL type added and this test "
        + "file needs updating to cover that new type", tested, is(allTypes));
  }

  @Test
  public void shouldGetLogicalForEverySqlType() {
    for (Entry<Type, Schema> entry : SQL_TO_LOGICAL.entrySet()) {
      final Type sqlType = entry.getKey();
      final Schema logical = entry.getValue();
      final Schema result = LogicalSchemas.fromSqlTypeConverter().fromSqlType(sqlType);
      assertThat(result, is(logical));
    }
  }

  @Test
  public void shouldGetSqlTypeForEveryLogicalType() {
    SQL_TO_LOGICAL.inverse().forEach((logical, sqlType) ->
        assertThat(LogicalSchemas.toSqlTypeConverter().toSqlType(logical), is(sqlType)));
  }

  @Test
  public void shouldConvertNestedComplexToSql() {
    assertThat(LogicalSchemas.toSqlTypeConverter().toSqlType(NESTED_LOGICAL_TYPE), is(NESTED_SQL_TYPE));
  }

  @Test
  public void shouldConvertNestedComplexFromSql() {
    assertThat(LogicalSchemas.fromSqlTypeConverter().fromSqlType(NESTED_SQL_TYPE), is(NESTED_LOGICAL_TYPE));
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
    LogicalSchemas.toSqlTypeConverter().toSqlType(mapSchema);
  }

  @Test
  public void shouldThrowOnUnsupportedConnectSchemaType() {
    // Given:
    final Schema unsupported = SchemaBuilder.int8().build();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unexpected logical type: Schema{INT8}");

    // When:
    LogicalSchemas.toSqlTypeConverter().toSqlType(unsupported);
  }
}
