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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.rest.entity.QueryResultEntity.Row;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class QueryResultEntityFactoryTest {

  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("i0", Schema.OPTIONAL_INT32_SCHEMA)
      .optional()
      .build();

  private static final Schema NESTED_SCHEMA = SchemaBuilder.struct()
      .field("o0", STRUCT_SCHEMA)
      .optional()
      .build();

  private static final Struct STRUCT = new Struct(STRUCT_SCHEMA)
      .put("i0", 10);

  private static final Struct NESTED_STRUCT = new Struct(NESTED_SCHEMA)
      .put("o0", STRUCT);

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .valueColumn("o0", SchemaConverters.connectToSqlConverter().toSqlType(STRUCT_SCHEMA))
      .build();

  private static final GenericRow A_VALUE = new GenericRow(STRUCT);

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnValueFieldMismatch() {
    // Given:
    final GenericRow value = new GenericRow("text", 10); // <-- 2 fields vs 1 in schema

    // When:
    QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.empty(), value), LOGICAL_SCHEMA);
  }

  @Test
  public void shouldHandleNestedStructsInKey() {
    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(NESTED_STRUCT, ImmutableMap.of(Optional.empty(), A_VALUE), LOGICAL_SCHEMA);

    // Then:
    final Map<String, ?> key = rows.get(0).getKey();
    assertThat(key.get("o0"), is(instanceOf(Map.class)));
    assertThat(((Map<?, ?>) key.get("o0")), hasEntry("i0", 10));
  }

  @Test
  public void shouldHandleValue() {
    // Given:
    final LogicalSchema logicalSchema = LogicalSchema.builder()
        .valueColumn("c0", SqlTypes.STRING)
        .valueColumn("c1", SqlTypes.INTEGER)
        .build();

    final GenericRow row = new GenericRow("text", 10);

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.empty(), row), logicalSchema);

    // Then:
    final Map<String, ?> value = rows.get(0).getValue();
    assertThat(value.get("c0"), is("text"));
    assertThat(value.get("c1"), is(10));
  }

  @Test
  public void shouldHandleNestedStructsInValue() {
    // Given:
    final GenericRow row = new GenericRow(STRUCT);

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.empty(), row), LOGICAL_SCHEMA);

    // Then:
    final Map<String, ?> value = rows.get(0).getValue();
    assertThat(value.get("o0"), is(instanceOf(Map.class)));
    assertThat(((Map<?, ?>) value.get("o0")), hasEntry("i0", 10));
  }

  @Test
  public void shouldHandleNoValue() {
    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(), LOGICAL_SCHEMA);

    // Then:
    assertThat(rows, is(empty()));
  }

  @Test
  public void shouldHandleNullStructInKey() {
    // Given:
    final Struct nestedStruct = new Struct(NESTED_SCHEMA)
        .put("o0", null);

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(nestedStruct, ImmutableMap.of(Optional.empty(), A_VALUE), LOGICAL_SCHEMA);

    // Then:
    assertThat(rows.get(0).getKey().get("o0"), is(nullValue()));
  }

  @Test
  public void shouldHandleNullStructInValue() {
    // Given:
    final GenericRow row = new GenericRow((Object) null);

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.empty(), row), LOGICAL_SCHEMA);

    // Then:
    assertThat(rows.get(0).getValue().get("o0"), is(nullValue()));
  }

  @Test
  public void shouldHandleTimedWindow() {
    // Given:
    final Window window = Window.of(Instant.ofEpochMilli(10_123), Optional.empty());

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.of(window), A_VALUE), LOGICAL_SCHEMA);

    // Then:
    final Optional<QueryResultEntity.Window> actual = rows.get(0).getWindow();
    assertThat(actual, is(Optional.of(new QueryResultEntity.Window(10_123, OptionalLong.empty()))));
  }

  @Test
  public void shouldHandleSessionWindow() {
    // Given:
    final Window window = Window.of(
        Instant.ofEpochMilli(10_123),
        Optional.of(Instant.ofEpochMilli(32_101))
    );

    // When:
    final List<Row> rows = QueryResultEntityFactory
        .createRows(STRUCT, ImmutableMap.of(Optional.of(window), A_VALUE), LOGICAL_SCHEMA);

    // Then:
    final Optional<QueryResultEntity.Window> actual = rows.get(0).getWindow();
    assertThat(actual, is(Optional.of(new QueryResultEntity.Window(
        10_123,
        OptionalLong.of(32_101)
    ))));
  }
}
