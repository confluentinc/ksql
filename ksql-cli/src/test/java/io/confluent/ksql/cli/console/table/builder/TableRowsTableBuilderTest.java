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

package io.confluent.ksql.cli.console.table.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.Before;
import org.junit.Test;


public class TableRowsTableBuilderTest {

  private static final String SOME_SQL = "some sql";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
      .keyColumn(ColumnName.of("k1"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.INTEGER)
      .build();

  private static final List<?> VALUES = ImmutableList.of(
      10L, 5.1D, "x", 5
  );

  private static final LogicalSchema TIME_WINDOW_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
      .keyColumn(ColumnName.of("k1"), SqlTypes.DOUBLE)
      .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.INTEGER)
      .build();

  private static final List<?> TIME_WINDOW_VALUES = ImmutableList.of(
      10L, 5.1D, 123456L, "x", 5
  );

  private static final LogicalSchema SESSION_WINDOW_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
      .keyColumn(ColumnName.of("k1"), SqlTypes.DOUBLE)
      .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
      .keyColumn(ColumnName.of("WINDOWEND"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.INTEGER)
      .build();

  private static final List<?> SESSION_WINDOW_VALUES = ImmutableList.of(
      10L, 5.1D, 123456L, 23456L, "x", 5
  );

  private TableRowsTableBuilder builder;

  @Before
  public void setUp() {
    builder = new TableRowsTableBuilder();
  }

  @Test
  public void shouldBuildTableHeadings() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        SCHEMA,
        ImmutableList.of(VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.headers(), contains(
        "k0 BIGINT KEY",
        "k1 DOUBLE KEY",
        "v0 STRING",
        "v1 INTEGER"
    ));
  }

  @Test
  public void shouldBuildTimeWindowedTableHeadings() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        TIME_WINDOW_SCHEMA,
        ImmutableList.of(TIME_WINDOW_VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.headers(), contains(
        "k0 BIGINT KEY",
        "k1 DOUBLE KEY",
        "WINDOWSTART BIGINT KEY",
        "v0 STRING",
        "v1 INTEGER"
    ));
  }

  @Test
  public void shouldBuildSessionWindowedTableHeadings() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        SESSION_WINDOW_SCHEMA,
        ImmutableList.of(SESSION_WINDOW_VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.headers(), contains(
        "k0 BIGINT KEY",
        "k1 DOUBLE KEY",
        "WINDOWSTART BIGINT KEY",
        "WINDOWEND BIGINT KEY",
        "v0 STRING",
        "v1 INTEGER"
    ));
  }

  @Test
  public void shouldBuildTableRows() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        SCHEMA,
        ImmutableList.of(VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "x", "5"));
  }

  @Test
  public void shouldBuildTimeWindowedTableRows() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        TIME_WINDOW_SCHEMA,
        ImmutableList.of(TIME_WINDOW_VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "123456", "x", "5"));
  }

  @Test
  public void shouldBuildSessionWindowedTableRows() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        SESSION_WINDOW_SCHEMA,
        ImmutableList.of(SESSION_WINDOW_VALUES)
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "123456", "23456", "x", "5"));
  }

  @Test
  public void shouldHandleNullFields() {
    // Given:
    final TableRowsEntity entity = new TableRowsEntity(
        SOME_SQL,
        SCHEMA,
        ImmutableList.of(Arrays.asList(10L, null, "x", null))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "null", "x", "null"));
  }

  private static LinkedHashMap<String, ?> orderedMap(final Object... keysAndValues) {
    assertThat("invalid test", keysAndValues.length % 2, is(0));

    final LinkedHashMap<String, Object> orderedMap = new LinkedHashMap<>();

    for (int idx = 0; idx < keysAndValues.length; idx = idx + 2) {
      final Object key = keysAndValues[idx];
      final Object value = keysAndValues[idx + 1];

      assertThat("invalid test", key, instanceOf(String.class));
      orderedMap.put((String) key, value);
    }

    return orderedMap;
  }
}