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
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.rest.entity.QueryResultEntity;
import io.confluent.ksql.rest.entity.QueryResultEntity.Row;
import io.confluent.ksql.rest.entity.QueryResultEntity.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.Before;
import org.junit.Test;


public class QueryResultTableBuilderTest {

  private static final String SOME_SQL = "some sql";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn("k0", SqlTypes.BIGINT)
      .keyColumn("k1", SqlTypes.DOUBLE)
      .valueColumn("v0", SqlTypes.STRING)
      .valueColumn("v1", SqlTypes.INTEGER)
      .build();

  private static final Optional<Window> SESSION_WINDOW = Optional
      .of(new Window(12_234, OptionalLong.of(43_234)));

  private static final Optional<Window> TIME_WINDOW = Optional
      .of(new Window(12_234, OptionalLong.empty()));

  private static final LinkedHashMap<String, ?> A_KEY =
      orderedMap("k0", 10L, "k1", 5.1D);

  private static final LinkedHashMap<String, ?> A_VALUE =
      orderedMap("v0", "x", "v1", 5);

  private QueryResultTableBuilder builder;

  @Before
  public void setUp() {
    builder = new QueryResultTableBuilder();
  }

  @Test
  public void shouldBuildTableHeadings() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(Optional.empty(), A_KEY, A_VALUE))
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
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.TUMBLING),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(TIME_WINDOW, A_KEY, A_VALUE))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.headers(), contains(
        "k0 BIGINT KEY",
        "k1 DOUBLE KEY",
        "WINDOWSTART BIGINT",
        "v0 STRING",
        "v1 INTEGER"
    ));
  }

  @Test
  public void shouldBuildSessionWindowedTableHeadings() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.SESSION),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(SESSION_WINDOW, A_KEY, A_VALUE))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.headers(), contains(
        "k0 BIGINT KEY",
        "k1 DOUBLE KEY",
        "WINDOWSTART BIGINT",
        "WINDOW_END BIGINT",
        "v0 STRING",
        "v1 INTEGER"
    ));
  }

  @Test
  public void shouldBuildTableRows() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(Optional.empty(), A_KEY, A_VALUE))
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
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.HOPPING),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(TIME_WINDOW, A_KEY, A_VALUE))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "12234", "x", "5"));
  }

  @Test
  public void shouldBuildSessionWindowedTableRows() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.of(WindowType.SESSION),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(SESSION_WINDOW, A_KEY, A_VALUE))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "12234", "43234", "x", "5"));
  }

  @Test
  public void shouldHandleNullValue() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(Optional.empty(), A_KEY, null))
    );

    // When:
    final Table table = builder.buildTable(entity);

    // Then:
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("10", "5.1", "null", "null"));
  }

  @Test
  public void shouldHandleNullFields() {
    // Given:
    final QueryResultEntity entity = new QueryResultEntity(
        SOME_SQL,
        Optional.empty(),
        LOGICAL_SCHEMA,
        ImmutableList.of(new Row(
            Optional.empty(),
            orderedMap("k0", 10L, "k1", null),
            orderedMap("v0", "x", "v1", null)
        ))
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