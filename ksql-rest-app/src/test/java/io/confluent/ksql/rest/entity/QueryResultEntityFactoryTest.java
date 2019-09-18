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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.Row;
import io.confluent.ksql.materialization.TableRow;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.materialization.WindowedRow;
import io.confluent.ksql.rest.entity.QueryResultEntity.ResultRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.StructKeyUtil;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.junit.Test;

public class QueryResultEntityFactoryTest {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .valueColumn("v0", SqlTypes.BOOLEAN)
      .build();

  private static final Window A_WINDOW = Window.of(Instant.now(), Optional.empty());

  private static final TableRow ROW = Row.of(
      LOGICAL_SCHEMA,
      StructKeyUtil.asStructKey("x"),
      new GenericRow(false)
  );

  private static final TableRow WINDOWED_ROW = WindowedRow.of(
      LOGICAL_SCHEMA,
      StructKeyUtil.asStructKey("y"),
      A_WINDOW,
      new GenericRow(true)
  );

  @Test
  public void shouldAddAllValuesToRow() {
    // Given:
    final List<? extends TableRow> input = ImmutableList.of(
        ROW,
        WINDOWED_ROW
    );

    // When:
    final List<ResultRow> output = QueryResultEntityFactory.createRows(input);

    // Then:
    final List<List<?>> values = output.stream()
        .map(ResultRow::getValues)
        .collect(Collectors.toList());

    assertThat(values, hasSize(2));
    assertThat(values.get(0), contains("x", false));
    assertThat(values.get(1), contains("y", true));
  }

  @Test
  public void shouldAddOptionalWindowToRow() {
    // Given:
    final List<? extends TableRow> input = ImmutableList.of(
        ROW,
        WINDOWED_ROW
    );

    // When:
    final List<ResultRow> output = QueryResultEntityFactory.createRows(input);

    // Then:
    final List<Optional<QueryResultEntity.Window>> windows = output.stream()
        .map(ResultRow::getWindow)
        .collect(Collectors.toList());

    assertThat(windows, hasSize(2));
    assertThat(windows.get(0), is(Optional.empty()));
    assertThat(windows.get(1), is(Optional.of(new QueryResultEntity.Window(
        A_WINDOW.start().toEpochMilli(),
        OptionalLong.empty()
    ))));
  }
}
