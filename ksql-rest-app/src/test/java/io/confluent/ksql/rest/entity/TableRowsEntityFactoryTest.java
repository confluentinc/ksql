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
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.Window;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class TableRowsEntityFactoryTest {

  private static final LogicalSchema SIMPLE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("v0"), SqlTypes.BOOLEAN)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
      .build();

  @Test
  public void shouldAddNonWindowedRowToValues() {
    // Given:
    final List<? extends TableRow> input = ImmutableList.of(
        Row.of(
            SIMPLE_SCHEMA,
            StructKeyUtil.asStructKey("x"),
            new GenericRow(false)
        )
    );

    // When:
    final List<List<?>> output = TableRowsEntityFactory.createRows(input);

    // Then:
    assertThat(output, hasSize(1));
    assertThat(output.get(0), contains("x", false));
  }

  @Test
  public void shouldAddWindowedRowToValues() {
    // Given:
    final Instant now = Instant.now();
    final Window window0 = Window.of(now, Optional.empty());
    final Window window1 = Window.of(now, Optional.of(now));

    final List<? extends TableRow> input = ImmutableList.of(
        WindowedRow.of(
            SIMPLE_SCHEMA,
            StructKeyUtil.asStructKey("x"),
            window0,
            new GenericRow(true)
        ),
        WindowedRow.of(
            SIMPLE_SCHEMA,
            StructKeyUtil.asStructKey("y"),
            window1,
            new GenericRow(false)
        )
    );

    // When:
    final List<List<?>> output = TableRowsEntityFactory.createRows(input);

    // Then:
    assertThat(output, hasSize(2));
    assertThat(output.get(0), contains("x", now.toEpochMilli(), true));
    assertThat(output.get(1), contains("y", now.toEpochMilli(), now.toEpochMilli(), false));
  }

  @Test
  public void shouldReturnSameSchemaIfNotWindowed() {
    // When:
    final LogicalSchema result = TableRowsEntityFactory.buildSchema(SCHEMA, Optional.empty());

    // Then:
    assertThat(result, is(sameInstance(SCHEMA)));
  }

  @Test
  public void shouldAddHoppingWindowFieldsToSchema() {
    // When:
    final LogicalSchema result = TableRowsEntityFactory
        .buildSchema(SCHEMA, Optional.of(WindowType.HOPPING));

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
        .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
        .build()
    ));
  }

  @Test
  public void shouldAddTumblingWindowFieldsToSchema() {
    // When:
    final LogicalSchema result = TableRowsEntityFactory
        .buildSchema(SCHEMA, Optional.of(WindowType.TUMBLING));

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
        .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
        .build()
    ));
  }

  @Test
  public void shouldAddSessionWindowFieldsToSchema() {
    // When:
    final LogicalSchema result = TableRowsEntityFactory
        .buildSchema(SCHEMA, Optional.of(WindowType.SESSION));

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
        .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
        .keyColumn(ColumnName.of("WINDOWEND"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
        .build()
    ));
  }
}
