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

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;

public class TableRowsFactoryTest {

  private static final ColumnName K0 = ColumnName.of("k0");

  private static final KeyBuilder KEY_BUILDER = StructKeyUtil
      .keyBuilder(K0, SqlTypes.STRING);

  private static final LogicalSchema SIMPLE_SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.BOOLEAN)
      .build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
      .build();

  private static final LogicalSchema SCHEMA_NULL = LogicalSchema.builder()
      .keyColumn(K0, SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("v2"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("v3"), SqlTypes.BOOLEAN)
      .build();

  private static final long ROWTIME = 285775L;

  @Test
  public void shouldAddNonWindowedRowToValues() {
    // Given:
    final List<? extends TableRow> input = ImmutableList.of(
        Row.of(
            SIMPLE_SCHEMA,
            KEY_BUILDER.build("x"),
            genericRow(false),
            ROWTIME
        )
    );

    // When:
    final List<List<?>> output = TableRowsFactory.createRows(input);

    // Then:
    assertThat(output, hasSize(1));
    assertThat(output.get(0), contains("x", false));
  }

  @Test
  public void shouldAddWindowedRowToValues() {
    // Given:
    final Instant now = Instant.now();
    final TimeWindow window0 = new TimeWindow(now.toEpochMilli(), now.plusMillis(2).toEpochMilli());
    final TimeWindow window1 = new TimeWindow(now.toEpochMilli(), now.plusMillis(1).toEpochMilli());

    final List<? extends TableRow> input = ImmutableList.of(
        WindowedRow.of(
            SIMPLE_SCHEMA,
            new Windowed<>(KEY_BUILDER.build("x"), window0),
            genericRow(true),
            ROWTIME
        ),
        WindowedRow.of(
            SIMPLE_SCHEMA,
            new Windowed<>(KEY_BUILDER.build("y"), window1),
            genericRow(false),
            ROWTIME
        )
    );

    // When:
    final List<List<?>> output = TableRowsFactory.createRows(input);

    // Then:
    assertThat(output, hasSize(2));
    assertThat(output.get(0),
        contains("x", now.toEpochMilli(), now.plusMillis(2).toEpochMilli(), true));
    assertThat(output.get(1),
        contains("y", now.toEpochMilli(), now.plusMillis(1).toEpochMilli(), false));
  }

  @Test
  public void shouldSupportNullColumns() {
    // Given:
    final GenericRow row = genericRow(null, null, null, null);

    final Builder<Row> builder = ImmutableList.builder();
    builder.add(Row.of(SCHEMA_NULL, KEY_BUILDER.build("k"), row, ROWTIME));

    // When:
    final List<List<?>> output = TableRowsFactory.createRows(builder.build());

    // Then:
    assertThat(output, hasSize(1));
    assertThat(output.get(0), contains("k", null, null, null, null));
  }

  @Test
  public void shouldJustDuplicateRowTimeInValueIfNotWindowed() {
    // When:
    final LogicalSchema result = TableRowsFactory.buildSchema(SCHEMA, false);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
        .build()
    ));
  }

  @Test
  public void shouldAddHoppingWindowFieldsToSchema() {
    // When:
    final LogicalSchema result = TableRowsFactory.buildSchema(SCHEMA, true);

    // Then:
    assertThat(result, is(LogicalSchema.builder()
        .keyColumn(K0, SqlTypes.STRING)
        .keyColumn(ColumnName.of("k1"), SqlTypes.BOOLEAN)
        .keyColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT)
        .keyColumn(ColumnName.of("WINDOWEND"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("v1"), SqlTypes.BOOLEAN)
        .build()
    ));
  }
}
