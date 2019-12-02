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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

/**
 * Factory class for {@link TableRowsEntity}
 */
public final class TableRowsEntityFactory {

  @SuppressWarnings("deprecation")
  private static final List<Column> TIME_WINDOW_COLUMNS = ImmutableList
      .of(Column.legacySystemWindowColumn(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT));

  @SuppressWarnings("deprecation")
  private static final List<Column> SESSION_WINDOW_COLUMNS = ImmutableList.<Column>builder()
      .addAll(TIME_WINDOW_COLUMNS)
      .add(Column.legacySystemWindowColumn(ColumnName.of("WINDOWEND"), SqlTypes.BIGINT))
      .build();

  private TableRowsEntityFactory() {
  }

  public static List<List<?>> createRows(
      final List<? extends TableRow> result
  ) {
    return result.stream()
        .map(TableRowsEntityFactory::createRow)
        .collect(Collectors.toList());
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema schema,
      final Optional<WindowType> windowType
  ) {
    final LogicalSchema adjusted = LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumns(schema.key())
        .valueColumns(schema.metadata())
        .valueColumns(schema.value())
        .build();

    return windowType
        .map(wt -> addWindowFieldsIntoSchema(wt, adjusted))
        .orElse(adjusted);
  }

  private static LogicalSchema addWindowFieldsIntoSchema(
      final WindowType windowType,
      final LogicalSchema schema
  ) {
    final List<Column> additionalKeyCols = windowType == WindowType.SESSION
        ? SESSION_WINDOW_COLUMNS
        : TIME_WINDOW_COLUMNS;

    return LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumns(schema.key())
        .keyColumns(additionalKeyCols)
        .valueColumns(schema.value())
        .build();
  }

  private static List<?> createRow(final TableRow row) {
    final List<Object> rowList = new ArrayList<>();

    keyFields(row.key()).forEach(rowList::add);

    row.window().ifPresent(window -> {
      rowList.add(window.start().toEpochMilli());
      window.end().map(Instant::toEpochMilli).ifPresent(rowList::add);
    });

    rowList.add(row.rowTime());

    rowList.addAll(row.value().getColumns());

    return rowList;
  }

  private static Stream<?> keyFields(final Struct key) {
    return key.schema().fields().stream().map(key::get);
  }
}
