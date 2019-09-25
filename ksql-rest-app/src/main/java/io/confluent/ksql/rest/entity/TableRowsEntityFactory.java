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
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.materialization.TableRow;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

/**
 * Factory class for {@link TableRowsEntity}
 */
public final class TableRowsEntityFactory {

  private static final List<Column> TIME_WINDOW_COLUMNS = ImmutableList
      .of(Column.of(ColumnName.of("WINDOWSTART"), SqlTypes.BIGINT));

  private static final List<Column> SESSION_WINDOW_COLUMNS = ImmutableList.<Column>builder()
      .addAll(TIME_WINDOW_COLUMNS)
      .add(Column.of(ColumnName.of("WINDOWEND"), SqlTypes.BIGINT))
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
    return windowType
        .map(wt -> addWindowFieldsIntoSchema(wt, schema))
        .orElse(schema);
  }

  private static LogicalSchema addWindowFieldsIntoSchema(
      final WindowType windowType,
      final LogicalSchema schema
  ) {
    final List<Column> additionalKeyCols = windowType == WindowType.SESSION
        ? SESSION_WINDOW_COLUMNS
        : TIME_WINDOW_COLUMNS;

    return LogicalSchema.builder()
        .keyColumns(schema.key())
        .keyColumns(additionalKeyCols)
        .valueColumns(schema.value())
        .build();
  }

  private static List<?> createRow(final TableRow row) {
    final Builder<Object> builder = ImmutableList.builder();

    keyFields(row.key()).forEach(builder::add);

    row.window().ifPresent(window -> {
      builder.add(window.start().toEpochMilli());
      window.end().map(Instant::toEpochMilli).ifPresent(builder::add);
    });

    builder.addAll(row.value().getColumns());

    return builder.build();
  }

  private static Stream<?> keyFields(final Struct key) {
    return key.schema().fields().stream().map(key::get);
  }
}
