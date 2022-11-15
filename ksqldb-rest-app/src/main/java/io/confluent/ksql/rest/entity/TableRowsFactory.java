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

import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

/**
 * Factory class for {@link TableRows}
 */
public final class TableRowsFactory {

  private TableRowsFactory() {
  }

  public static List<List<?>> createRows(
      final List<? extends TableRow> result
  ) {
    return result.stream()
        .map(TableRowsFactory::createRow)
        .collect(Collectors.toList());
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema schema,
      final boolean windowed
  ) {
    final Builder builder = LogicalSchema.builder()
        .keyColumns(schema.key());

    if (windowed) {
      builder.keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT);
      builder.keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT);
    }

    return builder
        .valueColumns(schema.value())
        .build();
  }

  private static List<?> createRow(final TableRow row) {
    final List<Object> rowList = new ArrayList<>();

    keyFields(row.key()).forEach(rowList::add);

    row.window().ifPresent(window -> {
      rowList.add(window.start().toEpochMilli());
      rowList.add(window.end().toEpochMilli());
    });

    rowList.addAll(row.value().values());

    return rowList;
  }

  private static Stream<?> keyFields(final Struct key) {
    return key.schema().fields().stream().map(key::get);
  }
}
