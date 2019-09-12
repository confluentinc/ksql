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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.rest.entity.QueryResultEntity;
import io.confluent.ksql.rest.entity.QueryResultEntity.Row;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryResultTableBuilder implements TableBuilder<QueryResultEntity> {

  private static final List<String> TIME_WINDOW_HEADINGS = ImmutableList
      .of("WINDOWSTART BIGINT");

  private static final List<String> SESSION_WINDOW_HEADINGS = ImmutableList.<String>builder()
      .addAll(TIME_WINDOW_HEADINGS)
      .add("WINDOWEND BIGINT")
      .build();

  @Override
  public Table buildTable(final QueryResultEntity entity) {

    final List<String> headers = buildHeadings(entity);

    final Stream<List<String>> rows = entity
        .getRows()
        .stream()
        .map(r -> buildRow(r, entity.getSchema().value()));

    return new Builder()
        .withColumnHeaders(headers)
        .withRows(rows)
        .build();
  }

  private static List<String> buildHeadings(final QueryResultEntity entity) {
    final LogicalSchema schema = entity.getSchema();

    final Stream<String> keys = schema.key().stream()
        .map(f -> f.fullName() + " " + f.type() + " KEY");

    final Stream<String> window = entity.getWindowType()
        .map(wt -> wt == WindowType.SESSION
            ? SESSION_WINDOW_HEADINGS
            : TIME_WINDOW_HEADINGS)
        .orElse(ImmutableList.of())
        .stream();

    final Stream<String> values = schema.value().stream()
        .map(f -> f.fullName() + " " + f.type());

    return Stream.concat(keys, Stream.concat(window, values))
        .collect(Collectors.toList());
  }

  private static List<String> buildRow(
      final Row row,
      final List<Column> valueSchema
  ) {
    final Stream<?> keys = row.getKey().values().stream();

    final Stream<?> window = row.getWindow()
        .map(w -> w.getEnd().isPresent()
            ? Stream.of(w.getStart(), w.getEnd().getAsLong())
            : Stream.of(w.getStart()))
        .orElse(Stream.of());

    final Stream<?> values = row.getValue() == null
        ? valueSchema.stream().map(f -> null)
        : row.getValue().values().stream();

    return Stream.concat(keys, Stream.concat(window, values))
        .map(Objects::toString)
        .collect(Collectors.toList());
  }
}
