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

import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableRowsTableBuilder implements TableBuilder<TableRowsEntity> {

  @Override
  public Table buildTable(final TableRowsEntity entity) {

    final List<String> headers = buildHeadings(entity);

    final Stream<List<String>> rows = entity
        .getRows()
        .stream()
        .map(TableRowsTableBuilder::buildRow);

    return new Builder()
        .withColumnHeaders(headers)
        .withRows(rows)
        .build();
  }

  private static List<String> buildHeadings(final TableRowsEntity entity) {
    final LogicalSchema schema = entity.getSchema();

    final Stream<String> keys = schema.key().stream()
        .map(f -> f.ref().aliasedFieldName() + " " + f.type() + " KEY");

    final Stream<String> values = schema.value().stream()
        .map(f -> f.ref().aliasedFieldName() + " " + f.type());

    return Stream.concat(keys, values)
        .collect(Collectors.toList());
  }

  private static List<String> buildRow(
      final List<?> row
  ) {
    return row.stream()
        .map(Objects::toString)
        .collect(Collectors.toList());
  }
}
