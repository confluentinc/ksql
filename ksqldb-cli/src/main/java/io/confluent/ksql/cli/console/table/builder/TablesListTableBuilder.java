/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.TablesList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class TablesListTableBuilder implements TableBuilder<TablesList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Table Name", "Kafka Topic", "Key Format", "Value Format", "Windowed");

  @Override
  public Table buildTable(final TablesList entity) {
    final Stream<List<String>> rows = entity.getTables()
        .stream()
        .sorted(Comparator.comparing(SourceInfo::getName))
        .map(t -> ImmutableList.of(
            t.getName(),
            t.getTopic(),
            t.getKeyFormat(),
            t.getValueFormat(),
            Boolean.toString(t.isWindowed())
        ));

    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(rows)
        .build();
  }
}
