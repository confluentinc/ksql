/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.rest.entity.StreamsList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class StreamsListTableBuilder implements TableBuilder<StreamsList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Stream Name", "Kafka Topic", "Key Format", "Value Format", "Windowed");

  @Override
  public Table buildTable(final StreamsList entity) {
    final Stream<List<String>> rows = entity
        .getStreams()
        .stream()
        .sorted(Comparator.comparing(SourceInfo::getName))
        .map(s -> ImmutableList.of(
            s.getName(),
            s.getTopic(),
            s.getKeyFormat(),
            s.getValueFormat(),
            Boolean.toString(s.isWindowed())
        ));

    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(rows)
        .build();
  }
}
