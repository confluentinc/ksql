/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.cli.console.table.builder;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import java.util.List;
import java.util.stream.Stream;

public class KsqlTopicsListTableBuilder implements TableBuilder<KsqlTopicsList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Ksql Topic", "Kafka Topic", "Format");

  @Override
  public Table buildTable(final KsqlTopicsList entity) {
    final Stream<List<String>> rows = entity.getTopics().stream()
        .map(t -> ImmutableList.of(t.getName(), t.getKafkaTopic(), t.getFormat().name()));

    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(rows)
        .build();
  }
}
