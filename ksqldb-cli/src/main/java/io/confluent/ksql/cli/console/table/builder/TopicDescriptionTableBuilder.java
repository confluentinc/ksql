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
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import java.util.ArrayList;
import java.util.List;

public class TopicDescriptionTableBuilder implements TableBuilder<TopicDescription> {

  private static final List<String> NON_SCHEMA_HEADERS =
      ImmutableList.of("Table Name", "Kafka Topic", "Type");

  private static final List<String> SCHEMA_HEADERS =
      ImmutableList.of("Table Name", "Kafka Topic", "Type", "Schema");

  @Override
  public Table buildTable(final TopicDescription topicDescription) {
    final String format = topicDescription.getFormat();
    final boolean supportsSchema = FormatFactory.fromName(format)
        .supportsFeature(SerdeFeature.SCHEMA_INFERENCE);

    final List<String> headings = supportsSchema ? SCHEMA_HEADERS : NON_SCHEMA_HEADERS;

    final List<String> row = new ArrayList<>(4);
    row.add(topicDescription.getName());
    row.add(topicDescription.getKafkaTopic());
    row.add(format);
    if (supportsSchema) {
      row.add(topicDescription.getSchemaString());
    }

    return new Builder()
        .withColumnHeaders(headings)
        .withRow(row)
        .build();
  }
}
