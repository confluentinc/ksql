/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.List;

public class TopicDescriptionTableBuilder implements TableBuilder<TopicDescription> {

  private static final List<String> NON_AVRO_HEADERS =
      ImmutableList.of("Table Name", "Kafka Topic", "Type");

  private static final List<String> AVRO_HEADERS =
      ImmutableList.of("Table Name", "Kafka Topic", "Type", "AvroSchema");

  @Override
  public Table buildTable(final TopicDescription topicDescription) {
    final boolean avro = topicDescription.getFormat().equalsIgnoreCase("AVRO");

    final List<String> headings = avro ? AVRO_HEADERS : NON_AVRO_HEADERS;

    final List<String> row = new ArrayList<>(4);
    row.add(topicDescription.getName());
    row.add(topicDescription.getKafkaTopic());
    row.add(topicDescription.getFormat());
    if (avro) {
      row.add(topicDescription.getSchemaString());
    }

    return new Builder()
        .withColumnHeaders(headings)
        .withRow(row)
        .build();
  }
}
