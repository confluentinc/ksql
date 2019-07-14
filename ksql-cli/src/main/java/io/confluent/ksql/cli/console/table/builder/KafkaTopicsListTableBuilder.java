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
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.util.StringUtil;
import java.util.List;
import java.util.stream.Stream;

public class KafkaTopicsListTableBuilder implements TableBuilder<KafkaTopicsList> {

  private static final List<String> HEADERS = ImmutableList.of(
      "Kafka Topic",
      "Partitions",
      "Partition Replicas",
      "Consumers",
      "ConsumerGroups");

  @Override
  public Table buildTable(final KafkaTopicsList entity) {
    final Stream<List<String>> rows = entity.getTopics().stream()
        .map(t -> ImmutableList.of(
            t.getName(),
            Integer.toString(t.getReplicaInfo().size()),
            getTopicReplicaInfo(t.getReplicaInfo()),
            Integer.toString(t.getConsumerCount()),
            Integer.toString(t.getConsumerGroupCount())));

    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(rows)
        .build();
  }

  /**
   * Pretty print replica info.
   *
   * @param replicaSizes list of replicas per partition
   * @return single value if all values are equal, else a csv representation
   */
  private static String getTopicReplicaInfo(final List<Integer> replicaSizes) {
    if (replicaSizes.isEmpty()) {
      return "0";
    } else if (replicaSizes.stream().distinct().limit(2).count() <= 1) {
      return String.valueOf(replicaSizes.get(0));
    } else {
      return StringUtil.join(", ", replicaSizes);
    }
  }
}
