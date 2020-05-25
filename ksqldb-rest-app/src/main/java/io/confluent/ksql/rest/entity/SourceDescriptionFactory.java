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

import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public final class SourceDescriptionFactory {

  private SourceDescriptionFactory() {
  }

  public static SourceDescription create(
      final DataSource dataSource,
      final boolean extended,
      final List<RunningQuery> readQueries,
      final List<RunningQuery> writeQueries,
      final Optional<TopicDescription> topicDescription,
      final Optional<ConsumerGroupDescription> consumerGroupDescription,
      final Map<TopicPartition, ListOffsetsResultInfo> topicAndEndOffsets,
      final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets
  ) {
    return new SourceDescription(
        dataSource.getName().toString(FormatOptions.noEscape()),
        dataSource.getKsqlTopic().getKeyFormat().getWindowType(),
        readQueries,
        writeQueries,
        EntityUtil.buildSourceSchemaEntity(dataSource.getSchema()),
        dataSource.getDataSourceType().getKsqlType(),
        dataSource.getTimestampColumn()
            .map(TimestampColumn::getColumn)
            .map(c -> c.toString(FormatOptions.noEscape())).orElse(""),
        (extended
            ? MetricCollectors.getAndFormatStatsFor(
            dataSource.getKafkaTopicName(), false) : ""),
        (extended
            ? MetricCollectors.getAndFormatStatsFor(
            dataSource.getKafkaTopicName(), true) : ""),
        extended,
        dataSource.getKsqlTopic().getKeyFormat().getFormat().name(),
        dataSource.getKsqlTopic().getValueFormat().getFormat().name(),
        dataSource.getKafkaTopicName(),
        topicDescription.map(td -> td.partitions().size()).orElse(0),
        topicDescription.map(td -> td.partitions().get(0).replicas().size()).orElse(0),
        dataSource.getSqlExpression(),
        consumerGroupDescription.map(
            cg -> new SourceConsumerGroupOffsets(cg.groupId(), dataSource.getKafkaTopicName(),
                consumerOffsets(topicAndEndOffsets, topicAndConsumerOffsets)))
            .orElse(new SourceConsumerGroupOffsets("", dataSource.getKafkaTopicName(), Collections
                .emptyList())));
  }

  private static List<SourceConsumerOffset> consumerOffsets(
      final Map<TopicPartition, ListOffsetsResultInfo> topicAndEndOffsets,
      final Map<TopicPartition, OffsetAndMetadata> topicAndConsumerOffsets
  ) {
    List<SourceConsumerOffset> consumerOffsets = new ArrayList<>();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicAndConsumerOffsets.entrySet()) {
      consumerOffsets.add(new SourceConsumerOffset(entry.getKey().partition(),
          topicAndEndOffsets.get(entry.getKey()).offset(), entry.getValue().offset()));
    }
    return consumerOffsets;
  }
}
