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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicInfoExtended;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KafkaTopicsListExtended;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClient.ConsumerSummary;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

public final class ListTopicsExecutor {

  private ListTopicsExecutor() {
  }

  @SuppressWarnings("unused")
  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListTopics> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    return statement.getStatement().getShowExtended()
        ? listTopicsExtended(statement, serviceContext)
        : listTopics(statement, serviceContext);
  }

  private static Optional<KsqlEntity> listTopics(
      final ConfiguredStatement<ListTopics> statement,
      final ServiceContext serviceContext
  ) {
    final Map<String, TopicDescription> topicDescriptions =
        getTopicDescriptions(serviceContext, statement);

    final List<KafkaTopicInfo> topicInfoList = topicDescriptions.values()
        .stream().map(ListTopicsExecutor::topicDescriptionToTopicInfo)
        .collect(Collectors.toList());

    return Optional.of(new KafkaTopicsList(statement.getStatementText(), topicInfoList));
  }

  private static Optional<KsqlEntity> listTopicsExtended(
      final ConfiguredStatement<ListTopics> statement,
      final ServiceContext serviceContext
  ) {
    final Map<String, List<Integer>> topicConsumersAndGroupCount =
        getTopicConsumerAndGroupCounts(serviceContext);

    final Map<String, TopicDescription> topicDescriptions =
        getTopicDescriptions(serviceContext, statement);

    final Map<String, Long> msgCounts =
        getTopicMessageCounts(serviceContext, topicDescriptions);

    final List<KafkaTopicInfoExtended> topicInfoExtendedList = topicDescriptions.values()
        .stream().map(desc ->
            topicDescriptionToTopicInfoExtended(desc, topicConsumersAndGroupCount, msgCounts))
        .collect(Collectors.toList());

    return Optional.of(
        new KafkaTopicsListExtended(statement.getStatementText(), topicInfoExtendedList));
  }

  private static Map<String, Long> getTopicMessageCounts(
      final ServiceContext serviceContext,
      final Map<String, TopicDescription> topicDescriptions
  ) {
    final Set<TopicPartition> topicPartitions = topicDescriptions.values().stream()
        .flatMap(td ->
            td.partitions().stream().map(pd -> new TopicPartition(td.name(), pd.partition())))
        .collect(Collectors.toSet());

    final Map<TopicPartition, Long> partitionMsgCounts = serviceContext.getTopicClient()
        .maxMsgCounts(topicPartitions);

    return partitionMsgCounts.entrySet().stream()
        .collect(Collectors.groupingBy(
            e -> e.getKey().topic(),
            Collectors.summingLong(Entry::getValue)
        ));
  }

  private static Map<String, TopicDescription> getTopicDescriptions(
      final ServiceContext serviceContext,
      final ConfiguredStatement<ListTopics> statement
  ) {
    final KafkaTopicClient topicClient = serviceContext.getTopicClient();

    final ReservedInternalTopics internalTopics = new ReservedInternalTopics(statement.getConfig());

    final Set<String> topics = statement.getStatement().getShowAll()
        ? topicClient.listTopicNames()
        : internalTopics.removeHiddenTopics(topicClient.listTopicNames());

    // TreeMap is used to keep elements sorted
    return new TreeMap<>(topicClient.describeTopics(topics));
  }

  private static KafkaTopicInfo topicDescriptionToTopicInfo(final TopicDescription description) {
    return new KafkaTopicInfo(
        description.name(),
        description.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()));
  }

  private static KafkaTopicInfoExtended topicDescriptionToTopicInfoExtended(
      final TopicDescription topicDescription,
      final Map<String, List<Integer>> topicConsumersAndGroupCount,
      final Map<String, Long> msgCounts
  ) {
    final List<Integer> consumerAndGroupCount = topicConsumersAndGroupCount
        .getOrDefault(topicDescription.name(), Arrays.asList(0, 0));

    final long msgCount = msgCounts.getOrDefault(topicDescription.name(), 0L);

    return new KafkaTopicInfoExtended(
        topicDescription.name(),
        topicDescription.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()),
        msgCount, consumerAndGroupCount.get(0),
        consumerAndGroupCount.get(1)
    );
  }

  /**
   * @return all topics with their associated consumerCount and consumerGroupCount
   */
  private static Map<String, List<Integer>> getTopicConsumerAndGroupCounts(
      final ServiceContext serviceContext
  ) {
    final KafkaConsumerGroupClient consumerGroupClient =
        new KafkaConsumerGroupClientImpl(serviceContext.getAdminClient());

    final List<String> consumerGroups = consumerGroupClient.listGroups();

    final Map<String, Integer> topicConsumerCount = new HashMap<>();
    final Map<String, Set<String>> topicConsumerGroupCount = new HashMap<>();

    for (final String group : consumerGroups) {
      final Collection<ConsumerSummary> consumerSummaryList =
          consumerGroupClient.describeConsumerGroup(group).consumers();

      for (final KafkaConsumerGroupClientImpl.ConsumerSummary summary : consumerSummaryList) {

        for (final TopicPartition topicPartition : summary.partitions()) {
          topicConsumerCount
              .compute(topicPartition.topic(), (topic, count) -> count == null ? 1 : count + 1);
          topicConsumerGroupCount
              .computeIfAbsent(topicPartition.topic(), k -> new HashSet<>())
              .add(group);
        }
      }
    }

    final HashMap<String, List<Integer>> results = new HashMap<>();
    topicConsumerCount.forEach(
        (k, v) -> {
          results.computeIfAbsent(k, v1 -> new ArrayList<>()).add(v);
          results.get(k).add(topicConsumerGroupCount.get(k).size());
        }
    );

    return results;
  }
}
