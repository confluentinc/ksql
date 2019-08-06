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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

public final class ListTopicsExecutor {

  private ListTopicsExecutor() {

  }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListTopics> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KafkaTopicClient client = serviceContext.getTopicClient();

    final Map<String, TopicDescription> kafkaTopicDescriptions
        = client.describeTopics(client.listNonInternalTopicNames());

    final Map<String, TopicDescription> filteredDescriptions = new TreeMap<>(
        filterKsqlInternalTopics(kafkaTopicDescriptions, statement.getConfig()));

    if (statement.getStatement().getShowExtended()) {
      final KafkaConsumerGroupClient consumerGroupClient
          = new KafkaConsumerGroupClientImpl(serviceContext.getAdminClient());
      final Map<String, List<Integer>> topicConsumersAndGroupCount
          = getTopicConsumerAndGroupCounts(consumerGroupClient);

      final List<KafkaTopicInfoExtended> topicInfoExtendedList = filteredDescriptions.values()
          .stream().map(desc ->
              topicDescriptionToTopicInfoExtended(desc, topicConsumersAndGroupCount))
          .collect(Collectors.toList());

      return Optional.of(
          new KafkaTopicsListExtended(statement.getStatementText(), topicInfoExtendedList));
    } else {
      final List<KafkaTopicInfo> topicInfoList = filteredDescriptions.values()
          .stream().map(desc -> topicDescriptionToTopicInfo(desc))
          .collect(Collectors.toList());

      return Optional.of(new KafkaTopicsList(statement.getStatementText(), topicInfoList));
    }
  }

  private static KafkaTopicInfo topicDescriptionToTopicInfo(final TopicDescription description) {
    return new KafkaTopicInfo(
        description.name(),
        description.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()));
  }

  private static KafkaTopicInfoExtended topicDescriptionToTopicInfoExtended(
      final TopicDescription topicDescription,
      final Map<String, List<Integer>> topicConsumersAndGroupCount
  ) {

    final List<Integer> consumerAndGroupCount = topicConsumersAndGroupCount
        .getOrDefault(topicDescription.name(), Arrays.asList(0, 0));

    return new KafkaTopicInfoExtended(
        topicDescription.name(),
        topicDescription.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()),
        consumerAndGroupCount.get(0),
        consumerAndGroupCount.get(1));
  }

  private static Map<String, TopicDescription> filterKsqlInternalTopics(
      final Map<String, TopicDescription> kafkaTopicDescriptions,
      final KsqlConfig ksqlConfig
  ) {
    final Map<String, TopicDescription> filteredKafkaTopics = new HashMap<>();
    final String serviceId = KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
        + ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String persistentQueryPrefix = ksqlConfig.getString(
        KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG);
    final String transientQueryPrefix = ksqlConfig.getString(
        KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG);

    for (final Map.Entry<String, TopicDescription> entry : kafkaTopicDescriptions.entrySet()) {
      if (!entry.getKey().startsWith(serviceId + persistentQueryPrefix)
          && !entry.getKey().startsWith(serviceId + transientQueryPrefix)) {
        filteredKafkaTopics.put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }
    return filteredKafkaTopics;
  }

  /**
   * @return all topics with their associated consumerCount and consumerGroupCount
   */
  private static Map<String, List<Integer>> getTopicConsumerAndGroupCounts(
      final KafkaConsumerGroupClient consumerGroupClient
  ) {

    final List<String> consumerGroups = consumerGroupClient.listGroups();

    final Map<String, AtomicInteger> topicConsumerCount = new HashMap<>();
    final Map<String, Set<String>> topicConsumerGroupCount = new HashMap<>();

    for (final String group : consumerGroups) {
      final Collection<ConsumerSummary> consumerSummaryList =
          consumerGroupClient.describeConsumerGroup(group).consumers();

      for (final KafkaConsumerGroupClientImpl.ConsumerSummary summary : consumerSummaryList) {

        for (final TopicPartition topicPartition : summary.partitions()) {
          topicConsumerCount
              .computeIfAbsent(topicPartition.topic(), k -> new AtomicInteger())
              .incrementAndGet();
          topicConsumerGroupCount
              .computeIfAbsent(topicPartition.topic(), k -> new HashSet<>()).add(group);
        }
      }
    }
    final HashMap<String, List<Integer>> results = new HashMap<>();
    topicConsumerCount.forEach(
        (k, v) -> {
          results.computeIfAbsent(k, v1 -> new ArrayList<>()).add(v.intValue());
          results.get(k).add(topicConsumerGroupCount.get(k).size());
        }
    );

    return results;
  }
}
