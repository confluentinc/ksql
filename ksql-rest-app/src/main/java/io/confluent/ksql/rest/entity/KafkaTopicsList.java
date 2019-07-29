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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaTopicsList extends KsqlEntity {

  private final Collection<KafkaTopicInfo> topics;

  @JsonCreator
  public KafkaTopicsList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("topics") final Collection<KafkaTopicInfo> topics
  ) {
    super(statementText);
    Preconditions.checkNotNull(topics, "topics field must not be null");
    this.topics = topics;
  }

  public List<KafkaTopicInfo> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaTopicsList)) {
      return false;
    }
    final KafkaTopicsList that = (KafkaTopicsList) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }

  public static KafkaTopicsList build(
      final String statementText,
      final Map<String, TopicDescription> kafkaTopicDescriptions,
      final KsqlConfig ksqlConfig,
      final KafkaConsumerGroupClient consumerGroupClient
  ) {
    final List<KafkaTopicInfo> kafkaTopicInfoList = new ArrayList<>();
    final Map<String, TopicDescription> filteredDescriptions = new TreeMap<>(
        filterKsqlInternalTopics(kafkaTopicDescriptions, ksqlConfig));

    final Map<String, List<Integer>> topicConsumersAndGroupCount = getTopicConsumerAndGroupCounts(
        consumerGroupClient);

    for (final TopicDescription desp : filteredDescriptions.values()) {
      kafkaTopicInfoList.add(new KafkaTopicInfo(
          desp.name(),
          desp.partitions()
              .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()),
          topicConsumersAndGroupCount.getOrDefault(desp.name(), Arrays.asList(0, 0)).get(0),
          topicConsumersAndGroupCount.getOrDefault(desp.name(), Arrays.asList(0, 0)).get(1)
      ));
    }
    return new KafkaTopicsList(statementText, kafkaTopicInfoList);
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
      final Collection<KafkaConsumerGroupClientImpl.ConsumerSummary> consumerSummaryList =
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

  private static Map<String, TopicDescription> filterKsqlInternalTopics(
      final Map<String, TopicDescription> kafkaTopicDescriptions, final KsqlConfig ksqlConfig
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

}
