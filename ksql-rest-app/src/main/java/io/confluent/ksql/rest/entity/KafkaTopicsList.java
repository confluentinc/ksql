/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.entity;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

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

import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;

@JsonTypeName("kafka_topics")
@JsonSubTypes({})
public class KafkaTopicsList extends KsqlEntity {

  private final Collection<KafkaTopicInfo> topics;

  @JsonCreator
  public KafkaTopicsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("topics") Collection<KafkaTopicInfo> topics
  ) {
    super(statementText);
    Preconditions.checkNotNull(topics, "topics field must not be null");
    this.topics = topics;
  }

  public List<KafkaTopicInfo> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaTopicsList)) {
      return false;
    }
    KafkaTopicsList that = (KafkaTopicsList) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }

  public static KafkaTopicsList build(
      String statementText,
      Collection<KsqlTopic> ksqlTopics,
      Map<String, TopicDescription> kafkaTopicDescriptions,
      KsqlConfig ksqlConfig,
      KafkaConsumerGroupClient consumerGroupClient
  ) {

    Set<String> registeredNames = getRegisteredKafkaTopicNames(ksqlTopics);

    List<KafkaTopicInfo> kafkaTopicInfoList = new ArrayList<>();
    kafkaTopicDescriptions = new TreeMap<>(filterKsqlInternalTopics(
        kafkaTopicDescriptions,
        ksqlConfig
    ));

    Map<String, List<Integer>> topicConsumersAndGroupCount = getTopicConsumerAndGroupCounts(
        consumerGroupClient);

    for (TopicDescription desp : kafkaTopicDescriptions.values()) {
      kafkaTopicInfoList.add(new KafkaTopicInfo(
          desp.name(),
          registeredNames.contains(desp.name()),
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
      KafkaConsumerGroupClient consumerGroupClient
  ) {

    List<String> consumerGroups = consumerGroupClient.listGroups();

    Map<String, AtomicInteger> topicConsumerCount = new HashMap<>();
    Map<String, Set<String>> topicConsumerGroupCount = new HashMap<>();

    for (String group : consumerGroups) {
      Collection<KafkaConsumerGroupClientImpl.ConsumerSummary> consumerSummaryList =
          consumerGroupClient.describeConsumerGroup(group).consumers();

      for (KafkaConsumerGroupClientImpl.ConsumerSummary consumerSummary : consumerSummaryList) {

        for (TopicPartition topicPartition : consumerSummary.partitions()) {
          topicConsumerCount
              .computeIfAbsent(topicPartition.topic(), k -> new AtomicInteger())
              .incrementAndGet();
          topicConsumerGroupCount
              .computeIfAbsent(topicPartition.topic(), k -> new HashSet<>()).add(group);
        }
      }
    }
    HashMap<String, List<Integer>> results = new HashMap<>();
    topicConsumerCount.forEach(
        (k, v) -> {
          results.computeIfAbsent(k, v1 -> new ArrayList<>()).add(v.intValue());
          results.get(k).add(topicConsumerGroupCount.get(k).size());
        }
    );

    return results;
  }

  private static Set<String> getRegisteredKafkaTopicNames(Collection<KsqlTopic> ksqlTopics) {
    Set<String> registeredNames = new HashSet<>();
    for (KsqlTopic ksqlTopic : ksqlTopics) {
      registeredNames.add(ksqlTopic.getKafkaTopicName());
    }
    return registeredNames;
  }

  private static Map<String, TopicDescription> filterKsqlInternalTopics(
      Map<String, TopicDescription> kafkaTopicDescriptions, KsqlConfig ksqlConfig
  ) {
    Map<String, TopicDescription> filteredKafkaTopics = new HashMap<>();
    String serviceId = KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX
                       + ksqlConfig.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG).toString();
    String persistentQueryPrefix = ksqlConfig.get(
        KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG
    ).toString();
    String transientQueryPrefix = ksqlConfig.get(
        KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG
    ).toString();

    for (Map.Entry<String, TopicDescription> entry : kafkaTopicDescriptions.entrySet()) {
      if (!entry.getKey().startsWith(serviceId + persistentQueryPrefix)
          && !entry.getKey().startsWith(serviceId + transientQueryPrefix)) {
        filteredKafkaTopics.put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }
    return filteredKafkaTopics;
  }

}
