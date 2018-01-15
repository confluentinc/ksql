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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StringUtil;
import kafka.admin.AdminClient;
import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@JsonTypeName("kafka_topics")
@JsonSubTypes({})
public class KafkaTopicsList extends KsqlEntity {
  private final Collection<KafkaTopicInfo> topics;

  @JsonCreator
  public KafkaTopicsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("kafka_topics")   Collection<KafkaTopicInfo> topics
  ) {
    super(statementText);
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

  public static KafkaTopicsList build(String statementText,
                                      Collection<KsqlTopic> ksqlTopics,
                                      Map<String, TopicDescription> kafkaTopicDescriptions,
                                      KsqlConfig ksqlConfig) {
    Set<String> registeredNames = getRegisteredKafkaTopicNames(ksqlTopics);

    List<KafkaTopicInfo> kafkaTopicInfoList = new ArrayList<>();
    kafkaTopicDescriptions = new TreeMap<>(filterKsqlInternalTopics(kafkaTopicDescriptions,
                                                                    ksqlConfig));

    Map<String, List<Integer>> topicConsumersAndGroupCount = getTopicConsumerAndGroupInfo(ksqlConfig);

    for (TopicDescription desp: kafkaTopicDescriptions.values()) {
      kafkaTopicInfoList.add(new KafkaTopicInfo(
          desp.name(),
          String.valueOf(registeredNames.contains(desp.name())),
          String.valueOf(desp.partitions().size()),
          String.valueOf(getTopicReplicaInfo(desp.partitions())),
              topicConsumersAndGroupCount.getOrDefault(desp.name(), Arrays.asList(0, 0)).get(0),
              topicConsumersAndGroupCount.getOrDefault(desp.name(), Arrays.asList(0, 0)).get(1)
      ));
    }
    return new KafkaTopicsList(statementText, kafkaTopicInfoList);
  }

  private static Map<String, List<Integer>> getTopicConsumerAndGroupInfo(KsqlConfig ksqlConfig) {

      Map<String, Object> clientConfigProps = ksqlConfig.getKsqlAdminClientConfigProps();
      String[] args = {
        "--bootstrap-server", (String) clientConfigProps.get("bootstrap.servers")
      };

      System.out.println(ConsumerGroupCommand.class.getName());

      ConsumerGroupCommand.ConsumerGroupCommandOptions opts = new ConsumerGroupCommand.ConsumerGroupCommandOptions(args);
      ConsumerGroupCommand.KafkaConsumerGroupService svc = new ConsumerGroupCommand.KafkaConsumerGroupService(opts);
      scala.collection.immutable.List<String> groups = svc.listGroups();
      scala.collection.Iterator<String> iterator = groups.iterator();

      Map<String, AtomicInteger> topicConsumerCount = new HashMap<>();
      Map<String, Set<String>> topicConsumerGroupCount = new HashMap<>();

      Properties props = new Properties();
      props.putAll(clientConfigProps);
      AdminClient adminClient = AdminClient.create(props);


      while (iterator.hasNext()) {
        String group = iterator.next();

        AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(group, 1000);
        scala.collection.immutable.List<AdminClient.ConsumerSummary> consumerSummaryList = consumerGroupSummary.consumers().get();

        scala.collection.Iterator<AdminClient.ConsumerSummary> consumerSummaryIterator = consumerSummaryList.iterator();
        while (consumerSummaryIterator.hasNext()) {
          AdminClient.ConsumerSummary summary = consumerSummaryIterator.next();

          scala.collection.immutable.List<TopicPartition> assignment = summary.assignment();
          scala.collection.Iterator<TopicPartition> partitionIterator = assignment.iterator();
          while (partitionIterator.hasNext()) {
            TopicPartition topicPartition = partitionIterator.next();
            topicConsumerCount.computeIfAbsent(topicPartition.topic(), k -> new AtomicInteger()).incrementAndGet();
            topicConsumerGroupCount.computeIfAbsent(topicPartition.topic(), k -> new HashSet<String>()).add(group);
          }
        }
      }
      HashMap<String, List<Integer>> results = new HashMap<>();
      topicConsumerCount.forEach((k, v) -> {
        results.computeIfAbsent(k,  v1 -> new ArrayList<>()).add(v.intValue());
        results.get(k).add(topicConsumerGroupCount.get(k).size());
      }
      );

      return results;
  }

  private static Set<String> getRegisteredKafkaTopicNames(Collection<KsqlTopic> ksqlTopics) {
    Set<String> registeredNames = new HashSet<>();
    for (KsqlTopic ksqlTopic: ksqlTopics) {
      registeredNames.add(ksqlTopic.getKafkaTopicName());
    }
    return registeredNames;
  }

  private static String getTopicReplicaInfo(List<TopicPartitionInfo> partitions) {
    List<Integer> replicaSizes = new ArrayList<>(partitions.size());

    for (TopicPartitionInfo partition : partitions) {
      replicaSizes.add(partition.replicas().size());
    }

    boolean sameReplicaCount = true;
    for (int i = 1; i < partitions.size(); i++) {
      if (!replicaSizes.get(i).equals(replicaSizes.get(i-1))) {
        sameReplicaCount = false;
        break;
      }
    }

    if (sameReplicaCount) {
      return partitions.size() == 0 ? "0" : String.valueOf(replicaSizes.get(0));
    } else {
      return StringUtil.join(", ", replicaSizes);
    }
  }

  private static Map<String, TopicDescription> filterKsqlInternalTopics(
      Map<String, TopicDescription> kafkaTopicDescriptions, KsqlConfig ksqlConfig) {
    Map<String, TopicDescription> filteredKafkaTopics = new HashMap<>();
    String serviceId = ksqlConfig.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
        .toString();
    String persistentQueryPrefix = ksqlConfig.get(KsqlConfig
                                                      .KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG)
        .toString();
    String transientQueryPrefix = ksqlConfig.get(KsqlConfig
                                                      .KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG)
        .toString();

    for (Map.Entry<String, TopicDescription> entry: kafkaTopicDescriptions.entrySet()) {
      if (!entry.getKey().startsWith(serviceId + persistentQueryPrefix) &&
          !entry.getKey().startsWith(serviceId + transientQueryPrefix)) {
        filteredKafkaTopics.put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }
    return filteredKafkaTopics;
  }

}
