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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StringUtil;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

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

@JsonTypeName("kafka_topics")
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
    for (TopicDescription desp: kafkaTopicDescriptions.values()) {
      kafkaTopicInfoList.add(new KafkaTopicInfo(
          desp.name(),
          String.valueOf(registeredNames.contains(desp.name())),
          String.valueOf(desp.partitions().size()),
          String.valueOf(getTopicReplicaInfo(desp.partitions()))
      ));
    }
    return new KafkaTopicsList(statementText, kafkaTopicInfoList);
  }

  private static Set<String> getRegisteredKafkaTopicNames(Collection<KsqlTopic> ksqlTopics) {
    Set<String> registeredNames = new HashSet<>();
    for (KsqlTopic ksqlTopic: ksqlTopics) {
      registeredNames.add(ksqlTopic.getKafkaTopicName());
    }
    return registeredNames;
  }

  private static String getTopicReplicaInfo(List<TopicPartitionInfo> partitions) {
    int[] replicaSizes = new int[partitions.size()];

    for (int i = 0; i < partitions.size(); i++) {
      replicaSizes[i] = partitions.get(i).replicas().size();
    }

    boolean sameReplicaCount = true;
    for (int i = 1; i < partitions.size(); i++) {
      if (replicaSizes[i] != replicaSizes[i-1]) {
        sameReplicaCount = false;
        break;
      }
    }

    if (sameReplicaCount) {
      return partitions.size() == 0 ? "0" : String.valueOf(replicaSizes[0]);
    } else {
      return StringUtil.join(", ", Arrays.asList(replicaSizes));
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

    for (String kafkaTopicName: kafkaTopicDescriptions.keySet()) {
      if (!kafkaTopicName.startsWith(serviceId + persistentQueryPrefix) &&
          !kafkaTopicName.startsWith(serviceId + transientQueryPrefix)) {
        filteredKafkaTopics.put(kafkaTopicName.toLowerCase(), kafkaTopicDescriptions.get(kafkaTopicName));
      }
    }
    return filteredKafkaTopics;
  }

}
