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

package io.confluent.ksql.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CleanUpUtil {

  final KafkaTopicClient kafkaTopicClient;

  public CleanUpUtil(KafkaTopicClient kafkaTopicClient) {
    this.kafkaTopicClient = kafkaTopicClient;
  }

  public void cleanUpQuery(QueryMetadata queryMetadata) {
    Set<String> topicSet = kafkaTopicClient.listTopicNames();
    if (topicSet == null) {
      return;
    }
    List<String> topicsToDeleteList = new ArrayList<>();
    for (String topicName: topicSet) {
      if (isInternalTopic(topicName, queryMetadata.getQueryApplicationId())) {
        topicsToDeleteList.add(topicName);
      }
    }
    if (!topicsToDeleteList.isEmpty()) {
      kafkaTopicClient.deleteTopics(topicsToDeleteList);
    }
  }

  private boolean isInternalTopic(final String topicName, final String applicationId) {
    return topicName.startsWith(applicationId + "-")
           && (topicName.endsWith(KsqlConfig.STREAM_INTERNAL_CHANGELOG_TOPIC_SUFFIX) ||
               topicName.endsWith(KsqlConfig.STREAM_INTERNAL_REPARTITION_TOPIC_SUFFIX));
  }

}
