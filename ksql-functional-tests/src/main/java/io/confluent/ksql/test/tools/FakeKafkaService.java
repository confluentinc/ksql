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

package io.confluent.ksql.test.tools;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class FakeKafkaService {

  private final Map<String, Topic> topicMap;
  private final Map<String, List<FakeKafkaRecord>> topicData;

  public static FakeKafkaService create() {
    return new FakeKafkaService();
  }

  private FakeKafkaService() {
    this.topicMap = new HashMap<>();
    this.topicData = new HashMap<>();
  }

  public void createTopic(final Topic topic) {
    Objects.requireNonNull(topic, "Topic");
    if (this.topicMap.containsKey(topic.getName())) {
      return;
    }
    this.topicMap.put(topic.getName(), topic);
    this.topicData.put(topic.getName(), new ArrayList<>());
  }

  public void writeRecord(final String topicName, final FakeKafkaRecord record) {
    Objects.requireNonNull(topicName, "Topic");
    Objects.requireNonNull(record, "Record");
    requireTopicExists(topicName);
    this.topicData.get(topicName).add(record);
  }

  List<FakeKafkaRecord> readRecords(final String topicName) {
    Objects.requireNonNull(topicName, "Topic");
    requireTopicExists(topicName);
    return ImmutableList.copyOf(topicData.get(topicName));
  }

  public void requireTopicExists(final String topicName) {
    if (!this.topicMap.containsKey(topicName)) {
      throw new KsqlException("Topic does not exist: " +  topicName);
    }
  }

  public boolean topicExists(final Topic topic) {
    return topicMap.containsKey(topic.getName());
  }

  public void updateTopic(final Topic topic) {
    if (!topicExists(topic)) {
      throw new KsqlException("Topic does not exist: " + topic.getName());
    }
    topicMap.put(topic.getName(), topic);
  }

  public Topic getTopic(final String topicName) {
    return topicMap.get(topicName);
  }

  public Collection<Topic> getAllTopics() {
    return topicMap.values();
  }


  Map<String, List<FakeKafkaRecord>> getTopicData() {
    return topicData;
  }
}