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

package io.confluent.ksql.test.commons.services;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.commons.Record;
import io.confluent.ksql.test.commons.Topic;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FakeKafkaService {

  private final Map<String, Topic> topicMap;
  private final Map<String, List<Record>> topicData;

  public static FakeKafkaService create() {
    return new FakeKafkaService();
  }

  private FakeKafkaService() {
    this.topicMap = new HashMap<>();
    this.topicData = new HashMap<>();
  }

  public void createTopic(final Topic topic) {
    Objects.requireNonNull(topic);
    if (this.topicMap.containsKey(topic.getName())) {
      throw new KsqlException("Topic already exist: " +  topic.getName());
    }
    this.topicMap.put(topic.getName(), topic);
    this.topicData.put(topic.getName(), new ArrayList<>());
  }

  public void writeSingleRecoredIntoTopic(final String topicName, final Record record) {
    Objects.requireNonNull(topicName);
    Objects.requireNonNull(record);
    ensureTopicExists(topicName);
    this.topicData.get(topicName).add(record);
  }

  public void writeRecoredsIntoTopic(final String topicName, final List<Record> record) {
    Objects.requireNonNull(topicName);
    Objects.requireNonNull(record);
    ensureTopicExists(topicName);
    this.topicData.get(topicName).addAll(record);
  }

  public List<Record> readRecordsFromTopic(final Topic topic) {
    Objects.requireNonNull(topic);
    return readRecordsFromTopic(topic.getName());
  }

  public List<Record> readRecordsFromTopic(final String topicName) {
    Objects.requireNonNull(topicName);
    ensureTopicExists(topicName);
    return ImmutableList.copyOf(topicData.get(topicName));
  }

  private void ensureTopicExists(final String topicName) {
    if (!this.topicMap.containsKey(topicName)) {
      throw new KsqlException("Topic does not exist: " +  topicName);
    }
  }
}
