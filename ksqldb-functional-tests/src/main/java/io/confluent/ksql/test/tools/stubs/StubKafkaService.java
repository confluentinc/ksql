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

package io.confluent.ksql.test.tools.stubs;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class StubKafkaService {

  private final Map<String, Topic> topicMap;
  private final Map<String, List<ProducerRecord<byte[], byte[]>>> producedRecords;

  public static StubKafkaService create() {
    return new StubKafkaService();
  }

  private StubKafkaService() {
    this.topicMap = new HashMap<>();
    this.producedRecords = new HashMap<>();
  }

  public void ensureTopic(final Topic topic) {
    topicMap.put(topic.getName(), topic);
    producedRecords.putIfAbsent(topic.getName(), new ArrayList<>());
  }

  public void writeRecord(final ProducerRecord<byte[], byte[]> record) {
    requireTopicExists(record.topic());
    producedRecords.get(record.topic()).add(record);
  }

  public List<ProducerRecord<byte[], byte[]>> readRecords(final String topicName) {
    requireTopicExists(topicName);
    return ImmutableList.copyOf(producedRecords.get(topicName));
  }

  public void requireTopicExists(final String topicName) {
    if (!topicMap.containsKey(topicName)) {
      throw new KsqlException("Topic does not exist: " + topicName);
    }
  }

  public Topic getTopic(final String topicName) {
    return topicMap.get(topicName);
  }

  public Collection<Topic> getAllTopics() {
    return topicMap.values();
  }
}