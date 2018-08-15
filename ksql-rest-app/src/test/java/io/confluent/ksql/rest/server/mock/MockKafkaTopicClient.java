/*
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

package io.confluent.ksql.rest.server.mock;

import io.confluent.ksql.util.KafkaTopicClient;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class MockKafkaTopicClient implements KafkaTopicClient {

  @Override
  public void createTopic(final String topic,
                          final int numPartitions,
                          final short replicationFactor,
                          final Map<String, ?> configs) {

  }

  @Override
  public boolean isTopicExists(final String topic) {
    return true;
  }

  @Override
  public Set<String> listTopicNames() {
    return Collections.emptySet();
  }

  @Override
  public Set<String> listNonInternalTopicNames() {
    return Collections.EMPTY_SET;
  }

  @Override
  public Map<String, TopicDescription> describeTopics(final Collection<String> topicNames) {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, String> getTopicConfig(final String topicName) {
    return Collections.emptyMap();
  }

  @Override
  public boolean addTopicConfig(final String topicName, final Map<String, ?> overrides) {
    return false;
  }

  @Override
  public TopicCleanupPolicy getTopicCleanupPolicy(final String topicName) {
    return null;
  }

  @Override
  public void deleteTopics(final List<String> topicsToDelete) {
  }

  @Override
  public void deleteInternalTopics(final String applicationId) {
  }

  @Override
  public void close() {
  }
}