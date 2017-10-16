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

package io.confluent.ksql.rest.server.mock;

import io.confluent.ksql.util.KafkaTopicClient;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fake Kafka Client is for test only, none of its methods should be called.
 */
public class MockKafkaTopicClient implements KafkaTopicClient {

  @Override
  public void createTopic(String topic, int numPartitions, short replicatonFactor) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public boolean isTopicExists(String topic) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public Set<String> listTopicNames() {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public Map<String, TopicDescription> describeTopics(Collection<String> topicNames) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public void deleteTopics(List<String> topicsToDelete) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public void deleteInternalTopics(String applicationId) {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Calling method on FakeObject");
  }

}