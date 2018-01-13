/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.server;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;


public class BrokerCompatibilityCheckTest {

  private final Consumer consumer = EasyMock.createNiceMock(Consumer.class);
  private final TopicPartition topic = new TopicPartition("someTopic", 0);
  private BrokerCompatibilityCheck compatibilityCheck;

  @Before
  public void before() {
    TopicPartition topic = new TopicPartition("someTopic", 0);
    compatibilityCheck = new BrokerCompatibilityCheck(consumer, topic);
  }

  @Test(expected = KsqlException.class)
  public void shouldRaiseKsqlExceptionOnUnsupportedVersionException() {
    EasyMock.expect(consumer.offsetsForTimes(Collections.singletonMap(topic, 0L)))
        .andThrow(new UnsupportedVersionException("not supported"));
    EasyMock.replay(consumer);
    compatibilityCheck.checkCompatibility();
  }

  @Test
  public void shouldNotRaiseExceptionWhenNoUnsupportedOperationException() {
    EasyMock.replay(consumer);
    compatibilityCheck.checkCompatibility();
  }

  @Test
  public void shouldCreateTopicIfNoTopicsAvailableToCheck() {
    final KafkaTopicClient topicClient = EasyMock.createMock(KafkaTopicClient.class);
    final Map<String, Object> streamsConfig = new HashMap<>();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090");
    EasyMock.expect(topicClient.listTopicNames()).andReturn(Collections.emptySet());
    topicClient.createTopic(anyString(), eq(1), eq((short)1));
    EasyMock.expectLastCall();

    EasyMock.replay(topicClient);

    BrokerCompatibilityCheck.create(streamsConfig, topicClient);

    EasyMock.verify(topicClient);
  }
}