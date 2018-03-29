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

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.common.config.TopicConfig;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.exception.KafkaTopicException;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.rest.RestConfig;

import static org.easymock.EasyMock.anyObject;


public class KsqlRestApplicationTest {

  private static final String COMMAND_TOPIC = "command_topic";
  private final KafkaTopicClient topicClient = EasyMock.createNiceMock(KafkaTopicClient.class);
  private final Map<String, ?> commandTopicConfig = Collections.singletonMap(
      TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE);
  private final KsqlRestConfig restConfig =
      new KsqlRestConfig(
          Collections.singletonMap(RestConfig.LISTENERS_CONFIG,
          "http://localhost:8088"));

  @Test
  public void shouldCreateCommandTopicIfItDoesNotExist() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 1,
        commandTopicConfig);
    EasyMock.expectLastCall();
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldNotAttemptToCreateCommandTopicIfItExists() {
    EasyMock.resetToStrict(topicClient);
    EasyMock.expect(topicClient.isTopicExists(COMMAND_TOPIC)).andReturn(true);
    EasyMock.expect(topicClient.addTopicConfig(anyObject(), anyObject())).andReturn(false);

    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldEnsureCommandTopicHasInfiniteRetention() {
    final Map<String, Object> retentionConfig = ImmutableMap.of(
        TopicConfig.RETENTION_MS_CONFIG, Long.MAX_VALUE
    );
    EasyMock.expect(topicClient.isTopicExists(COMMAND_TOPIC)).andReturn(true);
    EasyMock.expect(topicClient.addTopicConfig(COMMAND_TOPIC, retentionConfig)).andReturn(true);

    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateCommandTopicWithNumReplicasFromConfig() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 3,
        commandTopicConfig);
    EasyMock.expectLastCall();
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(
        new KsqlRestConfig(
            new HashMap(){{
              put(RestConfig.LISTENERS_CONFIG, "http://localhost:8080");
              put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, 3);
            }}),
        topicClient,
        COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

  @Test
  public void shouldNotFailIfTopicExistsOnCreation() {
    topicClient.createTopic(COMMAND_TOPIC,
        1,
        (short) 1,
        commandTopicConfig);

    EasyMock.expectLastCall().andThrow(new KafkaTopicException("blah"));
    EasyMock.replay(topicClient);

    KsqlRestApplication.ensureCommandTopic(restConfig,
                                           topicClient,
                                           COMMAND_TOPIC);

    EasyMock.verify(topicClient);
  }

}