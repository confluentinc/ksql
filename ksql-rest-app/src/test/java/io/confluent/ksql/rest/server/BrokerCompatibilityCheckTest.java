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
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.util.KsqlException;


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

  @Test(expected = KsqlException.class)
  public void shouldThrowKsqlExceptionIfNoTopicsAvailableToCheck() {
    BrokerCompatibilityCheck.create(Collections.emptyMap(), Collections.emptySet());
  }
}