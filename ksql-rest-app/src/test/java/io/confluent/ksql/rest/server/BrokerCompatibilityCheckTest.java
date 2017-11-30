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

import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;


public class BrokerCompatibilityCheckTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Test
  public void shouldBeCompatibleWithCurrentBrokerVersion() {
    Map<String, Object> config = new HashMap<>();
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "appid");
    final BrokerCompatibilityCheck check = BrokerCompatibilityCheck.create(BrokerCompatibilityCheck.Config.fromStreamsConfig(config));
    // shouldn't throw any exceptions
    check.checkCompatibility();
  }
}