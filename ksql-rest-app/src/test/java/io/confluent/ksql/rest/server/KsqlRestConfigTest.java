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

package io.confluent.ksql.rest.server;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.rest.RestConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

public class KsqlRestConfigTest {

  private static final Map<String, ?> MIN_VALID_CONFIGS = ImmutableMap.<String, Object>builder()
      .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      .put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test")
      .put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .build();

  @Test
  public void testGetKsqlConfigProperties() {
    final Map<String, Object> inputProperties = new HashMap<>(MIN_VALID_CONFIGS);
    inputProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    inputProperties.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test");

    final KsqlRestConfig config = new KsqlRestConfig(inputProperties);

    final Map<String, Object> ksqlConfigProperties = config.getKsqlConfigProperties();
    final Map<String, Object> expectedKsqlConfigProperties = new HashMap<>();
    expectedKsqlConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    expectedKsqlConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
    expectedKsqlConfigProperties.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    expectedKsqlConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    expectedKsqlConfigProperties.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test");
    assertThat(ksqlConfigProperties, equalTo(expectedKsqlConfigProperties));
  }

  // Just a sanity check to make sure that, although they contain identical mappings, successive maps returned by calls
  // to KsqlRestConfig.getOriginals() do not actually return the same object (mutability would then be an issue)
  @Test
  public void testOriginalsReplicability() {
    final String COMMIT_INTERVAL_MS = "10";

    final Map<String, Object> inputProperties = new HashMap<>(MIN_VALID_CONFIGS);
    inputProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
    final KsqlRestConfig config = new KsqlRestConfig(inputProperties);

    final Map<String, Object> originals1 = config.getOriginals();
    final Map<String, Object> originals2 = config.getOriginals();

    assertEquals(originals1, originals2);
    assertNotSame(originals1, originals2);
    assertEquals(COMMIT_INTERVAL_MS, originals1.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
    assertEquals(COMMIT_INTERVAL_MS, originals2.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
  }

  @Test
  public void ensureCorrectCommandTopicName() {
    final String commandTopicName = KsqlRestConfig.getCommandTopic("TestKSql");
    assertThat(commandTopicName,
               equalTo("_confluent-ksql-TestKSql_" + KsqlRestConfig.COMMAND_TOPIC_SUFFIX));
  }
}
