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

package io.confluent.ksql.datagen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.MockSystemExit;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class DataGenFunctionalTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Deserializer<byte[]> BYTE_DESERIALIZER = new ByteArrayDeserializer();
  private static final Deserializer<String> KAFKA_STRING_DESERIALIZER = new StringDeserializer();

  private static final int DEFAULT_MESSAGE_COUNT = 5;

  private static final Map<String, String> DEFAULT_ARGS = ImmutableMap.of(
      "quickstart", "users",
      "key-format", "kafka",
      "value-format", "json",
      "msgRate", "100",
      "iterations", "" + DEFAULT_MESSAGE_COUNT
  );

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER =
      EmbeddedSingleNodeKafkaCluster.build();

  private String topicName;

  @Before
  public void setUp() {
    topicName = TopicTestUtil.uniqueTopicName();
    CLUSTER.createTopic(topicName, 1, 1);
  }

  @Test
  public void shouldWorkWithoutAnyFormatSupplied() throws Throwable {
    // Given:
    final Map<String, String> args = new HashMap<>(DEFAULT_ARGS);
    args.remove("key-format");
    args.remove("value-format");

    // When:
    runWithExactArgs(args);

    // Then:
    final List<ConsumerRecord<String, String>> records = CLUSTER
        .verifyAvailableRecords(
            topicName,
            DEFAULT_MESSAGE_COUNT,
            KAFKA_STRING_DESERIALIZER,
            KAFKA_STRING_DESERIALIZER
        );

    assertKafkaKeys(records);
    assertJsonValues(records);
  }

  @Test
  public void shouldProduceDataWithKafkaFormatKeys() throws Throwable {
    // When:
    runWithArgOverrides(ImmutableMap.of(
        "key-format", "kafka"
    ));

    // Then:
    final List<ConsumerRecord<String, byte[]>> records = CLUSTER
        .verifyAvailableRecords(
            topicName,
            DEFAULT_MESSAGE_COUNT,
            KAFKA_STRING_DESERIALIZER,
            BYTE_DESERIALIZER
        );

    assertKafkaKeys(records);
  }

  @Test
  public void shouldProduceDataWithJsonFormatKeys() throws Throwable {
    // When:
    runWithArgOverrides(ImmutableMap.of(
        "key-format", "json"
    ));

    // Then:
    final List<ConsumerRecord<String, byte[]>> records = CLUSTER
        .verifyAvailableRecords(
            topicName,
            DEFAULT_MESSAGE_COUNT,
            KAFKA_STRING_DESERIALIZER,
            BYTE_DESERIALIZER
        );

    assertJsonKeys(records);
  }

  @Test
  public void shouldProduceDataWithKJsonFormatValues() throws Throwable {
    // When:
    runWithArgOverrides(ImmutableMap.of(
        "value-format", "json"
    ));

    // Then:
    final List<ConsumerRecord<byte[], String>> records = CLUSTER
        .verifyAvailableRecords(
            topicName,
            DEFAULT_MESSAGE_COUNT,
            BYTE_DESERIALIZER,
            KAFKA_STRING_DESERIALIZER
        );

    assertJsonValues(records);
  }

  private static <V> void assertKafkaKeys(
      final List<ConsumerRecord<String, V>> records
  ) {
    records.forEach(r -> {
      assertThat(r.key(), startsWith("User_"));
    });
  }

  private static <V> void assertJsonKeys(
      final List<ConsumerRecord<String, V>> records
  ) {
    records.forEach(r -> {
      assertThat(r.key(), startsWith("\"User_"));
    });
  }

  private static <K> void assertJsonValues(
      final List<ConsumerRecord<K, String>> records
  ) throws IOException {
    for (final ConsumerRecord<?, String> r : records) {
      final Map<?, ?> value = OBJECT_MAPPER.readValue(r.value(), Map.class);
      assertThat(value.keySet(), containsInAnyOrder(
          "registertime",
          "userid",
          "regionid",
          "gender"
      ));
    }
  }

  private void runWithArgOverrides(final Map<String, String> additionalArgs) throws Throwable {
    final Map<String, String> args = new HashMap<>(DEFAULT_ARGS);
    args.putAll(additionalArgs);

    runWithExactArgs(args);
  }

  private void runWithExactArgs(final Map<String, String> args) throws Throwable {
    args.put("topic", topicName);
    args.put("bootstrap-server", CLUSTER.bootstrapServers());

    final String[] argArray = args.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .toArray(String[]::new);

    DataGen.run(new MockSystemExit(), argArray);
  }
}
