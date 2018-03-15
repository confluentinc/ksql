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

package io.confluent.ksql.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.hamcrest.Matcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class TopicConsumer {

  private static final long RESULTS_POLL_MAX_TIME_MS = 30000;
  private static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  private final EmbeddedSingleNodeKafkaCluster cluster;

  public TopicConsumer(EmbeddedSingleNodeKafkaCluster cluster) {
    this.cluster = cluster;
  }

  public <K, V> Map<K, V> readResults(final String topic,
                                      final Matcher<Integer> expectedNumMessages,
                                      final Deserializer<V> valueDeserializer,
                                      final Deserializer<K> keyDeserializer) {
    Map<K, V> result = new HashMap<>();

    Properties consumerConfig = new Properties();
    consumerConfig.putAll(cluster.getClientProperties());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "filter-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<K, V> consumer =
             new KafkaConsumer<>(consumerConfig, keyDeserializer, valueDeserializer)
    ) {
      consumer.subscribe(Collections.singleton(topic));
      long pollStart = System.currentTimeMillis();
      long pollEnd = pollStart + RESULTS_POLL_MAX_TIME_MS;
      while (System.currentTimeMillis() < pollEnd && !expectedNumMessages.matches(result.size())) {
        for (ConsumerRecord<K, V> record : consumer
            .poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
          if (record.value() != null) {
            result.put(record.key(), record.value());
          }
        }
      }

      for (ConsumerRecord<K, V> record : consumer.poll(RESULTS_EXTRA_POLL_TIME_MS)) {
        if (record.value() != null) {
          result.put(record.key(), record.value());
        }
      }
    }
    return result;
  }

  public <K> Map<K, GenericRow> readResults(final String topic,
                                            final Schema schema,
                                            final int expectedNumMessages,
                                            final Deserializer<K> keyDeserializer) {
    return readResults(topic, greaterThanOrEqualTo(expectedNumMessages),
                       new KsqlJsonDeserializer(schema), keyDeserializer
    );
  }

  public void verifyRecordsReceived(final String topic,
                                    final Matcher<Integer> expectedNumMessages) {
    verifyRecordsReceived(topic, expectedNumMessages,
                          new ByteArrayDeserializer(),
                          new ByteArrayDeserializer());
  }

  public <K> Map<K, GenericRow> verifyRecordsReceived(final String topic,
                                                      final Schema schema,
                                                      final Matcher<Integer> expectedNumMessages,
                                                      final Deserializer<K> keyDeserializer) {
    return verifyRecordsReceived(topic, expectedNumMessages,
                                 new KsqlJsonDeserializer(schema), keyDeserializer);
  }

  public <K, V> Map<K, V> verifyRecordsReceived(final String topic,
                                                final Matcher<Integer> expectedNumMessages,
                                                final Deserializer<V> valueDeserializer,
                                                final Deserializer<K> keyDeserializer) {
    final Map<K, V> records =
        readResults(topic, expectedNumMessages, valueDeserializer, keyDeserializer);

    assertThat(records.keySet(), hasSize(expectedNumMessages));

    return records;
  }
}
