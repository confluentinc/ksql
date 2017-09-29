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

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TopicConsumer {

  public static final long RESULTS_POLL_MAX_TIME_MS = 30000;
  public static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  private final EmbeddedSingleNodeKafkaCluster cluster;

  public TopicConsumer(EmbeddedSingleNodeKafkaCluster cluster) {
    this.cluster = cluster;
  }

  public <K> Map<K, GenericRow> readResults(
      String topic,
      Schema schema,
      int expectedNumMessages,
      Deserializer<K> keyDeserializer
  ) {
    Map<K, GenericRow> result = new HashMap<>();

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "filter-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<K, GenericRow> consumer =
             new KafkaConsumer<>(consumerConfig, keyDeserializer, new KsqlJsonDeserializer(schema))
    ) {
      consumer.subscribe(Collections.singleton(topic));
      long pollStart = System.currentTimeMillis();
      long pollEnd = pollStart + RESULTS_POLL_MAX_TIME_MS;
      while (System.currentTimeMillis() < pollEnd && continueConsuming(result.size(), expectedNumMessages)) {
        for (ConsumerRecord<K, GenericRow> record : consumer.poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
          if (record.value() != null) {
            result.put(record.key(), record.value());
          }
        }
      }

      for (ConsumerRecord<K, GenericRow> record : consumer.poll(RESULTS_EXTRA_POLL_TIME_MS)) {
        if (record.value() != null) {
          result.put(record.key(), record.value());
        }
      }
    }
    return result;
  }

  private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
    return maxMessages < 0 || messagesConsumed < maxMessages;
  }

}
