/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.serde.GenericRowSerDe.GenericRowSerializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TopicProducer {

  public static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;

  private final EmbeddedSingleNodeKafkaCluster cluster;
  private final Properties producerConfig;

  public TopicProducer(final EmbeddedSingleNodeKafkaCluster cluster) {
    this.cluster = cluster;

    this.producerConfig = new Properties();
    producerConfig.putAll(cluster.getClientProperties());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
  }

  /**
   * Topic topicName will be automatically created if it doesn't exist.
   * @param topicName
   * @param recordsToPublish
   * @param schema
   * @return
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public Map<String, RecordMetadata> produceInputData(
      final String topicName,
      final Map<String, GenericRow> recordsToPublish,
      final KsqlSchema schema
  ) throws InterruptedException, TimeoutException, ExecutionException {

    final Serializer<GenericRow> serializer = new GenericRowSerializer(
        new KsqlJsonSerializer(schema.getSchema()),
        schema.getSchema());

    final KafkaProducer<String, GenericRow> producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), serializer);

    final Map<String, RecordMetadata> result = new HashMap<>();
    for (final Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      final String key = recordEntry.getKey();
      final ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(topicName, key, recordEntry.getValue());
      final Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
      result.put(key, recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }

  /**
   * Produce input data to the topic named dataProvider.topicName()
   */
  public Map<String, RecordMetadata> produceInputData(final TestDataProvider dataProvider) throws Exception {
    return produceInputData(dataProvider.topicName(), dataProvider.data(), dataProvider.schema());
  }

}
