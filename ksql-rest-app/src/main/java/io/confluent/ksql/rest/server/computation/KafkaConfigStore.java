/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConfigStore implements ConfigStore {
  private static final Logger log = LoggerFactory.getLogger(KafkaConfigStore.class);

  private static final String CONFIG_TOPIC_SUFFIX = "configs";
  private static final String CONFIG_MSG_KEY = "ksql-standalone-configs";

  private final KsqlConfig ksqlConfig;

  static Deserializer<KsqlProperties> createDeserializer() {
    final KafkaJsonDeserializer<KsqlProperties> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(
        ImmutableMap.of(
            "json.fail.unknown.properties", false,
            "json.value.type", KsqlProperties.class
        ),
        false
    );
    return deserializer;
  }

  private static KafkaConsumer<String, KsqlProperties> createConsumer(
      final KsqlConfig ksqlConfig) {
    return new KafkaConsumer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        Serdes.String().deserializer(),
        createDeserializer());
  }

  private static KafkaProducer<String, KsqlProperties> createProducer(
      final KsqlConfig ksqlConfig) {
    final Serializer<KsqlProperties> serializer = new KafkaJsonSerializer<>();
    serializer.configure(Collections.emptyMap(), false);
    return new KafkaProducer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        Serdes.String().serializer(),
        serializer);
  }

  public KafkaConfigStore(final KsqlConfig currentConfig, final KafkaTopicClient topicClient) {
    this.ksqlConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(
        readMaybeWriteProperties(
            currentConfig,
            topicClient,
            () -> createConsumer(currentConfig),
            () -> createProducer(currentConfig)
        )
    );
  }

  // for testing
  KafkaConfigStore(final KsqlConfig currentConfig,
                   final KafkaTopicClient topicClient,
                   final Supplier<KafkaConsumer<String, KsqlProperties>> consumer,
                   final Supplier<KafkaProducer<String, KsqlProperties>> producer) {
    this.ksqlConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(
        readMaybeWriteProperties(currentConfig, topicClient, consumer, producer)
    );
  }

  @Override
  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public static class KsqlProperties {
    private final Map<String, String> ksqlProperties;

    @JsonCreator
    KsqlProperties(
        @JsonProperty("ksqlProperties") final Map<String, String> ksqlProperties) {
      this.ksqlProperties = ksqlProperties == null
          ? Collections.emptyMap() : Collections.unmodifiableMap(ksqlProperties);
    }

    public Map<String, String> getKsqlProperties() {
      return ksqlProperties;
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof KsqlProperties
          && Objects.equals(ksqlProperties, ((KsqlProperties) o).ksqlProperties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ksqlProperties);
    }
  }

  private Optional<KsqlProperties> readProperties(
      final String topicName,
      final Supplier<KafkaConsumer<String, KsqlProperties>> consumerSupplier) {
    final TopicPartition topicPartition = new TopicPartition(topicName, 0);
    final List<TopicPartition> topicPartitionAsList = Collections.singletonList(topicPartition);

    try (KafkaConsumer<String, KsqlProperties> consumer = consumerSupplier.get()) {
      consumer.assign(topicPartitionAsList);
      consumer.seekToBeginning(topicPartitionAsList);

      final Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitionAsList);
      final long endOffset = offsets.get(topicPartition);
      while (consumer.position(topicPartition) < endOffset) {
        log.debug(
            "Reading from config topic. Position(%d) End(%d)",
            consumer.position(topicPartition),
            endOffset);
        final ConsumerRecords<String, KsqlProperties> records
            = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        final Optional<ConsumerRecord<String, KsqlProperties>> record =
            records.records(topicPartition)
                .stream()
                .filter(r -> r.key().equals(CONFIG_MSG_KEY))
                .findFirst();
        if (record.isPresent()) {
          log.info("Found existing config in config topic");
          return Optional.of(record.get().value());
        }
      }
      log.info("No config found on config topic");
      return Optional.empty();
    }
  }

  private void writeProperties(
      final String topicName,
      final KsqlProperties properties,
      final Supplier<KafkaProducer<String, KsqlProperties>> producerSupplier) {
    try (KafkaProducer<String, KsqlProperties> producer = producerSupplier.get()) {
      producer.send(new ProducerRecord<>(topicName, CONFIG_MSG_KEY, properties));
      producer.flush();
    }
  }

  private Map<String, String> readMaybeWriteProperties(
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient topicClient,
      final Supplier<KafkaConsumer<String, KsqlProperties>> consumerSupplier,
      final Supplier<KafkaProducer<String, KsqlProperties>> producerSupplier) {
    final String topicName = KsqlInternalTopicUtils.getTopicName(ksqlConfig, CONFIG_TOPIC_SUFFIX);
    log.info("Ensure existence of topic %s", topicName);
    KsqlInternalTopicUtils.ensureTopic(
        topicName, ksqlConfig, topicClient, TopicConfig.CLEANUP_POLICY_COMPACT);

    final Optional<KsqlProperties> properties = readProperties(topicName, consumerSupplier);
    if (properties.isPresent()) {
      return properties.get().getKsqlProperties();
    }

    log.info("Writing current config to config topic");
    writeProperties(topicName, new KsqlProperties(
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated()), producerSupplier);

    return readProperties(topicName, consumerSupplier).get().getKsqlProperties();
  }
}
