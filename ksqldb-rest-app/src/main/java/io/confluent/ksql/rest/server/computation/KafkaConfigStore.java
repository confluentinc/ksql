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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.server.computation.ConfigTopicKey.StringKey;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaConfigStore implements ConfigStore {
  private static final Logger log = LogManager.getLogger(KafkaConfigStore.class);

  public static final String CONFIG_MSG_KEY = "ksql-standalone-configs";

  private final KsqlConfig ksqlConfig;

  private static KafkaConsumer<byte[], byte[]> createConsumer(
      final KsqlConfig ksqlConfig) {
    return new KafkaConsumer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer());
  }

  private static KafkaProducer<StringKey, KsqlProperties> createProducer(
      final KsqlConfig ksqlConfig) {
    return new KafkaProducer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        InternalTopicSerdes.serializer(),
        InternalTopicSerdes.serializer()
    );
  }

  /**
   * KafkaConfigstore reads, and possibly writes, the KSQL config from/to a given Kafka topic,
   * and merges it with the provided config. It is meant to be used by headless-mode KSQL. The
   * config is written if the topic does not already contain a KSQL config. By merging the
   * provided config with the config saved in Kafka, we can ensure that headless mode KSQL can
   * always run queries compatibly across versions, while still allowing to the user to change
   * some configurations.
   *
   * @param topicName The name of the topic to use to store the config.
   * @param currentConfig The current KSQL config, to be merged with the config stored in Kafka.
   */
  public KafkaConfigStore(final String topicName, final KsqlConfig currentConfig) {
    this(
        topicName,
        currentConfig,
        () -> createConsumer(currentConfig),
        () -> createProducer(currentConfig)
    );
  }

  // for testing
  KafkaConfigStore(
      final String topicName,
      final KsqlConfig currentConfig,
      final Supplier<KafkaConsumer<byte[], byte[]>> consumer,
      final Supplier<KafkaProducer<StringKey, KsqlProperties>> producer) {
    final KsqlProperties currentProperties = KsqlProperties.createFor(currentConfig);
    final KsqlProperties savedProperties = new KafkaWriteOnceStore<>(
        topicName,
        new StringKey(CONFIG_MSG_KEY),
        InternalTopicSerdes.deserializer(ConfigTopicKey.class),
        InternalTopicSerdes.deserializer(KsqlProperties.class),
        consumer,
        producer
    ).readMaybeWrite(currentProperties);
    this.ksqlConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(
        savedProperties.getKsqlProperties());
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  public static class KsqlProperties {
    private final Map<String, String> ksqlProperties;

    @SuppressWarnings("UnstableApiUsage")
    @JsonCreator
    KsqlProperties(
        @JsonProperty("ksqlProperties") final Optional<Map<String, String>> ksqlProperties) {
      this.ksqlProperties = ksqlProperties.isPresent()
          ? ksqlProperties.get().entrySet()
              .stream()
              .filter(kv -> kv.getValue() != null)
              .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue))
          : Collections.emptyMap();
    }

    @SuppressWarnings("DefaultAnnotationParam") // ALWAYS overrides the default set on the mapper.
    @JsonInclude(content = Include.ALWAYS)
    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public Map<String, String> getKsqlProperties() {
      return ksqlProperties;
    }

    static KsqlProperties createFor(final KsqlConfig ksqlConfig) {
      return new KsqlProperties(Optional.of(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final KsqlProperties that = (KsqlProperties) o;
      return Objects.equals(ksqlProperties, that.ksqlProperties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ksqlProperties);
    }
  }

  private static class KafkaWriteOnceStore<V> {
    private final String topicName;
    private final StringKey key;
    private final Deserializer<ConfigTopicKey> keyDeserializer;
    private final Deserializer<V> deserializer;
    private final Supplier<KafkaConsumer<byte[], byte[]>> consumerSupplier;
    private final Supplier<KafkaProducer<StringKey, V>> producerSupplier;

    KafkaWriteOnceStore(
        final String topicName,
        final StringKey key,
        final Deserializer<ConfigTopicKey> keyDeserializer,
        final Deserializer<V> deserializer,
        final Supplier<KafkaConsumer<byte[], byte[]>> consumerSupplier,
        final Supplier<KafkaProducer<StringKey, V>> producerSupplier) {
      this.topicName = topicName;
      this.key = key;
      this.keyDeserializer = keyDeserializer;
      this.deserializer = deserializer;
      this.consumerSupplier = consumerSupplier;
      this.producerSupplier = producerSupplier;
    }

    private boolean matchKey(final ConsumerRecord<byte[], byte[]> record) {
      try {
        final ConfigTopicKey recordKey = keyDeserializer.deserialize(topicName, record.key());
        return this.key.equals(recordKey);
      } catch (final SerializationException e) {
        return false;
      }
    }

    private Optional<V> read() {
      final TopicPartition topicPartition = new TopicPartition(topicName, 0);
      final List<TopicPartition> topicPartitionAsList = Collections.singletonList(topicPartition);

      try (KafkaConsumer<byte[], byte[]> consumer = consumerSupplier.get()) {
        consumer.assign(topicPartitionAsList);
        consumer.seekToBeginning(topicPartitionAsList);

        final Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitionAsList);
        final long endOffset = offsets.get(topicPartition);
        while (consumer.position(topicPartition) < endOffset) {
          log.debug(
              "Reading from topic {}. Position({}) End({})",
              topicName,
              consumer.position(topicPartition),
              endOffset);
          final ConsumerRecords<byte[], byte[]> records
              = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
          final Optional<ConsumerRecord<byte[], byte[]>> record =
              records.records(topicPartition)
                  .stream()
                  .filter(this::matchKey)
                  .findFirst();
          if (!record.isPresent()) {
            continue;
          }
          final byte[] value = record.get().value();
          log.debug("Found existing value in topic {}", topicName);
          return Optional.of(deserializer.deserialize(topicName, value));
        }
        log.debug("No value found on topic {}", topicName);
        return Optional.empty();
      }
    }

    private void write(final String topicName, final V value) {
      try (KafkaProducer<ConfigTopicKey.StringKey, V> producer = producerSupplier.get()) {
        producer.send(new ProducerRecord<>(topicName, key, value));
        producer.flush();
      }
    }

    V readMaybeWrite(final V value) {
      final Optional<V> properties = read();
      if (properties.isPresent()) {
        return properties.get();
      }

      log.debug("Writing current config to config topic");
      write(topicName, value);

      return read().get();
    }
  }
}
