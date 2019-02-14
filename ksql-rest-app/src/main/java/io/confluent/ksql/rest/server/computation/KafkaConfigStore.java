/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
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

  public static final String CONFIG_MSG_KEY = "ksql-standalone-configs";

  private final KsqlConfig ksqlConfig;

  static <T> Deserializer<T> createDeserializer(Class<T> clazz) {
    final KafkaJsonDeserializer<T> deserializer = new KafkaJsonDeserializer<>();
    deserializer.configure(
        ImmutableMap.of(
            "json.fail.unknown.properties", false,
            "json.value.type", clazz
        ),
        false
    );
    return deserializer;
  }

  static <T> Serializer<T> createSerializer(Class<T> clazz, boolean isKey) {
    final Serializer<T> serializer = new KafkaJsonSerializer<>();
    serializer.configure(Collections.emptyMap(), isKey);
    return serializer;
  }

  private static KafkaConsumer<Key, KsqlProperties> createConsumer(
      final KsqlConfig ksqlConfig) {
    return new KafkaConsumer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        createDeserializer(Key.class),
        createDeserializer(KsqlProperties.class));
  }

  private static KafkaProducer<Key, KsqlProperties> createProducer(
      final KsqlConfig ksqlConfig) {
    return new KafkaProducer<>(
        ksqlConfig.getKsqlStreamConfigProps(),
        createSerializer(Key.class, true),
        createSerializer(KsqlProperties.class, false));
  }

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
      final Supplier<KafkaConsumer<Key, KsqlProperties>> consumer,
      final Supplier<KafkaProducer<Key, KsqlProperties>> producer) {
    final KsqlProperties currentProperties = KsqlProperties.createFor(currentConfig);
    final KsqlProperties savedProperties = new KafkaWriteOnceStore<>(
        topicName,
        new StringKey(CONFIG_MSG_KEY),
        consumer,
        producer
    ).readMaybeWrite(currentProperties);
    this.ksqlConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(
        savedProperties.getKsqlProperties());
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
          ? Collections.emptyMap() : ImmutableMap.copyOf(ksqlProperties);
    }

    public Map<String, String> getKsqlProperties() {
      return ksqlProperties;
    }

    static KsqlProperties createFor(final KsqlConfig ksqlConfig) {
      return new KsqlProperties(ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    }

    @Override
    public boolean equals(Object o) {
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

  @JsonTypeInfo(use = Id.NAME, include = As.WRAPPER_OBJECT, property = "type")
  @JsonSubTypes({
      @JsonSubTypes.Type(value = StringKey.class, name = "string")
  })
  public static class Key {
  }

  public final static class StringKey extends Key {
    private final String key;

    @JsonCreator
    public StringKey(@JsonProperty("key") final String key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StringKey stringKey = (StringKey) o;
      return Objects.equals(key, stringKey.key);
    }

    @Override
    public int hashCode() {

      return Objects.hash(key);
    }
  }

  private class KafkaWriteOnceStore<V> {
    private final String topicName;
    private final Key key;
    private final Supplier<KafkaConsumer<Key, V>> consumerSupplier;
    private final Supplier<KafkaProducer<Key, V>> producerSupplier;

    KafkaWriteOnceStore(
        final String topicName,
        final Key key,
        final Supplier<KafkaConsumer<Key, V>> consumerSupplier,
        final Supplier<KafkaProducer<Key, V>> producerSupplier) {
      this.topicName = topicName;
      this.key = key;
      this.consumerSupplier = consumerSupplier;
      this.producerSupplier = producerSupplier;
    }

    private Optional<V> read() {
      final TopicPartition topicPartition = new TopicPartition(topicName, 0);
      final List<TopicPartition> topicPartitionAsList = Collections.singletonList(topicPartition);

      try (KafkaConsumer<Key, V> consumer = consumerSupplier.get()) {
        consumer.assign(topicPartitionAsList);
        consumer.seekToBeginning(topicPartitionAsList);

        final Map<TopicPartition, Long> offsets = consumer.endOffsets(topicPartitionAsList);
        final long endOffset = offsets.get(topicPartition);
        while (consumer.position(topicPartition) < endOffset) {
          log.debug(
              "Reading from topic %s. Position(%d) End(%d)",
              topicName,
              consumer.position(topicPartition),
              endOffset);
          final ConsumerRecords<Key, V> records
              = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
          final Optional<ConsumerRecord<Key, V>> record =
              records.records(topicPartition)
                  .stream()
                  .filter(r -> r.key().equals(key))
                  .findFirst();
          if (record.isPresent()) {
            log.info("Found existing value in topic %s", topicName);
            return Optional.of(record.get().value());
          }
        }
        log.debug("No value found on topic %s", topicName);
        return Optional.empty();
      }
    }

    private void write(final String topicName, final V value) {
      try (KafkaProducer<Key, V> producer = producerSupplier.get()) {
        producer.send(new ProducerRecord<>(topicName, key, value));
        producer.flush();
      }
    }

    V readMaybeWrite(final V value) {
      final Optional<V> properties = read();
      if (properties.isPresent()) {
        return properties.get();
      }

      log.info("Writing current config to config topic");
      write(topicName, value);

      return read().get();
    }
  }
}
