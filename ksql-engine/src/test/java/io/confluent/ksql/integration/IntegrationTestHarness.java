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

package io.confluent.ksql.integration;

import static io.confluent.ksql.test.util.ConsumerTestUtil.hasUniqueRecords;
import static io.confluent.ksql.test.util.ConsumerTestUtil.toUniqueRecords;
import static io.confluent.ksql.test.util.MapMatchers.mapHasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.util.ConsumerTestUtil;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.TestDataProvider;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matcher;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess")
public class IntegrationTestHarness extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestHarness.class);
  private static final int DEFAULT_PARTITION_COUNT = 1;
  private static final short DEFAULT_REPLICATION_FACTOR = (short) 1;
  private static final long PRODUCE_TIMEOUT_MS = 30_000;

  private final LazyServiceContext serviceContext;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;
  private final SerdeFactories serdeFactories = new KsqlSerdeFactories();

  public static Builder builder() {
    return new Builder();
  }

  public static IntegrationTestHarness build() {
    return builder().build();
  }

  private IntegrationTestHarness(
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    this.kafkaCluster = Objects.requireNonNull(kafkaCluster, "kafkaCluster");
    this.serviceContext = new LazyServiceContext(schemaRegistryClient);
  }

  public EmbeddedSingleNodeKafkaCluster getKafkaCluster() {
    return kafkaCluster;
  }

  public String kafkaBootstrapServers() {
    return kafkaCluster.bootstrapServers();
  }

  public ServiceContext getServiceContext() {
    return serviceContext.get();
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return serviceContext.get().getSchemaRegistryClient();
  }

  public TestKsqlContext buildKsqlContext() {
    return ksqlContextBuilder().build();
  }

  public ContextBuilder ksqlContextBuilder() {
    return new ContextBuilder();
  }

  public boolean topicExists(final String topicName) {
    final KafkaTopicClient topicClient = serviceContext.get().getTopicClient();

    return topicClient.isTopicExists(topicName);
  }

  /**
   * Ensure topics with the given {@code topicNames} exist.
   *
   * <p>Topics will be creates, if they do not already exist, with a single partition and replica.
   *
   * @param topicNames the names of the topics to create.
   */
  public void ensureTopics(final String... topicNames) {
    ensureTopics(DEFAULT_PARTITION_COUNT, topicNames);
  }

  /**
   * Ensure topics with the given {@code topicNames} exist.
   *
   * <p>Topics will be creates, if they do not already exist, with the specified
   * {@code partitionCount}.
   *
   * @param topicNames the names of the topics to create.
   */
  public void ensureTopics(final int partitionCount, final String... topicNames) {
    final KafkaTopicClient topicClient = serviceContext.get().getTopicClient();

    Arrays.stream(topicNames)
        .filter(name -> !topicClient.isTopicExists(name))
        .forEach(name ->
            topicClient.createTopic(name, partitionCount, DEFAULT_REPLICATION_FACTOR));
  }

  /**
   * Produce a single record to a Kafka topic.
   *
   * @param topicName the topic to produce the record to.
   * @param key the String key of the record.
   * @param data the String value of the record.
   */
  public void produceRecord(final String topicName, final String key, final String data) {
    try {
      try (final KafkaProducer<String, String> producer =
          new KafkaProducer<>(producerConfig(), new StringSerializer(), new StringSerializer())) {
        producer.send(new ProducerRecord<>(topicName, key, data)).get();
      }
    } catch (final Exception e) {
      throw new RuntimeException("Failed to send record to " + topicName, e);
    }
  }

  /**
   * Publish test data to the supplied {@code topic}.
   *
   * @param topic the name of the topic to produce to.
   * @param dataProvider the provider of the test data.
   * @param valueFormat the format values should be produced as.
   * @return the map of produced rows
   */
  public Map<String, RecordMetadata> produceRows(
      final String topic,
      final TestDataProvider dataProvider,
      final Format valueFormat
  ) {
    return produceRows(
        topic,
        dataProvider,
        valueFormat,
        () -> null);
  }

  /**
   * Publish test data to the supplied {@code topic}.
   *
   * @param topic the name of the topic to produce to.
   * @param dataProvider the provider of the test data.
   * @param valueFormat the format values should be produced as.
   * @param timestampSupplier supplier of timestamps.
   * @return the map of produced rows
   */
  public Map<String, RecordMetadata> produceRows(
      final String topic,
      final TestDataProvider dataProvider,
      final Format valueFormat,
      final Supplier<Long> timestampSupplier
  ) {
    return produceRows(
        topic,
        dataProvider.data(),
        getSerializer(valueFormat, dataProvider.schema()),
        timestampSupplier
    );
  }

  /**
   * Publish test data to the supplied {@code topic}.
   *
   * @param topic the name of the topic to produce to.
   * @param recordsToPublish the records to produce.
   * @param valueSerializer the serializer to use to serialize values.
   * @param timestampSupplier supplier of timestamps.
   * @return the map of produced rows, with an iteration order that matches produce order.
   */
  public Map<String, RecordMetadata> produceRows(
      final String topic,
      final Map<String, GenericRow> recordsToPublish,
      final Serializer<GenericRow> valueSerializer,
      final Supplier<Long> timestampSupplier
  ) {
    ensureTopics(topic);

    try (KafkaProducer<String, GenericRow> producer =
        new KafkaProducer<>(producerConfig(), new StringSerializer(), valueSerializer)) {

      final Map<String, Future<RecordMetadata>> futures = recordsToPublish.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, entry -> {
            final String key = entry.getKey();
            final GenericRow value = entry.getValue();
            final Long timestamp = timestampSupplier.get();

            LOG.debug("Producing message. topic:{}, key:{}, value:{}, timestamp:{}",
                topic, key, value, timestamp);

            return producer.send(new ProducerRecord<>(topic, null, timestamp, key, value));
          }));

      return futures.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, entry -> {
            try {
              return entry.getValue().get(PRODUCE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }));
    }
  }

  /**
   * Verify there are {@code expectedCount} records available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<String, String>> verifyAvailableRecords(
      final String topic,
      final int expectedCount
  ) {
    try (final KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(consumerConfig(), new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(Collections.singleton(topic.toUpperCase()));

      return ConsumerTestUtil.verifyAvailableRecords(consumer, expectedCount);
    }
  }

  /**
   * Verify there are {@code expectedCount} rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<String, GenericRow>> verifyAvailableRows(
      final String topic,
      final int expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    final Deserializer<GenericRow> valueDeserializer =
        getDeserializer(valueFormat, schema);

    try (final KafkaConsumer<String, GenericRow> consumer
        = new KafkaConsumer<>(consumerConfig(), new StringDeserializer(), valueDeserializer)) {

      consumer.subscribe(Collections.singleton(topic));

      return ConsumerTestUtil.verifyAvailableRecords(consumer, expectedCount);
    }
  }

  /**
   * Verify there are {@code expected} records available to the supplied {@code consumer}.
   *
   * @param topic the name of the topic to check.
   * @param expected the expected rows.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<String, GenericRow>> verifyAvailableRows(
      final String topic,
      final Matcher<? super List<ConsumerRecord<String, GenericRow>>> expected,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    return verifyAvailableRows(topic, expected, valueFormat, schema, new StringDeserializer());
  }

  /**
   * Verify there are {@code expected} records available to the supplied {@code consumer}.
   *
   * @param topic the name of the topic to check.
   * @param expected the expected rows.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @param keyDeserializer the keyDeserilizer to use.
   * @param <K> the type of the key.
   * @return the list of consumed records.
   */
  public <K> List<ConsumerRecord<K, GenericRow>> verifyAvailableRows(
      final String topic,
      final Matcher<? super List<ConsumerRecord<K, GenericRow>>> expected,
      final Format valueFormat,
      final PhysicalSchema schema,
      final Deserializer<K> keyDeserializer
  ) {
    return verifyAvailableRows(
        topic, expected, valueFormat, schema, keyDeserializer,
        ConsumerTestUtil.DEFAULT_VERIFY_TIMEOUT);
  }

  /**
   * Verify there are {@code expected} records available to the supplied {@code consumer}.
   *
   * @param <K> the type of the key.
   * @param topic the name of the topic to check.
   * @param expected the expected rows.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @param keyDeserializer the key deserilizer to use.
   * @param timeout the max time to wait for the messages to be received.
   * @return the list of consumed records.
   */
  public <K> List<ConsumerRecord<K, GenericRow>> verifyAvailableRows(
      final String topic,
      final Matcher<? super List<ConsumerRecord<K, GenericRow>>> expected,
      final Format valueFormat,
      final PhysicalSchema schema,
      final Deserializer<K> keyDeserializer,
      final Duration timeout
  ) {
    final Deserializer<GenericRow> valueDeserializer =
        getDeserializer(valueFormat, schema);

    try (final KafkaConsumer<K, GenericRow> consumer
        = new KafkaConsumer<>(consumerConfig(), keyDeserializer, valueDeserializer)) {

      consumer.subscribe(Collections.singleton(topic));

      return ConsumerTestUtil.verifyAvailableRecords(consumer, expected, timeout);
    }
  }

  /**
   * Verify there are {@code expectedCount} unique rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @return the list of consumed records.
   */
  public Map<String, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final int expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    return verifyAvailableUniqueRows(
        topic, expectedCount, valueFormat, schema, new StringDeserializer());
  }

  /**
   * Verify there are {@code expectedCount} unique rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @param keyDeserializer the keyDeserilizer to use.
   * @param <K> the type of the key.
   * @return the list of consumed records.
   */
  public <K> Map<K, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final int expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema,
      final Deserializer<K> keyDeserializer
  ) {
    return verifyAvailableUniqueRows(topic, is(expectedCount), valueFormat, schema,
        keyDeserializer);
  }

  /**
   * Verify there are {@code expectedCount} unique rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @param keyDeserializer the keyDeserilizer to use.
   * @param <K> the type of the key.
   * @return the list of consumed records.
   */
  public <K> Map<K, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final Matcher<Integer> expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema,
      final Deserializer<K> keyDeserializer
  ) {
    final Deserializer<GenericRow> valueDeserializer =
        getDeserializer(valueFormat, schema);

    try (final KafkaConsumer<K, GenericRow> consumer
        = new KafkaConsumer<>(consumerConfig(), keyDeserializer, valueDeserializer)) {

      consumer.subscribe(Collections.singleton(topic));

      final List<ConsumerRecord<K, GenericRow>> consumerRecords = ConsumerTestUtil
          .verifyAvailableRecords(consumer, hasUniqueRecords(mapHasSize(expectedCount)));

      return toUniqueRecords(consumerRecords);
    }
  }

  /**
   * Wait for topics with names {@code topicNames} to exist in Kafka.
   *
   * @param topicNames the names of the topics to await existence for.
   */
  public void waitForTopicsToBePresent(final String... topicNames) throws Exception {
    TestUtils.waitForCondition(
        () -> {
          try {
            final KafkaTopicClient topicClient = serviceContext.get().getTopicClient();
            return Arrays.stream(topicNames)
                .allMatch(topicClient::isTopicExists);
          } catch (Exception e) {
            throw new RuntimeException("could not get subjects");
          }
        },
        30_000,
        "topics not all present after 30 seconds. topics: " + Arrays.toString(topicNames));
  }

  /**
   * Wait for topics with names {@code topicNames} to not exist in Kafka.
   *
   * @param topicNames the names of the topics to await absence for.
   */
  public void waitForTopicsToBeAbsent(final String... topicNames) throws Exception {
    TestUtils.waitForCondition(
        () -> {
          try {
            final KafkaTopicClient topicClient = serviceContext.get().getTopicClient();
            return Arrays.stream(topicNames)
                .noneMatch(topicClient::isTopicExists);
          } catch (Exception e) {
            throw new RuntimeException("could not get subjects");
          }
        },
        30_000,
        "topics not all absent after 30 seconds. topics: " + Arrays.toString(topicNames));
  }

  /**
   * Wait for a subject with name {@code subjectName} to exist in Schema Registry.
   *
   * @param subjectName the name of the subject to await existence for.
   */
  public void waitForSubjectToBePresent(final String subjectName) throws Exception {
    TestUtils.waitForCondition(
        () -> {
          try {
            return getSchemaRegistryClient().getAllSubjects().contains(subjectName);
          } catch (Exception e) {
            throw new RuntimeException("could not get subjects");
          }
        },
        30_000,
        "subject not present after 30 seconds. subject: " + subjectName);
  }

  /**
   * Wait for the subject with name {@code subjectName} to not exist in Schema Registry.
   *
   * @param subjectName the name of the subject to await absence for.
   */
  public void waitForSubjectToBeAbsent(final String subjectName) throws Exception {
    TestUtils.waitForCondition(
        () -> {
          try {
            return !getSchemaRegistryClient().getAllSubjects().contains(subjectName);
          } catch (Exception e) {
            throw new RuntimeException("could not get subjects");
          }
        },
        30_000,
        "subject still present after 30 seconds. subject: " + subjectName);
  }

  protected void before() throws Exception {
    kafkaCluster.start();
  }

  @Override
  protected void after() {
    serviceContext.close();
    kafkaCluster.stop();
  }

  private Map<String, Object> clientConfig() {
    return new HashMap<>(kafkaCluster.getClientProperties());
  }

  private Map<String, Object> producerConfig() {
    final Map<String, Object> config = clientConfig();
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, 0);
    return config;
  }

  Map<String, Object> consumerConfig() {
    final Map<String, Object> config = clientConfig();
    config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Try to keep consumer groups stable:
    config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000);
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
    return config;
  }

  private Serializer<GenericRow> getSerializer(
      final Format format,
      final PhysicalSchema schema
  ) {
    return GenericRowSerDe.from(
        serdeFactories.create(format, Optional.empty()),
        schema,
        new KsqlConfig(Collections.emptyMap()),
        serviceContext.get().getSchemaRegistryClientFactory(),
        "producer",
        ProcessingLogContext.create()
    ).serializer();
  }

  private Deserializer<GenericRow> getDeserializer(
      final Format format,
      final PhysicalSchema schema
  ) {
    return GenericRowSerDe.from(
        serdeFactories.create(format, Optional.empty()),
        schema,
        new KsqlConfig(Collections.emptyMap()),
        serviceContext.get().getSchemaRegistryClientFactory(),
        "consumer",
        ProcessingLogContext.create()
    ).deserializer();
  }

  public void ensureSchema(final String topicName, final PhysicalSchema schema) {
    final SchemaRegistryClient srClient = serviceContext.get().getSchemaRegistryClient();
    try {
      final org.apache.avro.Schema avroSchema = SchemaUtil
          .buildAvroSchema(schema.valueSchema(), "test-" + topicName);

      srClient.register(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, avroSchema);
    } catch (final Exception e) {
      throw new AssertionError(e);
    }
  }

  public static final class Builder {

    private final SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private EmbeddedSingleNodeKafkaCluster.Builder kafkaCluster
        = EmbeddedSingleNodeKafkaCluster.newBuilder();

    public Builder withKafkaCluster(final EmbeddedSingleNodeKafkaCluster.Builder kafkaCluster) {
      this.kafkaCluster = Objects.requireNonNull(kafkaCluster, "kafkaCluster");
      return this;
    }

    public IntegrationTestHarness build() {
      return new IntegrationTestHarness(kafkaCluster.build(), schemaRegistry);
    }
  }

  public final class ContextBuilder {

    private final Map<String, Object> additionalConfig = new HashMap<>();

    public ContextBuilder withAdditionalConfig(final String name, final Object value) {
      additionalConfig.put(name, value);
      return this;
    }

    public TestKsqlContext build() {
      return new TestKsqlContext(IntegrationTestHarness.this, additionalConfig);
    }
  }

  private final class LazyServiceContext {

    private final SchemaRegistryClient schemaRegistryClient;
    private final AtomicReference<ServiceContext> serviceContext = new AtomicReference<>();

    private LazyServiceContext(final SchemaRegistryClient schemaRegistryClient) {
      this.schemaRegistryClient = Objects
          .requireNonNull(schemaRegistryClient, "schemaRegistryClient");
    }

    private ServiceContext get() {
      if (serviceContext.get() == null) {
        final ServiceContext created = TestServiceContext.create(
            KsqlConfigTestUtil.create(kafkaCluster),
            () -> schemaRegistryClient);

        if (!serviceContext.compareAndSet(null, created)) {
          created.close();
        }
      }

      return serviceContext.get();
    }

    private void close() {
      final ServiceContext toClose = serviceContext.getAndSet(null);
      if (toClose != null) {
        toClose.close();
      }
    }
  }
}
