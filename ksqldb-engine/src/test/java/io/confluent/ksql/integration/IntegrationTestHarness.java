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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.util.ConsumerTestUtil;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.TestDataProvider;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matcher;
import org.junit.rules.ExternalResource;

@SuppressWarnings("WeakerAccess")
public final class IntegrationTestHarness extends ExternalResource {

  private static final int DEFAULT_PARTITION_COUNT = 1;
  private static final short DEFAULT_REPLICATION_FACTOR = (short) 1;
  private static final Supplier<Long> DEFAULT_TS_SUPPLIER = () -> null;

  private final LazyServiceContext serviceContext;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;

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
   * Deletes internal topics for the given application.
   */
  public void deleteInternalTopics(String applicationId) {
    final KafkaTopicClient topicClient = serviceContext.get().getTopicClient();
    topicClient.deleteInternalTopics(applicationId);
  }

  /**
   * Produce a single record to a Kafka topic.
   *
   * @param topicName the topic to produce the record to.
   * @param key the String key of the record.
   * @param data the String value of the record.
   */
  public void produceRecord(final String topicName, final String key, final String data) {
    kafkaCluster.produceRows(
        topicName,
        Collections.singletonMap(key, data).entrySet(),
        new StringSerializer(),
        new StringSerializer(),
        DEFAULT_TS_SUPPLIER
    );
  }

  /**
   * Publish test data to the supplied {@code topic}.
   *
   * @param topic the name of the topic to produce to.
   * @param dataProvider the provider of the test data.
   * @param valueFormat the format values should be produced as.
   * @return the map of produced rows
   */
  public <K> Multimap<K, RecordMetadata> produceRows(
      final String topic,
      final TestDataProvider<K> dataProvider,
      final Format valueFormat
  ) {
    return produceRows(
        topic,
        dataProvider,
        valueFormat,
        DEFAULT_TS_SUPPLIER
    );
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
  public <K> Multimap<K, RecordMetadata> produceRows(
      final String topic,
      final TestDataProvider<K> dataProvider,
      final Format valueFormat,
      final Supplier<Long> timestampSupplier
  ) {
    return produceRows(
        topic,
        dataProvider.data().entries(),
        getKeySerializer(dataProvider.schema()),
        getValueSerializer(valueFormat, dataProvider.schema()),
        timestampSupplier
    );
  }

  /**
   * Produce data to a topic
   *
   * @param topic the name of the topic to produce to.
   * @param rowsToPublish the rows to publish
   * @param schema the schema of the rows
   * @param valueFormat the format values should be produced as.
   * @return the map of produced rows
   */
  public <K> Multimap<K, RecordMetadata> produceRows(
      final String topic,
      final Collection<Entry<K, GenericRow>> rowsToPublish,
      final PhysicalSchema schema,
      final Format valueFormat
  ) {
    return produceRows(
        topic,
        rowsToPublish,
        getKeySerializer(schema),
        getValueSerializer(valueFormat, schema),
        DEFAULT_TS_SUPPLIER
    );
  }

  /**
   * Publish test data to the supplied {@code topic}.
   *
   * @param topic the name of the topic to produce to.
   * @param recordsToPublish the records to produce.
   * @param keySerializer the serializer to use to serialize keys.
   * @param valueSerializer the serializer to use to serialize values.
   * @param timestampSupplier supplier of timestamps.
   * @return the map of produced rows, with an iteration order that matches produce order.
   */
  public <K> Multimap<K, RecordMetadata> produceRows(
      final String topic,
      final Collection<Entry<K, GenericRow>> recordsToPublish,
      final Serializer<K> keySerializer,
      final Serializer<GenericRow> valueSerializer,
      final Supplier<Long> timestampSupplier
  ) {
    return kafkaCluster.produceRows(
        topic,
        recordsToPublish,
        keySerializer,
        valueSerializer,
        timestampSupplier
    );
  }

  /**
   * Verify there are {@code expectedCount} records available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<byte[], byte[]>> verifyAvailableRecords(
      final String topic,
      final int expectedCount
  ) {
    return verifyAvailableRecords(topic, is(expectedCount));
  }

  /**
   * Verify there are {@code expectedCount} records available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<byte[], byte[]>> verifyAvailableRecords(
      final String topic,
      final Matcher<Integer> expectedCount
  ) {
    return kafkaCluster.verifyAvailableRecords(
        topic,
        hasSize(expectedCount),
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
    );
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
  public <K> List<ConsumerRecord<K, GenericRow>> verifyAvailableRows(
      final String topic,
      final int expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    return verifyAvailableRows(topic, hasSize(expectedCount), valueFormat, schema);
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
  public <K> List<ConsumerRecord<K, GenericRow>> verifyAvailableRows(
      final String topic,
      final Matcher<? super List<ConsumerRecord<K, GenericRow>>> expected,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    final Deserializer<K> keyDeserializer = getKeyDeserializer(schema);
    return verifyAvailableRows(topic, expected, valueFormat, schema, keyDeserializer);
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
    final Deserializer<GenericRow> valueDeserializer = getValueDeserializer(valueFormat, schema);

    return kafkaCluster.verifyAvailableRecords(
        topic,
        expected,
        keyDeserializer,
        valueDeserializer,
        timeout
    );
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
  public <K> Map<K, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final int expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    return verifyAvailableNumUniqueRows(topic, is(expectedCount), valueFormat, schema);
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
  public <K> Map<K, GenericRow> verifyAvailableNumUniqueRows(
      final String topic,
      final Matcher<Integer> expectedCount,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    final Deserializer<K> keyDeserializer = getKeyDeserializer(schema);
    final Deserializer<GenericRow> valueDeserializer = getValueDeserializer(valueFormat, schema);

    return verifyAvailableUniqueRows(
        topic,
        mapHasSize(expectedCount),
        keyDeserializer,
        valueDeserializer
    );
  }

  /**
   * Verify there are {@code expectedCount} unique rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expected the expected records.
   * @param valueFormat the format of the value.
   * @param schema the schema of the value.
   * @return the list of consumed records.
   */
  public <K> Map<K, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final Matcher<Map<? extends K, ? extends GenericRow>> expected,
      final Format valueFormat,
      final PhysicalSchema schema
  ) {
    final Deserializer<K> keyDeserializer = getKeyDeserializer(schema);
    final Deserializer<GenericRow> valueDeserializer = getValueDeserializer(valueFormat, schema);

    return verifyAvailableUniqueRows(
        topic,
        expected,
        keyDeserializer,
        valueDeserializer
    );
  }

  /**
   * Verify there are {@code expected} unique rows available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expected the expected records.
   * @param keyDeserializer the keyDeserilizer to use.
   * @param valueDeserializer the valueDeserializer of use.
   * @return the list of consumed records.
   */
  public <K> Map<K, GenericRow> verifyAvailableUniqueRows(
      final String topic,
      final Matcher<Map<? extends K, ? extends GenericRow>> expected,
      final Deserializer<K> keyDeserializer,
      final Deserializer<GenericRow> valueDeserializer
  ) {
    try (KafkaConsumer<K, GenericRow> consumer = new KafkaConsumer<>(
        kafkaCluster.consumerConfig(),
        keyDeserializer,
        valueDeserializer
    )) {
      consumer.subscribe(Collections.singleton(topic));

      final List<ConsumerRecord<K, GenericRow>> consumerRecords = ConsumerTestUtil
          .verifyAvailableRecords(consumer, hasUniqueRecords(expected));

      return toUniqueRecords(consumerRecords);
    }
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
    final Deserializer<GenericRow> valueDeserializer = getValueDeserializer(valueFormat, schema);

    try (KafkaConsumer<K, GenericRow> consumer = new KafkaConsumer<>(
        kafkaCluster.consumerConfig(),
        keyDeserializer,
        valueDeserializer
    )) {
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
          } catch (final Exception e) {
            throw new RuntimeException("could not get subjects");
          }
        },
        30_000,
        "topics not all present after 30 seconds. topics: " + Arrays.toString(topicNames));
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
          } catch (final Exception e) {
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
          } catch (final Exception e) {
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <K> Serializer<K> getKeySerializer(final PhysicalSchema schema) {
    return (Serializer) KafkaSerdeFactory
        .getPrimitiveSerde(schema.keySchema().ksqlSchema())
        .serializer();
  }

  private Serializer<GenericRow> getValueSerializer(
      final Format format,
      final PhysicalSchema schema
  ) {
    return GenericRowSerDe.from(
        FormatInfo.of(format.name()),
        schema.valueSchema(),
        new KsqlConfig(Collections.emptyMap()),
        serviceContext.get().getSchemaRegistryClientFactory(),
        "producer",
        ProcessingLogContext.create()
    ).serializer();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <K> Deserializer<K> getKeyDeserializer(
      final PhysicalSchema schema
  ) {
    return (Deserializer) KafkaSerdeFactory
        .getPrimitiveSerde(schema.keySchema().ksqlSchema())
        .deserializer();
  }

  private Deserializer<GenericRow> getValueDeserializer(
      final Format format,
      final PhysicalSchema schema
  ) {
    return GenericRowSerDe.from(
        FormatInfo.of(format.name()),
        schema.valueSchema(),
        new KsqlConfig(Collections.emptyMap()),
        serviceContext.get().getSchemaRegistryClientFactory(),
        "consumer",
        ProcessingLogContext.create()
    ).deserializer();
  }

  public void ensureSchema(
      final String topicName,
      final PhysicalSchema schema) {
    final SchemaRegistryClient srClient = serviceContext.get().getSchemaRegistryClient();
    try {
      final ParsedSchema parsedSchema = new AvroFormat().toParsedSchema(
          schema.logicalSchema().value(),
          schema.serdeOptions(),
          FormatInfo.of(
              AvroFormat.NAME,
              ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "test_" + topicName)
          )
      );

      srClient.register(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, parsedSchema);
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
