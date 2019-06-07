package io.confluent.ksql.integration;


import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.delimited.KsqlDelimitedDeserializer;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerializer;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TestDataProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;


public class IntegrationTestHarness {

  public static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;
  public static final long RESULTS_POLL_MAX_TIME_MS = 60000;
  public static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  public static final String CONSUMER_GROUP_ID_PREFIX = "KSQL_Integration_Test_Consumer_";

  public KsqlConfig ksqlConfig;
  private KafkaTopicClientImpl topicClient;

  public Supplier<SchemaRegistryClient> schemaRegistryClientFactory;
  public SchemaRegistryClient schemaRegistryClient;

  private final Map<String, Object> unifiedConfigs = new HashMap<>();

  public IntegrationTestHarness() {
    this.schemaRegistryClient = new MockSchemaRegistryClient();
    this.schemaRegistryClientFactory = () -> this.schemaRegistryClient;
  }

  public KafkaTopicClient topicClient() {
    return topicClient;
  }

  // Topic generation
  public void createTopic(final String topicName) {
    createTopic(topicName, 1, (short) 1);
  }
  public void createTopic(final String topicName, final int numPartitions, final short replicatonFactor) {
    topicClient.createTopic(topicName, numPartitions, replicatonFactor);
  }

  /**
   * Topic topicName will be automatically created if it doesn't exist.
   * @param topicName
   * @param recordsToPublish
   * @param timestamp
   * @return
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public Map<String, RecordMetadata> produceData(final String topicName,
                                                 final Map<String, GenericRow> recordsToPublish,
                                                 final Serializer<GenericRow> serializer,
                                                 final Long timestamp)
          throws InterruptedException, TimeoutException, ExecutionException {

    createTopic(topicName);

    final Properties producerConfig = properties();
    final KafkaProducer<String, GenericRow> producer =
            new KafkaProducer<>(producerConfig, new StringSerializer(), serializer);

    final Map<String, RecordMetadata> result = new HashMap<>();
    for (final Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      final String key = recordEntry.getKey();
      final Future<RecordMetadata> recordMetadataFuture
          = producer.send(buildRecord(topicName, timestamp, recordEntry, key));
      result.put(key,
                 recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }

  private ProducerRecord<String, GenericRow> buildRecord(final String topicName,
                                                         final Long timestamp,
                                                         final Map.Entry<String,
                                                             GenericRow> recordEntry,
                                                         final String key) {
    return new ProducerRecord<>(topicName, null, timestamp,  key, recordEntry.getValue());
  }

  private Properties properties() {
    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                       ksqlConfig.getKsqlStreamConfigProps().get(
                           ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }

  void produceRecord(final String topicName, final String key, final String data) {
    try(final KafkaProducer<String, String> producer
                = new KafkaProducer<>(properties(),
                                      new StringSerializer(),
                                      new StringSerializer())) {
      producer.send(new ProducerRecord<>(topicName, key, data));
    }
  }

  /**
   *
   * @param topic
   * @param schema
   * @param expectedNumMessages
   * @param keyDeserializer
   * @param resultsPollMaxTimeMs
   * @param <K>
   * @return
   */
  public <K> Map<K, GenericRow> consumeData(final String topic,
                                            final Schema schema,
                                            final int expectedNumMessages,
                                            final Deserializer<K> keyDeserializer,
                                            final long resultsPollMaxTimeMs) {

    return consumeData(topic, schema, expectedNumMessages, keyDeserializer, resultsPollMaxTimeMs,
                 DataSource.DataSourceSerDe.JSON);

  }

  public <K> Map<K, GenericRow> consumeData(String topic,
                                            final Schema schema,
                                            final int expectedNumMessages,
                                            final Deserializer<K> keyDeserializer,
                                            final long resultsPollMaxTimeMs,
                                            final DataSource.DataSourceSerDe dataSourceSerDe) {

    topic = topic.toUpperCase();

    final Map<K, GenericRow> result = new HashMap<>();

    final Properties consumerConfig = consumerConfig();

    try (KafkaConsumer<K, GenericRow> consumer
             = new KafkaConsumer<>(consumerConfig,
                                 keyDeserializer,
                                 getDeserializer(schema, dataSourceSerDe))) {

      consumer.subscribe(Collections.singleton(topic));
      final long pollStart = System.currentTimeMillis();
      final long pollEnd = pollStart + resultsPollMaxTimeMs;
      while (System.currentTimeMillis() < pollEnd &&
             continueConsuming(result.size(), expectedNumMessages)) {
        for (final ConsumerRecord<K, GenericRow> record :
            consumer.poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
          if (record.value() != null) {
            result.put(record.key(), record.value());
          }
        }
      }

      for (final ConsumerRecord<K, GenericRow> record : consumer.poll(RESULTS_EXTRA_POLL_TIME_MS)) {
        if (record.value() != null) {
          result.put(record.key(), record.value());
        }
      }
    }
    return result;
  }

  public List<ConsumerRecord> consumerRecords(final String topic,
                                              final int expectedNumMessages,
                                              final long resultsPollMaxTimeMs) {

    final List<ConsumerRecord> results = new ArrayList<>();
    try(final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(),
        new StringDeserializer(),
        new ByteArrayDeserializer())) {
      consumer.subscribe(Collections.singleton(topic.toUpperCase()));
      final long pollStart = System.currentTimeMillis();
      final long pollEnd = pollStart + resultsPollMaxTimeMs;
      while (System.currentTimeMillis() < pollEnd &&
          continueConsuming(results.size(), expectedNumMessages)) {
        for (final ConsumerRecord<String, byte[]> record :
            consumer.poll(Math.max(1, pollEnd - System.currentTimeMillis()))) {
          if (record.value() != null) {
            results.add(record);
          }
        }
      }

      for (final ConsumerRecord<String, byte[]> record : consumer.poll(RESULTS_EXTRA_POLL_TIME_MS)) {
        if (record.value() != null) {
          results.add(record);
        }
      }
    }
    return results;

  }

  public Map<String, Object> allConfigs() {
    return unifiedConfigs;
  }

  private Properties consumerConfig() {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                       ksqlConfig.getKsqlStreamConfigProps().get(
                           ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG,
                       CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return consumerConfig;
  }
  private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
    return maxMessages < 0 || messagesConsumed < maxMessages;
  }

  EmbeddedSingleNodeKafkaCluster embeddedKafkaCluster = null;


  public static class DummyProducerInterceptor implements ProducerInterceptor {

    public void onAcknowledgement(final RecordMetadata rm, final Exception e) {
    }

    public ProducerRecord onSend(final ProducerRecord producerRecords) {
      return producerRecords;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> map) {
    }
  }


  public void start(final Map<String, Object> callerConfigMap) throws Exception {
    embeddedKafkaCluster = new EmbeddedSingleNodeKafkaCluster();
    embeddedKafkaCluster.start();
    final Map<String, Object> configMap = new HashMap<>();

    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaCluster.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    configMap.put("producer.interceptor.classes", DummyProducerInterceptor.class.getName());
    configMap.putAll(callerConfigMap);

    unifiedConfigs.putAll(configMap);

    this.ksqlConfig = new KsqlConfig(configMap);
    this.topicClient = new KafkaTopicClientImpl(ksqlConfig.getKsqlAdminClientConfigProps());
  }

  public void stop() {
    this.topicClient.close();
    this.embeddedKafkaCluster.stop();
  }

  public Map<String, RecordMetadata> publishTestData(final String topicName,
                                                     final TestDataProvider dataProvider,
                                                     final Long timestamp)
      throws InterruptedException, ExecutionException, TimeoutException {

    return publishTestData(topicName, dataProvider, timestamp, DataSource.DataSourceSerDe.JSON);
  }

  public Map<String, RecordMetadata> publishTestData(final String topicName,
                                                     final TestDataProvider dataProvider,
                                                     final Long timestamp,
                                                     final DataSource.DataSourceSerDe dataSourceSerDe)
      throws InterruptedException, ExecutionException, TimeoutException {
    createTopic(topicName);
    return produceData(topicName,
                       dataProvider.data(),
                       getSerializer(dataProvider.schema(),
                                     dataSourceSerDe),
                       timestamp);

  }

  private Serializer getSerializer(final Schema schema, final DataSource.DataSourceSerDe dataSourceSerDe) {
    switch (dataSourceSerDe) {
      case JSON:
        return new KsqlJsonSerializer(schema);
      case AVRO:
        return new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false,
            () -> this.schemaRegistryClient
        ).serializer();
      case DELIMITED:
        return new KsqlDelimitedSerializer(schema);
      default:
        throw new KsqlException("Format not supported: " + dataSourceSerDe);
    }
  }

  private Deserializer<GenericRow> getDeserializer(final Schema schema,
                                                   final DataSource.DataSourceSerDe dataSourceSerDe) {
    switch (dataSourceSerDe) {
      case JSON:
        return new KsqlJsonDeserializer(schema, false);
      case AVRO:
        return new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false,
            () -> this.schemaRegistryClient
        ).deserializer();
      case DELIMITED:
        return new KsqlDelimitedDeserializer(schema);
      default:
        throw new KsqlException("Format not supported: " + dataSourceSerDe);
    }
  }
}
