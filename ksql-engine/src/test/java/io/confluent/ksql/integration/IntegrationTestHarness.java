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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unchecked")
public class IntegrationTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestHarness.class);

  public static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;
  public static final long RESULTS_POLL_MAX_TIME_MS = 60000;
  public static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  public static final String CONSUMER_GROUP_ID_PREFIX = "KSQL_Integration_Test_Consumer_";

  public KsqlConfig ksqlConfig;
  private KafkaTopicClientImpl topicClient;

  public SchemaRegistryClient schemaRegistryClient;

  private final AtomicInteger consumedCount;
  private final AtomicInteger producedCount;

  private static IntegrationTestHarness THIS;
  private AdminClient adminClient;

  private final Map<String, Object> unifiedConfigs = new HashMap<>();

  public IntegrationTestHarness() {
    this.schemaRegistryClient = new MockSchemaRegistryClient();
    THIS = this;
    consumedCount = new AtomicInteger(0);
    producedCount = new AtomicInteger(0);
  }

  public KafkaTopicClient topicClient() {
    return topicClient;
  }

  // Topic generation
  public void createTopic(final String topicName) {
    if(!topicClient.isTopicExists(topicName)) {
      createTopic(topicName, 1, (short) 1);
    }
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
                                                 final Long timestamp) {

    createTopic(topicName);

    final Properties producerConfig = properties();
    try (KafkaProducer<String, GenericRow> producer =
            new KafkaProducer<>(producerConfig, new StringSerializer(), serializer)) {

      final Map<String, Future<RecordMetadata>> futures = recordsToPublish.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, entry -> {
            final String key = entry.getKey();
            final GenericRow value = entry.getValue();

            LOG.debug("Producing message. topic:{}, key:{}, value:{}, timestamp:{}",
                topicName, key, value, timestamp);

            return producer.send(buildRecord(topicName, timestamp, value, key));
          }));

      return futures.entrySet().stream()
          .collect(Collectors.toMap(Entry::getKey, entry -> {
            try {
              return entry.getValue().get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }));
    }
  }

  private static ProducerRecord<String, GenericRow> buildRecord(
      final String topicName,
      final Long timestamp,
      final GenericRow value,
      final String key) {
    return new ProducerRecord<>(topicName, null, timestamp,  key, value);
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

    final Properties consumerConfig = consumerConfig(
        CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis());

    try (KafkaConsumer<K, GenericRow> consumer
             = new KafkaConsumer<>(consumerConfig,
                                 keyDeserializer,
                                 getDeserializer(schema, dataSourceSerDe))) {

      consumer.subscribe(Collections.singleton(topic));
      final long pollStart = System.currentTimeMillis();
      final long pollEnd = pollStart + resultsPollMaxTimeMs;
      while (System.currentTimeMillis() < pollEnd &&
             continueConsuming(result.size(), expectedNumMessages)) {
        final Duration duration = Duration.ofMillis(Math.max(1, pollEnd - System.currentTimeMillis()));
        for (final ConsumerRecord<K, GenericRow> record : consumer.poll(duration)) {
          if (record.value() != null) {
            LOG.trace("Consumed record. topic:{}, key:{}, value:{}",
                topic, record.key(), record.value());
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
    try(final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(
        CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis()),
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


  // Just so we can test consumer group stuff

  KafkaConsumer<String, byte[]> createSubscribedConsumer(final String topic, final String groupId) {
    final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig(groupId),
        new StringDeserializer(),
        new ByteArrayDeserializer());
      consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }




  public Map<String, Object> allConfigs() {
    return unifiedConfigs;
  }

  private Properties consumerConfig(final String groupId) {
    final Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                       ksqlConfig.getKsqlStreamConfigProps().get(
                           ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG,
        groupId);
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return consumerConfig;
  }

  private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
    return maxMessages < 0 || messagesConsumed < maxMessages;
  }

  EmbeddedSingleNodeKafkaCluster embeddedKafkaCluster = null;

  public static class DummyConsumerInterceptor implements ConsumerInterceptor {

    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      THIS.consumedCount.updateAndGet((current) -> current + consumerRecords.count());
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }


  public static class DummyProducerInterceptor implements ProducerInterceptor {

    public void onAcknowledgement(final RecordMetadata rm, final Exception e) {
    }

    public ProducerRecord onSend(final ProducerRecord producerRecord) {
      THIS.producedCount.incrementAndGet();
      return producerRecord;
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
    configMap.put("consumer.interceptor.classes", DummyConsumerInterceptor.class.getName());
    configMap.putAll(callerConfigMap);

    unifiedConfigs.putAll(configMap);

    this.ksqlConfig = new KsqlConfig(configMap);
    this.adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    this.topicClient = new KafkaTopicClientImpl(
        adminClient);
  }

  public void stop() {
    this.adminClient.close();
    this.embeddedKafkaCluster.stop();
  }

  public Map<String, RecordMetadata> publishTestData(final String topicName,
                                                     final TestDataProvider dataProvider,
                                                     final Long timestamp) {

    return publishTestData(topicName, dataProvider, timestamp, DataSource.DataSourceSerDe.JSON);
  }

  public Map<String, RecordMetadata> publishTestData(final String topicName,
                                                     final TestDataProvider dataProvider,
                                                     final Long timestamp,
                                                     final DataSource.DataSourceSerDe dataSourceSerDe) {
    createTopic(topicName);
    return produceData(topicName,
                       dataProvider.data(),
                       getSerializer(dataProvider.schema(),
                                     dataSourceSerDe),
                       timestamp);

  }

  public int getConsumedCount() {
    return consumedCount.intValue();
  }

  public int getProducedCount() {
    return producedCount.intValue();
  }

  private Serializer getSerializer(final Schema schema, final DataSource.DataSourceSerDe dataSourceSerDe) {
    switch (dataSourceSerDe) {
      case JSON:
        return new KsqlJsonSerializer(schema);
      case AVRO:
        return new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, new KsqlConfig(Collections.emptyMap()), false, this.schemaRegistryClient
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
            schema, new KsqlConfig(Collections.emptyMap()), false, this.schemaRegistryClient
        ).deserializer();
      case DELIMITED:
        return new KsqlDelimitedDeserializer(schema);
      default:
        throw new KsqlException("Format not supported: " + dataSourceSerDe);
    }
  }
}
