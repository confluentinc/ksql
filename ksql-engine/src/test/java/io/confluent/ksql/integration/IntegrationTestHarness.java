package io.confluent.ksql.integration;


import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class IntegrationTestHarness {



  public static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;
  public static final long RESULTS_POLL_MAX_TIME_MS = 10000;
  public static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  public static final String CONSUMER_GROUP_ID_PREFIX = "KSQL_Iintegration_Test_Consumer_";

  public KsqlConfig ksqlConfig;
  KafkaTopicClientImpl topicClient;

  private TopicConsumer topicConsumer;

  public IntegrationTestHarness() {
  }


  // Topic generation
  public void createTopic(String topicName) {
    topicClient.createTopic(topicName, 1, (short) 1);
  }
  public void createTopic(String topicName, int numPartitions, short replicatonFactor) {
    topicClient.createTopic(topicName, numPartitions, replicatonFactor);
  }


  /**
   * Topic topicName will be automatically created if it doesn't exist.
   * @param topicName
   * @param recordsToPublish
   * @param schema
   * @param timestamp
   * @return
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public Map<String, RecordMetadata> produceData(String topicName, Map<String, GenericRow> recordsToPublish, Schema schema, Long timestamp)
          throws InterruptedException, TimeoutException, ExecutionException {

    createTopic(topicName);

    Properties producerConfig = properties();
    KafkaProducer<String, GenericRow> producer =
            new KafkaProducer<>(producerConfig, new StringSerializer(), new KsqlJsonSerializer(schema));

    Map<String, RecordMetadata> result = new HashMap<>();
    for (Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      String key = recordEntry.getKey();
      Future<RecordMetadata> recordMetadataFuture = producer.send(buildRecord(topicName, timestamp, recordEntry, key));
      result.put(key, recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }

  private ProducerRecord<String, GenericRow> buildRecord(String topicName, Long timestamp, Map.Entry<String, GenericRow> recordEntry, String key) {
    ProducerRecord<String, GenericRow> producerRecord;
    if (timestamp == null) {
      producerRecord = new ProducerRecord<>(topicName, key, recordEntry.getValue());
    } else {
      producerRecord = new ProducerRecord<>(topicName, null, timestamp,  key, recordEntry.getValue());
    }
    return producerRecord;
  }

  private Properties properties() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    return producerConfig;
  }

  void produceRecord(final String topicName, final String key, final String jsonData) {
    try(final KafkaProducer<String, String> producer
                = new KafkaProducer<>(properties(), new StringSerializer(), new StringSerializer())) {
      producer.send(new ProducerRecord<>(topicName, key, jsonData));
    }
  }

  /**
   *
   * @param topic
   * @param schema
   * @param expectedNumMessages
   * @param keyDeserializer
   * @param <K>
   * @return
   */
  public <K> Map<K, GenericRow> consumeData(String topic, Schema schema, int expectedNumMessages, Deserializer<K> keyDeserializer) {

    topic = topic.toUpperCase();

    Map<K, GenericRow> result = new HashMap<>();

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<K, GenericRow> consumer = new KafkaConsumer<>(consumerConfig, keyDeserializer, new KsqlJsonDeserializer(schema))) {

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

  EmbeddedSingleNodeKafkaCluster embeddedKafkaCluster = null;



  public void start() throws Exception {
    embeddedKafkaCluster = new EmbeddedSingleNodeKafkaCluster();
    embeddedKafkaCluster.start();
    Map<String, Object> configMap = new HashMap<>();

    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaCluster.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");

    this.ksqlConfig = new KsqlConfig(configMap);
    this.topicClient = new KafkaTopicClientImpl(ksqlConfig.getKsqlAdminClientConfigProps());
    this.topicConsumer = new TopicConsumer(embeddedKafkaCluster);
  }

  public void stop() {
    this.topicClient.close();
    this.embeddedKafkaCluster.stop();
  }

  public Map<String, RecordMetadata> publishTestData(String topicName, TestDataProvider dataProvider, Long timestamp) throws InterruptedException, ExecutionException, TimeoutException {
    createTopic(topicName);
    return produceData(topicName, dataProvider.data(), dataProvider.schema(), timestamp);
  }
}
