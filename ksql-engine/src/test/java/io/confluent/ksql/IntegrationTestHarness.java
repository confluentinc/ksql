package io.confluent.ksql;


import io.confluent.ksql.util.TopicConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import org.junit.Assert;

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
   * @return
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws ExecutionException
   */
  public Map<String, RecordMetadata> produceData(String topicName, Map<String, GenericRow> recordsToPublish, Schema schema)
          throws InterruptedException, TimeoutException, ExecutionException {

    createTopic(topicName);

    Properties producerConfig = properties();
    KafkaProducer<String, GenericRow> producer =
            new KafkaProducer<>(producerConfig, new StringSerializer(), new KsqlJsonSerializer(schema));

    Map<String, RecordMetadata> result = new HashMap<>();
    for (Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      String key = recordEntry.getKey();
      ProducerRecord<String, GenericRow>
              producerRecord = new ProducerRecord<>(topicName, key, recordEntry.getValue());
      Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
      result.put(key, recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
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

  public <K> Map<K, GenericRow> readRow(String topic, Deserializer<K> keyDeserializer, Schema schema, int expectedResults) {
    return topicConsumer.readResults(topic, schema, expectedResults, keyDeserializer);
  };


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

  public void assertExpectedResults(Map<String, GenericRow> actualResult,
                                    Map<String, GenericRow> expectedResult) {
    Assert.assertEquals(actualResult.size(), expectedResult.size());

    for (String k: expectedResult.keySet()) {
      Assert.assertTrue(actualResult.containsKey(k));
      Assert.assertEquals(expectedResult.get(k), actualResult.get(k));
    }
  }

  public static void main(String args[]) throws Exception {

    IntegrationTestHarness testHarness = new IntegrationTestHarness();

    testHarness.start();


    OrderDataProvider orderDataProvider = new OrderDataProvider();

    String topicName = "TestTopic";
    testHarness.createTopic(topicName, 1, (short)1);
    testHarness.produceData(topicName, orderDataProvider.data(), orderDataProvider.schema());


    KsqlContext ksqlContext = new KsqlContext(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, "
            + "ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON');");
    ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE ORDERUNITS > 40;");

    Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS",
            orderDataProvider.schema
                    (), 4,
            new StringDeserializer());

    System.out.println();
    testHarness.stop();

  }


}
