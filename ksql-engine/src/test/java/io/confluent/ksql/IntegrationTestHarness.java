package io.confluent.ksql;


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
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
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

public class IntegrationTestHarness {



  public static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 5000;

  public static final long RESULTS_POLL_MAX_TIME_MS = 30000;
  public static final long RESULTS_EXTRA_POLL_TIME_MS = 250;

  public static final String CONSUMER_GROUP_ID_PREFIX = "KSQL_Iintegration_Test_Consumer_";

  final KsqlConfig ksqlConfig;
  final KafkaTopicClientImpl kafkaTopicClient;

  public IntegrationTestHarness(final KsqlConfig ksqlConfig) {
    this.ksqlConfig = ksqlConfig;
    kafkaTopicClient = new KafkaTopicClientImpl(ksqlConfig);
  }


  // Topic generation
  public void createTopic(String topicName) {
    kafkaTopicClient.createTopic(topicName, 1, (short) 1);
  }
  public void createTopic(String topicName, int numPartitions, short replicatonFactor) {
    kafkaTopicClient.createTopic(topicName, numPartitions, replicatonFactor);
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
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
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

  /**
   *
   * @param topic
   * @param schema
   * @param expectedNumMessages
   * @param keyDeserializer
   * @param <K>
   * @return
   */
  public <K> Map<K, GenericRow> consumeData(
      String topic,
      Schema schema,
      int expectedNumMessages,
      Deserializer<K> keyDeserializer
  ) {
    Map<K, GenericRow> result = new HashMap<>();

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ksqlConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID_PREFIX + System.currentTimeMillis());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<K, GenericRow> consumer =
             new KafkaConsumer<>(consumerConfig, keyDeserializer, new KsqlJsonDeserializer(schema))
    ) {
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

  public static void main(String args[])
      throws Exception {
    final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    CLUSTER.start();

    Map<String, Object> configMap = new HashMap<>();

    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");

    KsqlConfig ksqlConfig = new KsqlConfig(configMap);

    IntegrationTestHarness integrationTestHarness = new IntegrationTestHarness(ksqlConfig);
    OrderDataProvider orderDataProvider = new OrderDataProvider();

    String topicName = "TestTopic";
    integrationTestHarness.createTopic(topicName, 1, (short)1);
    integrationTestHarness.produceData(topicName, orderDataProvider.data(), orderDataProvider.schema());


    KsqlContext ksqlContext = new KsqlContext(ksqlConfig.getKsqlStreamConfigProps());
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, "
                    + "ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON');");
    ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE ORDERUNITS > 40;");

    Map<String, GenericRow> results = integrationTestHarness.consumeData("BIGORDERS",
                                                                         orderDataProvider.schema
                                                                             (), 4,
                                                                         new StringDeserializer());

    System.out.println();
    CLUSTER.stop();
  }

}
