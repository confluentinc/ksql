package io.confluent.kql.integtests.json;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.metastore.MetaStoreImpl;
import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.json.KQLJsonPOJODeserializer;
import io.confluent.kql.serde.json.KQLJsonPOJOSerializer;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kql.util.QueryMetadata;

/**
 * Created by hojjat on 5/3/17.
 */
public class JSONFormatTest {

  MetaStore metaStore;
  KQLEngine kqlEngine;
  Map<String, GenericRow> inputData;

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "orders_topic";

  @Before
  public void before() throws IOException, ExecutionException, InterruptedException {
    metaStore = new MetaStoreImpl();
    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
        .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
        .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
        .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA)
        .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
        .field("KEYVALUEMAP", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.FLOAT64_SCHEMA));

    KQLTopic
        kqlTopicOrders =
        new KQLTopic("ORDERS_TOPIC", "orders_topic", new KQLJsonTopicSerDe(null));

    KQLStream
        kqlStreamOrders = new KQLStream("ORDERS", schemaBuilderOrders, schemaBuilderOrders.field("ORDERTIME"),
                                        kqlTopicOrders);

    metaStore.putTopic(kqlTopicOrders);
    metaStore.putSource(kqlStreamOrders);
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    kqlEngine = new KQLEngine(metaStore, configMap);
    inputData = getInputData();
    produceInputData(inputData, kqlStreamOrders.getSchema());
  }


  @Test
  public void testSelectStar() throws Exception {
    kqlEngine.runMultipleQueries(true, "CREATE STREAM STARTSTREAM AS SELECT * FROM ORDERS;");
    Thread.sleep(1000);
    Schema resultSchema = metaStore.getSource("STARTSTREAM").getSchema();
    Map<String, GenericRow> results = readResults("STARTSTREAM", resultSchema);
    Assert.assertEquals(inputData.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, inputData));
    terminateAllQueries();
  }


  @Test
  public void testSelectProject() throws Exception {
    kqlEngine.runMultipleQueries(true, "CREATE STREAM STARTSTREAM AS SELECT ITEMID, ORDERUNITS, PRICEARRAY"
                                       + " FROM "
                                       + "ORDERS;");
    Thread.sleep(5000);
    SchemaBuilder resultSchema = SchemaBuilder.struct()
        .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA)
        .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA));

    Map<String, GenericRow> results = readResults("STARTSTREAM", resultSchema);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("ITEM_1", 10.0, new
        Double[]{100.0,
                 110.99,
                 90.0 })));
    expectedResults.put("2", new GenericRow(Arrays.asList("ITEM_2", 20.0, new
        Double[]{10.0,
                 10.99,
                 9.0 })));

    expectedResults.put("3", new GenericRow(Arrays.asList("ITEM_3", 30.0, new
        Double[]{10.0,
                 10.99,
                 91.0 })));

    expectedResults.put("4", new GenericRow(Arrays.asList("ITEM_4", 40.0, new
        Double[]{10.0,
                 140.99,
                 94.0 })));

    expectedResults.put("5", new GenericRow(Arrays.asList("ITEM_5", 50.0, new
        Double[]{160.0,
                 160.99,
                 98.0 })));

    expectedResults.put("6", new GenericRow(Arrays.asList("ITEM_6", 60.0, new
        Double[]{1000.0,
                 1100.99,
                 900.0 })));

    expectedResults.put("7", new GenericRow(Arrays.asList("ITEM_7", 70.0, new
        Double[]{1100.0,
                 1110.99,
                 190.0 })));

    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 80.0, new
        Double[]{1100.0,
                 1110.99,
                 970.0 })));

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));
    terminateAllQueries();
  }


  @Test
  public void testSelectFilter() throws Exception {
    kqlEngine.runMultipleQueries(true, "CREATE STREAM FILTERSTREAM AS SELECT * FROM ORDERS "
                                       + "WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';");
    Thread.sleep(1000);
    Schema resultSchema = metaStore.getSource("FILTERSTREAM").getSchema();
    Map<String, GenericRow> results = readResults("FILTERSTREAM", resultSchema);
    Map<String, GenericRow> expectedResults = new HashMap<>();
    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);
    expectedResults.put("8", new GenericRow(Arrays.asList(8, "ORDER_6",
                                                         "ITEM_8", 80.0, new
                                                             Double[]{1100.0,
                                                                      1110.99,
                                                                      970.0 },
                                                         mapField)));

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));
    terminateAllQueries();
  }

  @Test
  public void testSelectExpression() throws Exception {
    kqlEngine.runMultipleQueries(true, "CREATE STREAM FILTERSTREAM AS SELECT ITEMID, "
                                       + "ORDERUNITS*10, PRICEARRAY[0]+10, "
                                       + "KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, "
                                       + "PRICEARRAY[1]>1000 "
                                       + "FROM "
                                       + "ORDERS "
                                       + "WHERE ORDERUNITS > 20 AND ITEMID LIKE '%_8';");
    Thread.sleep(1000);
    Schema resultSchema = metaStore.getSource("FILTERSTREAM").getSchema();
    Map<String, GenericRow> results = readResults("FILTERSTREAM", resultSchema);
    Map<String, GenericRow> expectedResults = new HashMap<>();
    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));
    terminateAllQueries();
  }

  @Test
  public void testSelectUDFs() throws Exception {
    kqlEngine.runMultipleQueries(true, "CREATE STREAM UDFSTREAM AS SELECT ITEMID, "
                                       + "ORDERUNITS*10, PRICEARRAY[0]+10, "
                                       + "KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, "
                                       + "PRICEARRAY[1]>1000 "
                                       + "FROM "
                                       + "ORDERS "
                                       + "WHERE ORDERUNITS > 20 AND ITEMID LIKE '%_8';");
    Thread.sleep(1000);
    Schema resultSchema = metaStore.getSource("UDFSTREAM").getSchema();
    Map<String, GenericRow> results = readResults("UDFSTREAM", resultSchema);
    Map<String, GenericRow> expectedResults = new HashMap<>();
    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));
    terminateAllQueries();
  }

  //*********************************************************//


  private void produceInputData(Map<String, GenericRow> recordsToPublish, Schema
      schema) {

    Properties producerConfig = new Properties();
    producerConfig
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);

    KafkaProducer<String, GenericRow>
        producer = new KafkaProducer<String, GenericRow>(producerConfig, new StringSerializer(), new
        KQLJsonPOJOSerializer(schema));
    for (String key: recordsToPublish.keySet()) {
      GenericRow row = recordsToPublish.get(key);
      ProducerRecord
          producerRecord = new ProducerRecord(inputTopic, key, row);
//      System.out.println(key + " : " + row);
      producer.send(producerRecord);
    }

    producer.close();
  }

  private Map<String, GenericRow> readResults(String resultTopic, Schema resultSchema) {
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG,
                       "filter-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, GenericRow> consumer = new KafkaConsumer<>(consumerConfig, new
        StringDeserializer(), new KQLJsonPOJODeserializer(resultSchema));

    consumer.subscribe(Collections.singletonList(resultTopic));
    int pollIntervalMs = 500;
    int maxTotalPollTimeMs = 20000;
    int totalPollTimeMs = 0;
    Map<String, GenericRow> consumedValues = new HashMap<>();
    while (totalPollTimeMs < maxTotalPollTimeMs) {
      totalPollTimeMs += pollIntervalMs;
      ConsumerRecords<String, GenericRow> records = consumer.poll(pollIntervalMs);
      for (ConsumerRecord<String, GenericRow> record : records) {
        consumedValues.put(record.key(), record.value());
      }
    }
    consumer.close();
    return consumedValues;

  }

  private Map<String, GenericRow> getInputData() {

    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);

    Map<String, GenericRow> dataMap = new HashMap<>();
    dataMap.put("1", new GenericRow(Arrays.asList(1,
                                                  "ORDER_1",
                                                  "ITEM_1", 10.0, new
                                                      Double[]{100.0,
                                                               110.99,
                                                               90.0 },
                                                  mapField)));
    dataMap.put("2", new GenericRow(Arrays.asList(2, "ORDER_2",
                                                  "ITEM_2", 20.0, new
                                                      Double[]{10.0,
                                                               10.99,
                                                               9.0 },
                                                  mapField)));

    dataMap.put("3", new GenericRow(Arrays.asList(3, "ORDER_3",
                                                  "ITEM_3", 30.0, new
                                                      Double[]{10.0,
                                                               10.99,
                                                               91.0 },
                                                  mapField)));

    dataMap.put("4", new GenericRow(Arrays.asList(4, "ORDER_4",
                                                  "ITEM_4", 40.0, new
                                                      Double[]{10.0,
                                                               140.99,
                                                               94.0 },
                                                  mapField)));

    dataMap.put("5", new GenericRow(Arrays.asList(5, "ORDER_5",
                                                  "ITEM_5", 50.0, new
                                                      Double[]{160.0,
                                                               160.99,
                                                               98.0 },
                                                  mapField)));

    dataMap.put("6", new GenericRow(Arrays.asList(6, "ORDER_6",
                                                  "ITEM_6", 60.0, new
                                                      Double[]{1000.0,
                                                               1100.99,
                                                               900.0 },
                                                  mapField)));

    dataMap.put("7", new GenericRow(Arrays.asList(7, "ORDER_6",
                                                  "ITEM_7", 70.0, new
                                                      Double[]{1100.0,
                                                               1110.99,
                                                               190.0 },
                                                  mapField)));

    dataMap.put("8", new GenericRow(Arrays.asList(8, "ORDER_6",
                                                  "ITEM_8", 80.0, new
                                                      Double[]{1100.0,
                                                               1110.99,
                                                               970.0 },
                                                  mapField)));

    return dataMap;
  }

  private boolean assertExpectedResults(Map<String, GenericRow> actualResult,
                                        Map<String, GenericRow> expectedResult) {
    if (actualResult.size() != expectedResult.size()) {
      return false;
    }
    for (String k: expectedResult.keySet()) {
      if (!actualResult.containsKey(k)) {
        return false;
      }
      if (!expectedResult.get(k).hasTheSameContent(actualResult.get(k))) {
        return false;
      }
    }
    return true;
  }

  private void terminateAllQueries() {
    Map<String, QueryMetadata> liveQueries = kqlEngine.getLiveQueries();
    for (String queryId: liveQueries.keySet()) {
      kqlEngine.terminateQuery(queryId);
    }
  }

}
