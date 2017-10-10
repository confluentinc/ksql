package io.confluent.ksql;

import io.confluent.ksql.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedResults;
import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedWindowedResults;

public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = new KsqlContext(testHarness.ksqlConfig.getKsqlConfigProps());
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }
//  @Test
//  public void testSinkProperties() throws Exception {
//    final String streamName = "SinkPropertiesStream".toUpperCase();
//    final int resultPartitionCount = 3;
//    final String queryString = String.format("CREATE STREAM %s WITH (PARTITIONS = %d) AS SELECT * "
//                    + "FROM %s;",
//            streamName, resultPartitionCount, inputStream);
//
//    PersistentQueryMetadata queryMetadata =
//            (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
//    queryMetadata.getKafkaStreams().start();
//
//    KafkaTopicClient kafkaTopicClient = ksqlEngine.getTopicClient();
//
//    /*
//     * It may take several seconds after AdminClient#createTopics returns
//     * success for all the brokers to become aware that the topics have been created.
//     * During this time, AdminClient#listTopics may not return information about the new topics.
//     */
//    log.info("Wait for the created topic to appear in the topic list...");
//    Thread.sleep(2000);
//
//    Assert.assertTrue(kafkaTopicClient.isTopicExists(streamName));
//    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
//  }

  @Test
  public void testAggSelectStar() throws Exception {

    OrderDataProvider orderDataProvider = publishOrdersTopicData();
    createOrdersStream();

    TopicProducer topicProducer = new TopicProducer(testHarness.embeddedKafkaCluster);

    Map<String, RecordMetadata> newRecordsMetadata = topicProducer.produceInputData("ORDERS", orderDataProvider.data(), orderDataProvider.schema());


    final String streamName = "AGGTEST";
    final long windowSizeMilliseconds = 2000;

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM %s WINDOW %s WHERE ORDERUNITS > 60 GROUP BY ITEMID "
                    + "HAVING %s;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10)",
            "ORDERS",
            String.format("TUMBLING ( SIZE %d MILLISECOND)", windowSizeMilliseconds),
            "SUM(ORDERUNITS) > 150"
    );


    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    Map<Windowed<String>, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put(
            new Windowed<>("ITEM_8",new TimeWindow(0, 1)),
            new GenericRow(Arrays.asList("ITEM_8", 2, 160.0, 80.0, 2220.0))
    );

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema, expectedResults.size(), new WindowedDeserializer<>(new StringDeserializer()));


    System.out.println("Results:" + results + " size:" + results.size());

// MINE
//[2017-10-10 16:33:00,065] INFO Building AST for CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');. (io.confluent.ksql.KsqlEngine:215)
//[2017-10-10 16:33:00,088] INFO Build logical plan for CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');. (io.confluent.ksql.QueryEngine:114)
//>>[2017-10-10 16:33:00,104] WARN Error while fetching metadata with correlation id 1 : {ORDERS=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient:846)
//[2017-10-10 16:33:00,263] INFO Building AST for CREATE TABLE AGGTEST AS SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10) FROM ORDERS WINDOW TUMBLING ( SIZE 2000 MILLISECOND) WHERE ORDERUNITS > 60 GROUP BY ITEMID HAVING SUM(ORDERUNITS) > 150;. (io.confluent.ksql.KsqlEngine:215)
//[2017-10-10 16:33:00,289] INFO Build logical plan for CREATE TABLE AGGTEST AS SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10) FROM ORDERS WINDOW TUMBLING ( SIZE 2000 MILLISECOND) WHERE ORDERUNITS > 60 GROUP BY ITEMID HAVING SUM(ORDERUNITS) > 150;. (io.confluent.ksql.QueryEngine:114)

// WORKS JsonFormatTest.testAggSelectStar
//[2017-10-10 16:21:29,109] INFO Building AST for CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (value_format = 'json', kafka_topic='orders_topic' , key='ordertime');. (io.confluent.ksql.KsqlEngine:215)
//[2017-10-10 16:21:29,128] INFO Build logical plan for CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (value_format = 'json', kafka_topic='orders_topic' , key='ordertime');. (io.confluent.ksql.QueryEngine:114)
//[2017-10-10 16:21:29,130] INFO Building AST for CREATE STREAM message_log (message varchar) WITH (value_format = 'json', kafka_topic='log_topic');. (io.confluent.ksql.KsqlEngine:215)
//[2017-10-10 16:21:29,133] INFO Build logical plan for CREATE STREAM message_log (message varchar) WITH (value_format = 'json', kafka_topic='log_topic');. (io.confluent.ksql.QueryEngine:114)
//[2017-10-10 16:21:29,208] INFO Building AST for CREATE TABLE AGGTEST AS SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10) FROM ORDERS WINDOW TUMBLING ( SIZE 2000 MILLISECOND) WHERE ORDERUNITS > 60 GROUP BY ITEMID HAVING SUM(ORDERUNITS) > 150;. (io.confluent.ksql.KsqlEngine:215)
//[2017-10-10 16:21:29,232] INFO Build logical plan for CREATE TABLE AGGTEST AS SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10) FROM ORDERS WINDOW TUMBLING ( SIZE 2000 MILLISECOND) WHERE ORDERUNITS > 60 GROUP BY ITEMID HAVING SUM(ORDERUNITS) > 150;. (io.confluent.ksql.QueryEngine:114)

    Assert.assertEquals(1, results.size());
    assertExpectedWindowedResults(results, expectedResults);
  }

  @Test
  public void testTimestampColumnSelection() throws Exception {

    publishOrdersTopicData();

    createOrdersStream();

    final String stream1Name = "ORIGINALSTREAM";
    final String stream2Name = "TIMESTAMPSTREAM";
    final String query1String =
            String.format("CREATE STREAM %s WITH (timestamp='RTIME') AS SELECT ROWKEY AS RKEY, "
                            + "ROWTIME+10000 AS "
                            + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                            + "CREATE STREAM %s AS SELECT ROWKEY AS NEWRKEY, "
                            + "ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
                            + "FROM %s ;", stream1Name,
                    "ORDERS", stream2Name, stream1Name);


    ksqlContext.sql(query1String);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", recordMetadataMap.get("8")
                    .timestamp() + 10000, "8", recordMetadataMap.get("8").timestamp() + 10000,
            recordMetadataMap.get("8").timestamp
                    () + 100, "ORDER_8", "ITEM_8")));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    Map<String, GenericRow> results1 = testHarness.consumeData(stream1Name, resultSchema , expectedResults.size(), new StringDeserializer());
    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema , expectedResults.size(), new StringDeserializer());

    System.out.println("Results:" + results1);

    Assert.assertEquals(expectedResults.size(), results2.size());
    assertExpectedResults(expectedResults, results2);
  }


  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();

    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_KEY_TIMESTAMP AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID FROM ORDERS WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_KEY_TIMESTAMP").getSchema();

    Map<String, GenericRow> results = testHarness.consumeData("PROJECT_KEY_TIMESTAMP", resultSchema , dataProvider.data().size(), new StringDeserializer());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", recordMetadataMap.get("8").timestamp(), "ITEM_8")));

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testSelectProject() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();
    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM ORDERS;");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM", resultSchema, dataProvider.data().size(), new StringDeserializer());

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectStar() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();
    createOrdersStream();
    ksqlContext.sql("CREATE STREAM EASYORDERS AS SELECT * FROM orders;");

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("EASYORDERS", dataProvider.schema(), dataProvider.data().size(), new StringDeserializer());

    testHarness.assertExpectedResults(dataProvider.data(), easyOrdersData);
  }


  @Test
  public void testSelectWithFilter() throws Exception {

    OrderDataProvider orderDataProvider = publishOrdersTopicData();

    createOrdersStream();

    ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE ORDERUNITS > 40;");

    Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS", orderDataProvider.schema(), 4, new StringDeserializer());

    Assert.assertEquals(4, results.size());
  }


  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }

  private OrderDataProvider publishOrdersTopicData() throws InterruptedException, TimeoutException, ExecutionException {
    OrderDataProvider dataProvider = new OrderDataProvider();

    String topicName = "TestTopic";
    testHarness.createTopic(topicName);
    recordMetadataMap = testHarness.produceData(topicName, dataProvider.data(), dataProvider.schema());
    return dataProvider;
  }
}
