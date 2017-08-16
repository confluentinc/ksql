package io.confluent.ksql.integtests.json;

import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.ksql.util.KsqlTestUtil.assertExpectedResults;
import static io.confluent.ksql.util.KsqlTestUtil.assertExpectedWindowedResults;

public class JsonFormatTest {

  private MetaStore metaStore;
  private KsqlEngine ksqlEngine;
  private TopicProducer topicProducer;
  private TopicConsumer topicConsumer;

  private Map<String, GenericRow> inputData;
  private Map<String, RecordMetadata> inputRecordsMetadata;

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";

  private static final Logger log = LoggerFactory.getLogger(JsonFormatTest.class);

  @Before
  public void before() throws Exception {

    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");

    KsqlConfig ksqlConfig = new KsqlConfig(configMap);
    ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(ksqlConfig));
    metaStore = ksqlEngine.getMetaStore();
    topicProducer = new TopicProducer(CLUSTER);
    topicConsumer = new TopicConsumer(CLUSTER);

    createInitTopics();
    produceInitData();
    execInitCreateStreamQueries();

  }

  private void createInitTopics() {
    ksqlEngine.getKafkaTopicClient().createTopic(inputTopic, 1, (short)1);
    ksqlEngine.getKafkaTopicClient().createTopic(messageLogTopic, 1, (short)1);
  }

  private void produceInitData() throws Exception {
    OrderDataProvider orderDataProvider = new OrderDataProvider();
    inputData = orderDataProvider.data();
    inputRecordsMetadata = topicProducer.produceInputData(inputTopic, orderDataProvider.data(), orderDataProvider.schema());

    Schema messageSchema = SchemaBuilder.struct().field("MESSAGE", SchemaBuilder.STRING_SCHEMA).build();

    GenericRow messageRow = new GenericRow(Arrays.asList
        ("{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
            + "\"caasVersion\":\"0.0.2\",\"cloud\":\"aws\",\"clusterId\":\"cp99\",\"clusterName\":\"kafka\",\"cpComponentId\":\"kafka\",\"host\":\"kafka-1-wwl0p\",\"k8sId\":\"k8s13\",\"k8sName\":\"perf\",\"level\":\"ERROR\",\"logger\":\"kafka.server.ReplicaFetcherThread\",\"message\":\"Found invalid messages during fetch for partition [foo512,172] offset 0 error Record is corrupt (stored crc = 1321230880, computed crc = 1139143803)\",\"networkId\":\"vpc-d8c7a9bf\",\"region\":\"us-west-2\",\"serverId\":\"1\",\"skuId\":\"sku5\",\"source\":\"kafka\",\"tenantId\":\"t47\",\"tenantName\":\"perf-test\",\"thread\":\"ReplicaFetcherThread-0-2\",\"zone\":\"us-west-2a\"},\"stream\":\"stdout\",\"time\":2017}"));

    Map<String, GenericRow> records = new HashMap<>();
    records.put("1", messageRow);

    topicProducer.produceInputData(messageLogTopic, records, messageSchema);
  }

  private void execInitCreateStreamQueries() throws Exception {
    String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
        + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
        + "map<varchar, double>) WITH (value_format = 'json', "
        + "kafka_topic='%s' , "
        + "key='ordertime');", inputStream, inputTopic);

    String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (value_format = 'json', "
        + "kafka_topic='%s');", messageLogStream, messageLogTopic);

    ksqlEngine.buildMultipleQueries(false, ordersStreamStr, Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(false, messageStreamStr, Collections.emptyMap());
  }

  @After
  public void after() throws Exception {
    ksqlEngine.close();
  }

  @Test
  public void testSelectStar() throws Exception {
    final String streamName = "SelectStarStream".toUpperCase();
    final String queryString = String.format("CREATE STREAM %s AS SELECT * FROM %s;", streamName, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());
    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, inputData.size());

    Assert.assertEquals(inputData.size(), results.size());
    assertExpectedResults(results, inputData);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectProject() throws Exception {
    final String streamName = "SelectProjectStream".toUpperCase();
    final String queryString =
        String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM %s;", streamName, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

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

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {
    final String streamName = "SelectProjectKeyTimestampStream".toUpperCase();
    final String queryString =
        String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID "
                + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';", streamName,
            inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", inputRecordsMetadata.get("8")
        .timestamp(), "ITEM_8")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testTimestampColumnSelection() throws Exception {
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
            inputStream, stream2Name, stream1Name);

    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(true, query1String, Collections.emptyMap());

    PersistentQueryMetadata query1Metadata = (PersistentQueryMetadata) queryMetadataList.get(0);
    PersistentQueryMetadata query2Metadata = (PersistentQueryMetadata) queryMetadataList.get(1);

    query1Metadata.getKafkaStreams().start();
    query2Metadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(stream2Name).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", inputRecordsMetadata.get("8")
            .timestamp() + 10000, "8", inputRecordsMetadata.get("8").timestamp() + 10000,
        inputRecordsMetadata.get("8").timestamp
            () + 100, "ORDER_6", "ITEM_8")));

    Map<String, GenericRow> results1 = readNormalResults(stream1Name, resultSchema,
        expectedResults.size());

    Map<String, GenericRow> results = readNormalResults(stream2Name, resultSchema,
        expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(query1Metadata.getId(), true);
    ksqlEngine.terminateQuery(query2Metadata.getId(), true);
  }

  @Test
  public void testSelectFilter() throws Exception {
    final String streamName = "SelectFilterStream".toUpperCase();
    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';",
        streamName,
        inputStream
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

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

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectExpression() throws Exception {
    final String streamName = "SelectExpressionStream".toUpperCase();

    final String selectColumns =
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }


  @Test
  public void testCastExpression() throws Exception {
    final String streamName = "CastExpressionStream".toUpperCase();

    final String selectColumns =
        " CAST (ORDERUNITS AS INTEGER), CAST( PRICEARRAY[1]>1000 AS STRING), CAST (SUBSTRING"
            + "(ITEMID, 5) AS DOUBLE), CAST(ORDERUNITS AS VARCHAR) ";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(80, "true", 8.0, "80.0")));


    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectUDFs() throws Exception {
    final String streamName = "SelectUDFsStream".toUpperCase();

    final String selectColumns =
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectUDFLogicalExpression() throws Exception {
    final String streamName = "SelectUDFLogicalExpressionStream".toUpperCase();

    final String selectColumns =
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000";
    final String whereClause = "UCASE(ITEMID) = 'ITEM_8' AND ORDERUNITS > 20";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  //@Test
  public void testSelectDateTimeUDFs() throws Exception {
    final String streamName = "SelectDateTimeUDFsStream".toUpperCase();

    final String selectColumns =
        "(ORDERTIME+1500962514806) , TIMESTAMPTOSTRING(ORDERTIME+1500962514806, "
        + "'yyyy-MM-dd HH:mm:ss.SSS'), "
        + "STRINGTOTIMESTAMP"
            + "(TIMESTAMPTOSTRING"
            + "(ORDERTIME+1500962514806, 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy-MM-dd HH:mm:ss.SSS')";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(1500962514814l,
        "2017-07-24 23:01:54.814",
        1500962514814l)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testAggSelectStar() throws Exception {

    Map<String, RecordMetadata> newRecordsMetadata = topicProducer.produceInputData(inputTopic, inputData, SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(ksqlEngine.getMetaStore().getSource(inputStream).getSchema()));
    final String streamName = "AGGTEST";
    final long windowSizeMilliseconds = 2000;
    final String selectColumns =
        "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10)";
    final String window = String.format("TUMBLING ( SIZE %d MILLISECOND)", windowSizeMilliseconds);
    final String havingClause = "SUM(ORDERUNITS) > 150";

    final String queryString = String.format(
        "CREATE TABLE %s AS SELECT %s FROM %s WINDOW %s WHERE ORDERUNITS > 60 GROUP BY ITEMID "
            + "HAVING %s;",
        streamName,
        selectColumns,
        inputStream,
        window,
        havingClause
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();
    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(ksqlEngine.getMetaStore().getSource(streamName).getSchema());

    long firstItem8Window  = inputRecordsMetadata.get("8").timestamp() / windowSizeMilliseconds;
    long secondItem8Window =   newRecordsMetadata.get("8").timestamp() / windowSizeMilliseconds;

    Map<Windowed<String>, GenericRow> expectedResults = new HashMap<>();
    if (firstItem8Window == secondItem8Window) {
      expectedResults.put(
          new Windowed<>("ITEM_8",new TimeWindow(0, 1)),
          new GenericRow(Arrays.asList("ITEM_8", 2, 160.0, 80.0, 2220.0))
      );
    }

    Map<Windowed<String>, GenericRow> results = readWindowedResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedWindowedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSinkProperties() throws Exception {
    final String streamName = "SinkPropertiesStream".toUpperCase();
    final int resultPartitionCount = 3;
    final String queryString = String.format("CREATE STREAM %s WITH (PARTITIONS = %d) AS SELECT * "
            + "FROM %s;",
        streamName, resultPartitionCount, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    KafkaTopicClient kafkaTopicClient = ksqlEngine.getKafkaTopicClient();

    /*
     * It may take several seconds after AdminClient#createTopics returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, AdminClient#listTopics may not return information about the new topics.
     */
    log.info("Wait for the created topic to appear in the topic list...");
    Thread.sleep(2000);

    Assert.assertTrue(kafkaTopicClient.isTopicExists(streamName));
    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testJsonStreamExtractor() throws Exception {

    final String streamName = "JSONSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
            + "(message, '$.log.cloud') "
            + "FROM %s;",
        streamName, messageLogStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("aws")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  //*********************************************************//

  private Map<String, GenericRow> readNormalResults(String resultTopic, Schema resultSchema, int expectedNumMessages) {
    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, new StringDeserializer());
  }

  private Map<Windowed<String>, GenericRow> readWindowedResults(
      String resultTopic,
      Schema resultSchema,
      int expectedNumMessages
  ) {
    Deserializer<Windowed<String>> keyDeserializer = new WindowedDeserializer<>(new StringDeserializer());
    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, keyDeserializer);
  }

}