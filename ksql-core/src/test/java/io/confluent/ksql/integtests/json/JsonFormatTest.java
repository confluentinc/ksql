package io.confluent.ksql.integtests.json;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonPojoDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonPojoSerializer;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.processor.internals.StreamsKafkaClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JsonFormatTest {

  MetaStore metaStore;
  KsqlEngine ksqlEngine;
  Map<String, GenericRow> inputData;
  Map<String, RecordMetadata> inputRecordsMetadata;
  Map<String, Object> configMap;

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final long TEST_RECORD_FUTURE_TIMEOUT_MS = 1000;
  private static final long RESULTS_POLL_MAX_TIME_MS = 30000;
  private static final long RESULTS_EXTRA_POLL_TIME_MS = 250;
  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";

  @Before
  public void before() throws Exception {
    metaStore = new MetaStoreImpl();

    SchemaBuilder schemaBuilderOrders = SchemaBuilder.struct()
        .field("ORDERTIME", SchemaBuilder.INT64_SCHEMA)
        .field("ORDERID", SchemaBuilder.STRING_SCHEMA)
        .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA)
        .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
        .field("KEYVALUEMAP", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.FLOAT64_SCHEMA));

    SchemaBuilder schemaBuilderMessage = SchemaBuilder.struct()
        .field("MESSAGE", SchemaBuilder.STRING_SCHEMA);

    configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    ksqlEngine = new KsqlEngine(metaStore, configMap);
    inputData = getInputData();

    String ordersTopicStr = String.format("CREATE TOPIC %s WITH (format = 'json', "
                          + "kafka_topic='%s');", inputTopic, inputTopic);
    String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                    + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                             + "map<varchar, double>) WITH (topicname = '%s' , "
                                           + "key='ordertime');", inputStream, inputTopic);

    String messageTopicStr = String.format("CREATE TOPIC %s WITH (format = 'json', "
                                           + "kafka_topic='%s');", messageLogTopic,
                                           messageLogTopic);
    String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (topicname = "
                                            + "'%s');", messageLogStream, messageLogTopic);

    ksqlEngine.buildMultipleQueries(false, ordersTopicStr);
    ksqlEngine.buildMultipleQueries(false, ordersStreamStr);

    ksqlEngine.buildMultipleQueries(false, messageTopicStr);
    ksqlEngine.buildMultipleQueries(false, messageStreamStr);

    inputRecordsMetadata = produceInputData(inputData, schemaBuilderOrders.build());
    produceMessageData(schemaBuilderMessage.build());

  }


  @Test
  public void testSelectStar() throws Exception {
    final String streamName = "STARSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT * FROM %s;", streamName, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());
    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, inputData.size());

    Assert.assertEquals(inputData.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, inputData));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }




  @Test
  public void testSelectProject() throws Exception {
    final String streamName = "STARTSTREAM";
    final String queryString =
        String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM %s;", streamName, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
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
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {
    final String streamName = "STARTSTREAM";
    final String queryString =
        String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID "
                      + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';", streamName,
                      inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", inputRecordsMetadata.get("8")
        .timestamp(), "ITEM_8")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

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

    List<QueryMetadata> queryMetadataList = ksqlEngine.buildMultipleQueries(true, query1String);

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
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(query1Metadata.getId(), true);
    ksqlEngine.terminateQuery(query2Metadata.getId(), true);
  }

  @Test
  public void testSelectFilter() throws Exception {
    final String streamName = "FILTERSTREAM";
    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';",
        streamName,
        inputStream
    );

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
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
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectExpression() throws Exception {
    final String streamName = "FILTERSTREAM";

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
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testSelectUDFs() throws Exception {
    final String streamName = "UDFSTREAM";

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
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  @Test
  public void testAggSelectStar() throws Exception {

    Map<String, RecordMetadata> newRecordsMetadata = produceInputData(inputData, SchemaUtil
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
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
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
    Assert.assertTrue(assertExpectedWindowedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }


  @Test
  public void testSinkProperties() throws Exception {
    final String streamName = "STARSTREAM";
    final int resultPartitionCount = 3;
    final String queryString = String.format("CREATE STREAM %s WITH (PARTITIONS = %d) AS SELECT * "
                                             + "FROM %s;",
                                             streamName, resultPartitionCount, inputStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    StreamsKafkaClient streamsKafkaClient = new StreamsKafkaClient(new StreamsConfig(configMap));
    final MetadataResponse metadata = streamsKafkaClient.fetchMetadata();
    final Collection<MetadataResponse.TopicMetadata> topicsMetadata = metadata.topicMetadata();

    boolean topicExists = false;
    for (MetadataResponse.TopicMetadata topicMetadata: topicsMetadata) {
      if (topicMetadata.topic().equalsIgnoreCase(streamName)) {
        topicExists = true;
        Assert.assertTrue(topicMetadata.partitionMetadata().size() == resultPartitionCount);
      }
    }
    Assert.assertTrue(topicExists);
    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }


  @Test
  public void testJsonStreamExtractor() throws Exception {

    final String streamName = "JSONSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT GETSTREAMFROMJSON"
                                             + "(message, '$.log.cloud') "
                                             + "FROM %s;",
                                             streamName, messageLogStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    queryMetadata.getKafkaStreams().start();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("aws")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    Assert.assertTrue(assertExpectedResults(results, expectedResults));

    ksqlEngine.terminateQuery(queryMetadata.getId(), true);
  }

  //*********************************************************//


  private Map<String, RecordMetadata> produceInputData(Map<String, GenericRow> recordsToPublish, Schema schema)
      throws InterruptedException, TimeoutException, ExecutionException {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);

    KafkaProducer<String, GenericRow> producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), new KsqlJsonPojoSerializer(schema));

    Map<String, RecordMetadata> result = new HashMap<>();
    for (Map.Entry<String, GenericRow> recordEntry : recordsToPublish.entrySet()) {
      String key = recordEntry.getKey();
      ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(inputTopic, key, recordEntry.getValue());
      Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
      result.put(key, recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }
    producer.close();

    return result;
  }

  private Map<String, RecordMetadata> produceMessageData(Schema schema)
      throws InterruptedException, ExecutionException, TimeoutException {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);

    KafkaProducer<String, GenericRow> producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), new KsqlJsonPojoSerializer(schema));

    Map<String, RecordMetadata> result = new HashMap<>();

    GenericRow messageRow = new GenericRow(Arrays.asList
        ("{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
         + "\"caasVersion\":\"0.0.2\",\"cloud\":\"aws\",\"clusterId\":\"cp99\",\"clusterName\":\"kafka\",\"cpComponentId\":\"kafka\",\"host\":\"kafka-1-wwl0p\",\"k8sId\":\"k8s13\",\"k8sName\":\"perf\",\"level\":\"ERROR\",\"logger\":\"kafka.server.ReplicaFetcherThread\",\"message\":\"Found invalid messages during fetch for partition [foo512,172] offset 0 error Record is corrupt (stored crc = 1321230880, computed crc = 1139143803)\",\"networkId\":\"vpc-d8c7a9bf\",\"region\":\"us-west-2\",\"serverId\":\"1\",\"skuId\":\"sku5\",\"source\":\"kafka\",\"tenantId\":\"t47\",\"tenantName\":\"perf-test\",\"thread\":\"ReplicaFetcherThread-0-2\",\"zone\":\"us-west-2a\"},\"stream\":\"stdout\",\"time\":2017}"));

    ProducerRecord<String, GenericRow> producerRecord = new ProducerRecord<>(messageLogTopic, "1",
                                                                             messageRow);
    Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
    result.put("1", recordMetadataFuture.get(TEST_RECORD_FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    producer.close();

    return result;
  }


  private Map<String, GenericRow> readNormalResults(String resultTopic, Schema resultSchema, int expectedNumMessages) {
    return readResults(resultTopic, resultSchema, expectedNumMessages, new StringDeserializer());
  }

  private Map<Windowed<String>, GenericRow> readWindowedResults(
      String resultTopic,
      Schema resultSchema,
      int expectedNumMessages
  ) {
    Deserializer<Windowed<String>> keyDeserializer = new WindowedDeserializer<>(new StringDeserializer());
    return readResults(resultTopic, resultSchema, expectedNumMessages, keyDeserializer);
  }

  private <K>Map<K, GenericRow> readResults(
      String resultTopic,
      Schema resultSchema,
      int expectedNumMessages,
      Deserializer<K> keyDeserializer
  ) {
    Map<K, GenericRow> result = new HashMap<>();

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "filter-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<K, GenericRow> consumer =
             new KafkaConsumer<>(consumerConfig, keyDeserializer, new KsqlJsonPojoDeserializer(resultSchema))
    ) {
      consumer.subscribe(Collections.singleton(resultTopic));
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

  private boolean assertExpectedWindowedResults(Map<Windowed<String>, GenericRow> actualResult,
                                                Map<Windowed<String>, GenericRow> expectedResult) {
    Map<String, GenericRow> actualResultSimplified = new HashMap<>();
    Map<String, GenericRow> expectedResultSimplified = new HashMap<>();
    for (Windowed<String> k: expectedResult.keySet()) {
      expectedResultSimplified.put(k.key(), expectedResult.get(k));
    }

    for (Windowed<String> k: actualResult.keySet()) {
      if (actualResult.get(k) != null) {
        actualResultSimplified.put(k.key(), actualResult.get(k));
      }

    }
    return assertExpectedResults(actualResultSimplified, expectedResultSimplified);
  }

}