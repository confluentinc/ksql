package io.confluent.ksql.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.OrderDataProvider;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


@Category({IntegrationTest.class})
public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> jsonRecordMetadataMap;
  private final String jsonTopicName = "jsonTopic";
  private final String jsonStreamName = "orders_json";
  private Map<String, RecordMetadata> avroRecordMetadataMap;
  private final String avroTopicName = "avroTopic";
  private final String avroStreamName = "orders_avro";
  private final String avroTimestampStreamName = "orders_timestamp_avro";
  private OrderDataProvider dataProvider;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig, testHarness.schemaRegistryClient);
    testHarness.createTopic(jsonTopicName);
    testHarness.createTopic(avroTopicName);

    /**
     * Setup test data
     */
    dataProvider = new OrderDataProvider();
    jsonRecordMetadataMap = testHarness.publishTestData(jsonTopicName,
                                                        dataProvider,
                                                        null,
                                                        DataSource.DataSourceSerDe.JSON);
    avroRecordMetadataMap = testHarness.publishTestData(avroTopicName,
                                                        dataProvider,
                                                        null,
                                                        DataSource.DataSourceSerDe.AVRO);
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }



  @Test
  public void testTimestampColumnSelectionJson() throws Exception {

    testTimestampColumnSelection(
        "ORIGINALSTREAM_JSON",
        "TIMESTAMPSTREAM_JSON",
        jsonStreamName,
        DataSource.DataSourceSerDe.JSON,
        jsonRecordMetadataMap);
  }

  @Test
  public void testTimestampColumnSelectionAvro() throws Exception {

    testTimestampColumnSelection(
        "ORIGINALSTREAM_AVRO",
        "TIMESTAMPSTREAM_AVRO",
        avroStreamName,
        DataSource.DataSourceSerDe.AVRO,
        avroRecordMetadataMap);
  }

  @Test
  public void testSelectProjectKeyTimestampJson() throws Exception {
    testSelectProjectKeyTimestamp("PROJECT_KEY_TIMESTAMP_JSON",
                                  jsonStreamName,
                                  DataSource.DataSourceSerDe.JSON,
                                  jsonRecordMetadataMap);
  }

  @Test
  public void testSelectProjectKeyTimestampAvro() throws Exception {
    testSelectProjectKeyTimestamp("PROJECT_KEY_TIMESTAMP_AVRO",
                                  avroStreamName,
                                  DataSource.DataSourceSerDe.AVRO,
                                  avroRecordMetadataMap);
  }

  @Test
  public void testSelectProjectJson() throws Exception {
    testSelectProject("PROJECT_STREAM_JSON",
                      jsonStreamName,
                      DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectProjectAvro() throws Exception {
    testSelectProject("PROJECT_STREAM_AVRO",
                      avroStreamName,
                      DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void testSelectStarJson() throws Exception {
    testSelectStar("EASYORDERS_JSON",
                   jsonStreamName,
                   DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectStarAvro() throws Exception {
    testSelectStar("EASYORDERS_AVRO",
                   avroStreamName,
                   DataSource.DataSourceSerDe.AVRO);
  }


  @Test
  public void testSelectWithFilterJson() throws Exception {
    testSelectWithFilter("BIGORDERS_JSON",
                         jsonStreamName,
                         DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectWithFilterAvro() throws Exception {
    testSelectWithFilter("BIGORDERS_AVRO",
                         avroStreamName,
                         DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void shouldSkipBadData() throws Exception {
    testHarness.createTopic(jsonTopicName);
    testHarness.produceRecord(jsonTopicName, "bad", "something that is not json");
    testSelectWithFilter("BIGORDERS_JSON1",
                         jsonStreamName,
                         DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void shouldSkipBadDataAvro() throws Exception {
    testHarness.createTopic(avroTopicName);
    testHarness.produceRecord(avroTopicName, "bad", "something that is not avro");
    testSelectWithFilter("BIGORDERS_AVRO1",
                         avroStreamName,
                         DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void shouldUseStringTimestampWithFormat() throws Exception {
    final String outputStream = "TIMESTAMP_STRING";
    ksqlContext.sql("CREATE STREAM STRING_TIMESTAMP WITH (timestamp='TIMESTAMP', timestamp_format='yyyy-MM-dd')"
        + " AS SELECT ORDERID, TIMESTAMP FROM ORDERS_AVRO WHERE ITEMID='ITEM_6';"
        + " CREATE STREAM TIMESTAMP_STRING AS SELECT ORDERID, TIMESTAMP from STRING_TIMESTAMP;");

    final List<ConsumerRecord> records = testHarness.consumerRecords(outputStream,
        1,
        IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final long timestamp = records.get(0).timestamp();
    assertThat(timestamp, equalTo(dateFormat.parse("2018-01-06").getTime()));
  }

  @Test
  public void shouldUseTimestampExtractedFromDDLStatement() throws Exception {
    final String outputStream = "DDL_TIMESTAMP";
    ksqlContext.sql("CREATE STREAM "+  outputStream
        + " WITH(timestamp='ordertime')"
        + " AS SELECT ORDERID, ORDERTIME FROM "
        + avroTimestampStreamName
        + " WHERE ITEMID='ITEM_4';");

    final List<ConsumerRecord> records = testHarness.consumerRecords(outputStream,
        1,
        IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final long timestamp = records.get(0).timestamp();
    assertThat(timestamp, equalTo(dateFormat.parse("2018-01-04").getTime()));
  }

  private void testTimestampColumnSelection(String stream1Name,
                                            String stream2Name,
                                            String inputStreamName,
                                            DataSource.DataSourceSerDe dataSourceSerDe,
                                            Map<String, RecordMetadata> recordMetadataMap)
      throws Exception {
    final String query1String =
        String.format("CREATE STREAM %s WITH (timestamp='RTIME') AS SELECT ROWKEY AS RKEY, "
                + "ROWTIME+10000 AS "
                + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                + "CREATE STREAM %s AS SELECT ROWKEY AS NEWRKEY, "
                + "ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
                + "FROM %s ;", stream1Name,
            inputStreamName, stream2Name, stream1Name);


    ksqlContext.sql(query1String);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8",
        new GenericRow(Arrays.asList(null,
            null,
            "8",
            recordMetadataMap.get("8").timestamp() + 10000,
            "8",
            recordMetadataMap.get("8").timestamp() + 10000,
            recordMetadataMap.get("8").timestamp() + 100,
            "ORDER_6",
            "ITEM_8")));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema ,
        expectedResults.size(),
        new StringDeserializer(),
        IntegrationTestHarness
            .RESULTS_POLL_MAX_TIME_MS,
        dataSourceSerDe);

    assertThat(results2, equalTo(expectedResults));
  }

  private void testSelectProjectKeyTimestamp(String resultStream,
                                             String inputStreamName,
                                             DataSource.DataSourceSerDe dataSourceSerDe,
                                             Map<String, RecordMetadata> recordMetadataMap)
      throws Exception {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME "
                                  + "AS RTIME, ITEMID FROM %s WHERE ORDERUNITS > 20 AND ITEMID = "
                                  + "'ITEM_8';", resultStream, inputStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> results = testHarness.consumeData(resultStream, resultSchema ,
                                                              dataProvider.data().size(),
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS,
                                                              dataSourceSerDe);

    Map<String, GenericRow> expectedResults =
        Collections.singletonMap("8",
                                 new GenericRow(
                                     Arrays.asList(null,
                                                   null,
                                                   "8",
                                                   recordMetadataMap.get("8").timestamp(),
                                                   "ITEM_8")));

    assertThat(results, equalTo(expectedResults));
  }

  private void testSelectProject(String resultStream,
                                 String inputStreamName,
                                 DataSource
      .DataSourceSerDe dataSourceSerDe) throws Exception {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, inputStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> easyOrdersData =
        testHarness.consumeData(resultStream,
                                resultSchema,
                                dataProvider.data().size(),
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                dataSourceSerDe);

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectProjectAvroJson() throws Exception {

    String resultStream = "PROJECT_STREAM_AVRO";
    ksqlContext.sql(String.format("CREATE STREAM %s WITH ( value_format = 'JSON') AS SELECT "
                                  + "ITEMID, "
                                  + "ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, avroStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> easyOrdersData =
        testHarness.consumeData(resultStream, resultSchema,
                                dataProvider.data().size(),
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                DataSource.DataSourceSerDe.JSON);

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }

  private void testSelectStar(String resultStream,
                              String inputStreamName,
                              DataSource.DataSourceSerDe dataSourceSerDe) throws Exception {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;",
                                  resultStream,
                                  inputStreamName));

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream,
                                                                     dataProvider.schema(),
                                                                     dataProvider.data().size(),
                                                                     new StringDeserializer(),
                                                                     IntegrationTestHarness
                                                                         .RESULTS_POLL_MAX_TIME_MS,
                                                                     dataSourceSerDe);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }

  private void testSelectWithFilter(String resultStream,
                                    String inputStreamName,
                                    DataSource.DataSourceSerDe dataSourceSerDe) throws Exception {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 40;",
                                  resultStream, inputStreamName));

    Map<String, GenericRow> results = testHarness.consumeData(resultStream,
                                                              dataProvider.schema(),
                                                              4,
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS,
                                                              dataSourceSerDe);

    Assert.assertEquals(4, results.size());
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
            + "varchar, ORDERUNITS double, TIMESTAMP varchar, PRICEARRAY array<double>,"
            + " KEYVALUEMAP "
            + "map<varchar, double>) WITH (kafka_topic='%s', "
            + "value_format='JSON', key='ordertime');",
        jsonStreamName,
        jsonTopicName,
        DataSource.DataSourceSerDe.JSON.name()));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
            + "varchar, "
            + "ORDERUNITS double, TIMESTAMP varchar, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, "
            + "double>) WITH (kafka_topic='%s', value_format='%s', "
            + "key='ordertime');",
        avroStreamName,
        avroTopicName,
        DataSource.DataSourceSerDe.AVRO.name()));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
            + "varchar, "
            + "ORDERUNITS double, TIMESTAMP varchar, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, "
            + "double>) WITH (kafka_topic='%s', value_format='%s', "
            + "key='ordertime', timestamp='timestamp', timestamp_format='yyyy-MM-dd');",
        avroTimestampStreamName,
        avroTopicName,
        DataSource.DataSourceSerDe.AVRO.name()));
  }

}