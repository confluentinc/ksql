package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.OrderDataProvider;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.IntegrationTest;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.anyOf;
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
    jsonRecordMetadataMap = testHarness.publishTestData(jsonTopicName, dataProvider, null , DataSource.DataSourceSerDe.JSON.name());
    avroRecordMetadataMap = testHarness.publishTestData(avroTopicName, dataProvider, null , DataSource.DataSourceSerDe.AVRO.name());
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testTimestampColumnSelection() throws Exception {

    final String stream1Name = "ORIGINALSTREAM_JSON";
    final String stream2Name = "TIMESTAMPSTREAM_JSON";
    final String query1String =
            String.format("CREATE STREAM %s WITH (timestamp='RTIME') AS SELECT ROWKEY AS RKEY, "
                            + "ROWTIME+10000 AS "
                            + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                            + "CREATE STREAM %s AS SELECT ROWKEY AS NEWRKEY, "
                            + "ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
                            + "FROM %s ;", stream1Name,
                    jsonStreamName, stream2Name, stream1Name);


    ksqlContext.sql(query1String);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", jsonRecordMetadataMap
                                                                               .get("8")
                    .timestamp() + 10000, "8", jsonRecordMetadataMap.get("8").timestamp() + 10000,
                                                          jsonRecordMetadataMap.get("8").timestamp
                    () + 100, "ORDER_6", "ITEM_8")));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema , expectedResults.size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(results2, equalTo(expectedResults));
  }


  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {

    String resultStream = "PROJECT_KEY_TIMESTAMP";

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME "
                         + "AS RTIME, ITEMID FROM %s WHERE ORDERUNITS > 20 AND ITEMID = "
                                  + "'ITEM_8';", resultStream, jsonStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> results = testHarness.consumeData(resultStream, resultSchema , dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays
                                                                                               .asList(null, null, "8", jsonRecordMetadataMap.get("8").timestamp(), "ITEM_8")));

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testSelectProject() throws Exception {

    String resultStream = "PROJECT_STREAM";

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, "
                            + "PRICEARRAY FROM %s;", resultStream, jsonStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream, resultSchema, dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectStar() throws Exception {

    String resultStream = "EASYORDERS";

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;", resultStream, jsonStreamName));

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream, dataProvider.schema(), dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }


  @Test
  public void testSelectWithFilter() throws Exception {

    String resultStream = "bigorders".toUpperCase();

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 40;",
                                  resultStream, jsonStreamName));

    Map<String, GenericRow> results = testHarness.consumeData(resultStream, dataProvider.schema(), 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    Assert.assertEquals(4, results.size());
  }

  @Test
  public void shouldSkipBadData() throws Exception {
    testHarness.createTopic(jsonTopicName);
    testHarness.produceRecord(jsonTopicName, "bad", "something that is not json");
    testSelectWithFilter();
  }
//======================


  @Test
  public void testTimestampColumnSelectionAvro() throws Exception {

    final String stream1Name = "ORIGINALSTREAM_AVRO";
    final String stream2Name = "TIMESTAMPSTREAM_AVRO";
    final String query1String =
        String.format("CREATE STREAM %s WITH (timestamp='RTIME') AS SELECT ROWKEY AS RKEY, "
                      + "ROWTIME+10000 AS "
                      + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                      + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                      + "CREATE STREAM %s AS SELECT ROWKEY AS NEWRKEY, "
                      + "ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
                      + "FROM %s ;", stream1Name,
                      avroStreamName, stream2Name, stream1Name);


    ksqlContext.sql(query1String);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", avroRecordMetadataMap
                                                                               .get("8")
                                                                               .timestamp() + 10000, "8", avroRecordMetadataMap.get("8").timestamp() + 10000,
                                                          avroRecordMetadataMap.get("8").timestamp
                                                              () + 100, "ORDER_6", "ITEM_8")));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema , expectedResults.size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());
    assertThat(results2, equalTo(expectedResults));
  }


  @Test
  public void testSelectProjectKeyTimestampAvro() throws Exception {

    String resultStream = "PROJECT_KEY_TIMESTAMP_AVRO";

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, "
                                  + "ITEMID "
                           + "FROM "
                     + "%s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';", resultStream, avroStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> results = testHarness.consumeData(resultStream, resultSchema , dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, "8", avroRecordMetadataMap.get("8").timestamp(), "ITEM_8")));
    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testSelectProjectAvro() throws Exception {

    String resultStream = "PROJECT_STREAM_AVRO";
    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, "
                            + "PRICEARRAY FROM %s;", resultStream, avroStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream, resultSchema, dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }

  @Test
  public void testSelectProjectAvroJson() throws Exception {

    String resultStream = "PROJECT_STREAM_AVRO";
    ksqlContext.sql(String.format("CREATE STREAM %s WITH ( value_format = 'JSON') AS SELECT "
                                  + "ITEMID, "
                                  + "ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, avroStreamName));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream, resultSchema,
                                                                     dataProvider.data().size(),
                                                                     new StringDeserializer(),
                                                                     IntegrationTestHarness
                                                                         .RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.JSON.name());

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }


  @Test
  public void testSelectStarAvro() throws Exception {
    String resultStream = "EASYORDERS_AVRO";
    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;",
                                  resultStream, avroStreamName));

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream, dataProvider.schema(), dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }


  @Test
  public void testSelectWithFilterAvro() throws Exception {

    String resultStream = "bigorders_avro".toUpperCase();

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE "
                                  + "ORDERUNITS >  40;", resultStream, avroStreamName));

    Map<String, GenericRow> results = testHarness.consumeData(resultStream, dataProvider.schema(), 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());

    Assert.assertEquals(4, results.size());
  }

  @Test
  public void shouldSkipBadDataAvro() throws Exception {
    testHarness.createTopic(avroTopicName);
    testHarness.produceRecord(avroTopicName, "bad", "something that is not avro");
    testSelectWithFilterAvro();
  }


  private void createOrdersStream() throws Exception {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                         + "varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                  + "map<varchar, double>) WITH (kafka_topic='%s', "
                                  + "value_format='JSON', key='ordertime');", jsonStreamName,
                                  jsonTopicName, DataSource.DataSourceSerDe.JSON.name()));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                           + "varchar, "
                    + "ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, "
                    + "double>) WITH (kafka_topic='%s', value_format='%s', "
                    + "key='ordertime');", avroStreamName, avroTopicName, DataSource.DataSourceSerDe.AVRO.name()));
  }

}
