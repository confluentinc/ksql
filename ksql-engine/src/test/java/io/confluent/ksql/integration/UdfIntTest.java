package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class UdfIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> jsonRecordMetadataMap;
  private String jsonTopicName = "jsonTopic";
  private String jsonStreamName = "orders_json";

  private Map<String, RecordMetadata> avroRecordMetadataMap;
  private String avroTopicName = "avroTopic";
  private String avroStreamName = "orders_avro";

  private Map<String, RecordMetadata> delimitedRecordMetadataMap;
  private String delimitedTopicName = "delimitedTopic";
  private String delimitedStreamName = "items_delimited";

  private OrderDataProvider orderDataProvider;
  private ItemDataProvider itemDataProvider;
  String format = DataSource.DataSourceSerDe.JSON.name();

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
    orderDataProvider = new OrderDataProvider();
    itemDataProvider = new ItemDataProvider();
    jsonRecordMetadataMap = testHarness.publishTestData(jsonTopicName, orderDataProvider, null, DataSource.DataSourceSerDe.JSON.name());
    avroRecordMetadataMap = testHarness.publishTestData(avroTopicName, orderDataProvider, null, DataSource.DataSourceSerDe.AVRO.name());
    delimitedRecordMetadataMap = testHarness.publishTestData(delimitedTopicName, itemDataProvider, null, DataSource
        .DataSourceSerDe.DELIMITED.name());
    createOrdersStream();

  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }


  @Test
  public void testApplyUdfsToColumnsJson() throws Exception {
    final String testStreamName = "SelectUDFsStreamJson".toUpperCase();

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
            testStreamName,
            "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000",
            jsonStreamName,
            "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, "ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = testHarness.consumeData(testStreamName, resultSchema, 4,
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS,
                                                              DataSource.DataSourceSerDe.JSON.name());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testApplyUdfsToColumnsAvro() throws Exception {
    final String testStreamName = "SelectUDFsStreamAvro".toUpperCase();

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        testStreamName,
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000",
        avroStreamName,
        "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, "ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = testHarness.consumeData(testStreamName, resultSchema, 4,
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());
    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testShouldCastSelectedColumns() throws Exception {
    final String streamName = "CastExpressionStream".toUpperCase();

    final String selectColumns =
            " CAST (ORDERUNITS AS INTEGER), CAST( PRICEARRAY[1]>1000 AS STRING), CAST (SUBSTRING"
                    + "(ITEMID, 5) AS DOUBLE), CAST(ORDERUNITS AS VARCHAR) ";

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
            streamName,
            selectColumns,
            jsonStreamName,
            "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, 80, "true", 8.0, "80.0")));

    Map<String, GenericRow> results = testHarness.consumeData(streamName, resultSchema, 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(results, equalTo(expectedResults));
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
                          jsonStreamName, stream2Name, stream1Name);

    ksqlContext.sql(query1String);

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(ksqlContext.getMetaStore().getSource(stream2Name).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", jsonRecordMetadataMap.get("8").timestamp() +
                    10000, "8", jsonRecordMetadataMap.get("8").timestamp() + 10000,
                                                          jsonRecordMetadataMap.get("8").timestamp() + 100, "ORDER_6", "ITEM_8")));

    Map<String, GenericRow> results = testHarness.consumeData(stream2Name, resultSchema,expectedResults.size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(results, equalTo(expectedResults));
  }


  @Test
  public void testShouldCastSelectedColumnsAvro() throws Exception {
    final String streamName = "CastExpressionStreamAvro".toUpperCase();

    final String selectColumns =
        " CAST (ORDERUNITS AS INTEGER), CAST( PRICEARRAY[1]>1000 AS STRING), CAST (SUBSTRING"
        + "(ITEMID, 5) AS DOUBLE), CAST(ORDERUNITS AS VARCHAR) ";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        avroStreamName,
        "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, 80, "true", 8.0, "80.0")));

    Map<String, GenericRow> results = testHarness.consumeData(streamName, resultSchema, 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());
    assertThat(results, equalTo(expectedResults));
  }

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

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(ksqlContext.getMetaStore().getSource(stream2Name).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", avroRecordMetadataMap.get("8")
                                                                   .timestamp() +
                                                               10000, "8", avroRecordMetadataMap.get("8").timestamp() + 10000,
                                                          avroRecordMetadataMap.get("8").timestamp() + 100, "ORDER_6", "ITEM_8")));

    Map<String, GenericRow> results = testHarness.consumeData(stream2Name, resultSchema,expectedResults.size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name());
    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testApplyUdfsToColumns() throws Exception {
    final String testStreamName = "SelectUDFsStreamDelimited".toUpperCase();

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        testStreamName,
        "ID, DESCRIPTION",
        delimitedStreamName,
        "ID LIKE '%_1'"
    );

    ksqlContext.sql(queryString);

    Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList( "ITEM_1", "home cinema")));

    Map<String, GenericRow> results = testHarness.consumeData(testStreamName, itemDataProvider.schema
        (), 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.DELIMITED.name());

    assertThat(results, equalTo(expectedResults));
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                         + "varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                  + "map<varchar, double>) WITH (kafka_topic='%s', "
                                  + "value_format='%s');", jsonStreamName, jsonTopicName, DataSource.DataSourceSerDe.JSON.name()));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                                  + "varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                  + "map<varchar, double>) WITH (kafka_topic='%s', "
                                  + "value_format='%s');", avroStreamName, avroTopicName, DataSource.DataSourceSerDe.AVRO.name()));
    ksqlContext.sql(String.format("CREATE STREAM %s (ID varchar, DESCRIPTION varchar) WITH "
                     + "(kafka_topic='%s', value_format='%s');",
                    delimitedStreamName, delimitedTopicName, DataSource.DataSourceSerDe.DELIMITED.name()));
  }

}
