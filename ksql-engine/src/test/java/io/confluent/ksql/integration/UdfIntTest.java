package io.confluent.ksql.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.function.UdfCompiler;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  private String delimitedTopicName = "delimitedTopic";
  private String delimitedStreamName = "items_delimited";

  private OrderDataProvider orderDataProvider;
  private ItemDataProvider itemDataProvider;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start(Collections.emptyMap());
    ksqlContext = KsqlContext.create(
        testHarness.ksqlConfig,
        testHarness.schemaRegistryClientFactory);
    testHarness.createTopic(jsonTopicName);

    testHarness.createTopic(avroTopicName);

    // load substring udf from classpath
    new UdfLoader(ksqlContext.getMetaStore(),
        TestUtils.tempDirectory(),
        getClass().getClassLoader(),
        value -> true, new UdfCompiler(Optional.empty()), Optional.empty(), true)
        .load();

    /**
     * Setup test data
     */
    orderDataProvider = new OrderDataProvider();
    itemDataProvider = new ItemDataProvider();
    jsonRecordMetadataMap = testHarness.publishTestData(jsonTopicName,
                                                        orderDataProvider,
                                                        null,
                                                        DataSource.DataSourceSerDe.JSON);
    avroRecordMetadataMap = testHarness.publishTestData(avroTopicName,
                                                        orderDataProvider,
                                                        null,
                                                        DataSource.DataSourceSerDe.AVRO);
    testHarness.publishTestData(delimitedTopicName,
                                itemDataProvider,
                                null,
                                DataSource.DataSourceSerDe.DELIMITED);
    createOrdersStream();

  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testApplyUdfsToColumnsJson() throws Exception {
    testApplyUdfsToColumns("SelectUDFsStreamJson".toUpperCase(),
                           jsonStreamName,
                           DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testApplyUdfsToColumnsAvro() throws Exception {
    testApplyUdfsToColumns("SelectUDFsStreamAvro".toUpperCase(),
                           avroStreamName,
                           DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void testShouldCastSelectedColumnsJson() throws Exception {
    testShouldCastSelectedColumns("CastExpressionStream".toUpperCase(),
                                  jsonStreamName,
                                  DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testShouldCastSelectedColumnsAvro() throws Exception {
    testShouldCastSelectedColumns("CastExpressionStreamAvro".toUpperCase(),
                                  avroStreamName,
                                  DataSource.DataSourceSerDe.AVRO);
  }


  @Test
  public void testTimestampColumnSelectionJson() throws Exception {
    testTimestampColumnSelection("ORIGINALSTREAM",
                                 "TIMESTAMPSTREAM",
                                 jsonStreamName,
                                 DataSource.DataSourceSerDe.JSON,
                                 jsonRecordMetadataMap);
  }


  @Test
  public void testTimestampColumnSelectionAvro() throws Exception {
    testTimestampColumnSelection("ORIGINALSTREAM_AVRO",
                                 "TIMESTAMPSTREAM_AVRO",
                                 avroStreamName,
                                 DataSource.DataSourceSerDe.AVRO,
                                 avroRecordMetadataMap);
  }

  private void testApplyUdfsToColumns(final String resultStreamName,
                                      final String inputStreamName,
                                      final DataSource.DataSourceSerDe dataSourceSerde) throws Exception {

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        resultStreamName,
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, "
        + "PRICEARRAY[1]>1000",
        inputStreamName,
        "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStreamName).getSchema();

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("8",
                                 new GenericRow(Arrays.asList(null,
                                                              null,
                                                              "ITEM_8",
                                                              800.0,
                                                              1110.0,
                                                              12.0,
                                                              true)));

    final Map<String, GenericRow> results =
        testHarness.consumeData(resultStreamName,
                                resultSchema,
                                4,
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                dataSourceSerde);

    assertThat(results, equalTo(expectedResults));
  }

  private void testShouldCastSelectedColumns(final String resultStreamName,
                                             final String inputStreamName,
                                             final DataSource.DataSourceSerDe dataSourceSerde)
      throws Exception {
    final String selectColumns =
        " CAST (ORDERUNITS AS INTEGER), CAST( PRICEARRAY[1]>1000 AS STRING), CAST (SUBSTRING"
        + "(ITEMID, 5) AS DOUBLE), CAST(ORDERUNITS AS VARCHAR) ";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        resultStreamName,
        selectColumns,
        inputStreamName,
        "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStreamName).getSchema();

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("8",
                                 new GenericRow(Arrays.asList(
                                     null,
                                     null,
                                     80,
                                     "true",
                                     8.0,
                                     "80.0")));

    final Map<String, GenericRow> results =
        testHarness.consumeData(resultStreamName,
                                resultSchema,
                                4,
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, dataSourceSerde);

    assertThat(results, equalTo(expectedResults));
  }

  private void testTimestampColumnSelection(final String stream1Name, final String stream2Name, final String
      inputStreamName, final DataSource.DataSourceSerDe dataSourceSerDe, final Map<String, RecordMetadata>
                                                recordMetadataMap) throws Exception {

    final String query1String =
        String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, "
                      + "ROWTIME+10000 AS "
                      + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                      + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                      + "CREATE STREAM %s AS SELECT ROWKEY AS NEWRKEY, "
                      + "ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
                      + "FROM %s ;", stream1Name,
                      inputStreamName, stream2Name, stream1Name);

    ksqlContext.sql(query1String);

    final Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
        ksqlContext.getMetaStore().getSource(stream2Name).getSchema());

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8",
                        new GenericRow(Arrays.asList(
                            "8",
                            recordMetadataMap.get("8").timestamp(),
                            "8",
                            recordMetadataMap.get("8").timestamp() + 10000,
                            recordMetadataMap.get("8").timestamp() + 100,
                            "ORDER_6",
                            "ITEM_8")));

    final Map<String, GenericRow> results =
        testHarness.consumeData(stream2Name,
                                resultSchema,
                                expectedResults.size(),
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, dataSourceSerDe);

    assertThat(results, equalTo(expectedResults));
  }


  @Test
  public void testApplyUdfsToColumnsDelimited() throws Exception {
    final String testStreamName = "SelectUDFsStreamDelimited".toUpperCase();

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        testStreamName,
        "ID, DESCRIPTION",
        delimitedStreamName,
        "ID LIKE '%_1'"
    );

    ksqlContext.sql(queryString);

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("ITEM_1",
                                 new GenericRow(Arrays.asList(
                                     "ITEM_1",
                                     "home cinema")));

    final Map<String, GenericRow> results =
        testHarness.consumeData(
            testStreamName,
            itemDataProvider.schema(),
            1,
            new StringDeserializer(),
            IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
            DataSource.DataSourceSerDe.DELIMITED);

    assertThat(results, equalTo(expectedResults));
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                  + "ITEMID varchar, "
                                  + "ORDERUNITS double, "
                                  + "PRICEARRAY array<double>, "
                                  + "KEYVALUEMAP map<varchar, double>) "
                                  + "WITH (kafka_topic='%s', value_format='%s');",
                                  jsonStreamName,
                                  jsonTopicName,
                                  DataSource.DataSourceSerDe.JSON.name()));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, "
                                  + "ORDERID varchar, "
                                  + "ITEMID varchar, "
                                  + "ORDERUNITS double, "
                                  + "PRICEARRAY array<double>, "
                                  + "KEYVALUEMAP map<varchar, double>) "
                                  + "WITH (kafka_topic='%s', value_format='%s');",
                                  avroStreamName,
                                  avroTopicName,
                                  DataSource.DataSourceSerDe.AVRO.name()));
    ksqlContext.sql(String.format("CREATE STREAM %s (ID varchar, DESCRIPTION varchar) WITH "
                     + "(kafka_topic='%s', value_format='%s');",
                                  delimitedStreamName,
                                  delimitedTopicName,
                                  DataSource.DataSourceSerDe.DELIMITED.name()));
  }

}
