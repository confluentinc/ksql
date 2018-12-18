/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.KsqlTestContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.OrderDataProvider;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({IntegrationTest.class})
public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> jsonRecordMetadataMap;
  private static final String jsonTopicName = "jsonTopic";
  private static final String jsonStreamName = "orders_json";
  private Map<String, RecordMetadata> avroRecordMetadataMap;
  private static final String avroTopicName = "avroTopic";
  private static final String avroStreamName = "orders_avro";
  private static final String avroTimestampStreamName = "orders_timestamp_avro";
  private OrderDataProvider dataProvider;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start(Collections.emptyMap());
    ksqlContext = KsqlTestContext.create(
        testHarness.ksqlConfig,
        testHarness.schemaRegistryClientFactory);
    testHarness.createTopic(jsonTopicName);
    testHarness.createTopic(avroTopicName);

    /*
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
  public void after() {
    ksqlContext.close();
    testHarness.stop();
  }



  @Test
  public void testTimestampColumnSelectionJson() {

    testTimestampColumnSelection(
        "ORIGINALSTREAM_JSON",
        "TIMESTAMPSTREAM_JSON",
        jsonStreamName,
        DataSource.DataSourceSerDe.JSON,
        jsonRecordMetadataMap);
  }

  @Test
  public void testTimestampColumnSelectionAvro() {

    testTimestampColumnSelection(
        "ORIGINALSTREAM_AVRO",
        "TIMESTAMPSTREAM_AVRO",
        avroStreamName,
        DataSource.DataSourceSerDe.AVRO,
        avroRecordMetadataMap);
  }

  @Test
  public void testSelectProjectKeyTimestampJson() {
    testSelectProjectKeyTimestamp("PROJECT_KEY_TIMESTAMP_JSON",
                                  jsonStreamName,
                                  DataSource.DataSourceSerDe.JSON,
                                  jsonRecordMetadataMap);
  }

  @Test
  public void testSelectProjectKeyTimestampAvro() {
    testSelectProjectKeyTimestamp("PROJECT_KEY_TIMESTAMP_AVRO",
                                  avroStreamName,
                                  DataSource.DataSourceSerDe.AVRO,
                                  avroRecordMetadataMap);
  }

  @Test
  public void testSelectProjectJson() {
    testSelectProject("PROJECT_STREAM_JSON",
                      jsonStreamName,
                      DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectProjectAvro() {
    testSelectProject("PROJECT_STREAM_AVRO",
                      avroStreamName,
                      DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void testSelectStarJson() {
    testSelectStar("EASYORDERS_JSON",
                   jsonStreamName,
                   DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectStarAvro() {
    testSelectStar("EASYORDERS_AVRO",
                   avroStreamName,
                   DataSource.DataSourceSerDe.AVRO);
  }


  @Test
  public void testSelectWithFilterJson() {
    testSelectWithFilter("BIGORDERS_JSON",
                         jsonStreamName,
                         DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void testSelectWithFilterAvro() {
    testSelectWithFilter("BIGORDERS_AVRO",
                         avroStreamName,
                         DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void shouldSkipBadData() {
    testHarness.createTopic(jsonTopicName);
    testHarness.produceRecord(jsonTopicName, "bad", "something that is not json");
    testSelectWithFilter("BIGORDERS_JSON1",
                         jsonStreamName,
                         DataSource.DataSourceSerDe.JSON);
  }

  @Test
  public void shouldSkipBadDataAvro() {
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

  private void testTimestampColumnSelection(final String stream1Name,
                                            final String stream2Name,
                                            final String inputStreamName,
                                            final DataSource.DataSourceSerDe dataSourceSerDe,
                                            final Map<String, RecordMetadata> recordMetadataMap) {
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

    final Map<String, GenericRow> expectedResults = new HashMap<>();
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

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    final Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema ,
        expectedResults.size(),
        new StringDeserializer(),
        IntegrationTestHarness
            .RESULTS_POLL_MAX_TIME_MS,
        dataSourceSerDe);

    assertThat(results2, equalTo(expectedResults));
  }

  private void testSelectProjectKeyTimestamp(final String resultStream,
                                             final String inputStreamName,
                                             final DataSource.DataSourceSerDe dataSourceSerDe,
                                             final Map<String, RecordMetadata> recordMetadataMap) {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ROWKEY AS RKEY, ROWTIME "
                                  + "AS RTIME, ITEMID FROM %s WHERE ORDERUNITS > 20 AND ITEMID = "
                                  + "'ITEM_8';", resultStream, inputStreamName));

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    final Map<String, GenericRow> results = testHarness.consumeData(resultStream, resultSchema ,
                                                              dataProvider.data().size(),
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS,
                                                              dataSourceSerDe);

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("8",
                                 new GenericRow(
                                     Arrays.asList(null,
                                                   null,
                                                   "8",
                                                   recordMetadataMap.get("8").timestamp(),
                                                   "ITEM_8")));

    assertThat(results, equalTo(expectedResults));
  }

  private void testSelectProject(final String resultStream,
                                 final String inputStreamName,
                                 final DataSource
      .DataSourceSerDe dataSourceSerDe) {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ITEMID, ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, inputStreamName));

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    final Map<String, GenericRow> easyOrdersData =
        testHarness.consumeData(resultStream,
                                resultSchema,
                                dataProvider.data().size(),
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                dataSourceSerDe);

    final GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectProjectAvroJson() {

    final String resultStream = "PROJECT_STREAM_AVRO";
    ksqlContext.sql(String.format("CREATE STREAM %s WITH ( value_format = 'JSON') AS SELECT "
                                  + "ITEMID, "
                                  + "ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, avroStreamName));

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(resultStream).getSchema();

    final Map<String, GenericRow> easyOrdersData =
        testHarness.consumeData(resultStream, resultSchema,
                                dataProvider.data().size(),
                                new StringDeserializer(),
                                IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                DataSource.DataSourceSerDe.JSON);

    final GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }

  private void testSelectStar(final String resultStream,
                              final String inputStreamName,
                              final DataSource.DataSourceSerDe dataSourceSerDe) {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;",
                                  resultStream,
                                  inputStreamName));

    final Map<String, GenericRow> easyOrdersData = testHarness.consumeData(resultStream,
                                                                     dataProvider.schema(),
                                                                     dataProvider.data().size(),
                                                                     new StringDeserializer(),
                                                                     IntegrationTestHarness
                                                                         .RESULTS_POLL_MAX_TIME_MS,
                                                                     dataSourceSerDe);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }

  private void testSelectWithFilter(final String resultStream,
                                    final String inputStreamName,
                                    final DataSource.DataSourceSerDe dataSourceSerDe) {

    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s WHERE ORDERUNITS > 40;",
                                  resultStream, inputStreamName));

    final Map<String, GenericRow> results = testHarness.consumeData(resultStream,
                                                              dataProvider.schema(),
                                                              4,
                                                              new StringDeserializer(),
                                                              IntegrationTestHarness
                                                                  .RESULTS_POLL_MAX_TIME_MS,
                                                              dataSourceSerDe);

    Assert.assertEquals(4, results.size());
  }

  @Test
  public void testInsertIntoJson() {

    ksqlContext.sql(String.format("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, "
                            + "PRICEARRAY FROM "
                    + "%s WHERE ITEMID = 'HELLO';", jsonStreamName));

    ksqlContext.sql(String.format("INSERT INTO PROJECT_STREAM SELECT ITEMID, ORDERUNITS, PRICEARRAY "
                            + "FROM %s;", jsonStreamName));

    final Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema();

    final Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM",
                                                                     resultSchema, dataProvider
                                                                         .data().size(), new
                                                                         StringDeserializer(),
                                                                     IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                                                     DataSource.DataSourceSerDe.JSON);

    final GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }

  @Test
  public void testInsertIntoAvro() {

    ksqlContext.sql(String.format("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, "
                                  + "PRICEARRAY FROM "
                                  + "%s WHERE ITEMID = 'HELLO';", avroStreamName));

    ksqlContext.sql(String.format("INSERT INTO PROJECT_STREAM SELECT ITEMID, ORDERUNITS, PRICEARRAY "
                                  + "FROM %s;", avroStreamName));

    final Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema();

    final Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM",
                                                                     resultSchema, dataProvider
                                                                         .data().size(), new
                                                                         StringDeserializer(),
                                                                     IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                                                     DataSource.DataSourceSerDe.AVRO);

    final GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2).toString());
  }

  @Test
  public void testInsertSelectStarJson() {

    ksqlContext.sql(String.format("CREATE STREAM EASYORDERS AS SELECT * FROM %s WHERE ITEMID = "
                                  + "'HELLO';", jsonStreamName));
    ksqlContext.sql(String.format("INSERT INTO EASYORDERS SELECT * FROM %s;", jsonStreamName));

    final Map<String, GenericRow> easyOrdersData = testHarness.consumeData("EASYORDERS", dataProvider
        .schema(), dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness
        .RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.JSON);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }

  @Test
  public void testInsertSelectStarAvro() {

    ksqlContext.sql(String.format("CREATE STREAM EASYORDERS AS SELECT * FROM %s WHERE ITEMID = "
                                  + "'HELLO';", avroStreamName));
    ksqlContext.sql(String.format("INSERT INTO EASYORDERS SELECT * FROM %s;", avroStreamName));

    final Map<String, GenericRow> easyOrdersData = testHarness.consumeData("EASYORDERS", dataProvider
        .schema(), dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness
                                                                         .RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }

  @Test
  public void testInsertSelectWithFilterJson() {

    ksqlContext.sql(String.format("CREATE STREAM BIGORDERS_json AS SELECT * FROM %s WHERE ORDERUNITS > "
                           + "100000;", jsonStreamName));
    ksqlContext.sql(String.format("INSERT INTO BIGORDERS_json SELECT * FROM %s WHERE ORDERUNITS > 40;"
                                  + "", jsonStreamName));

    final Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS_json", dataProvider
                                                                  .schema()
        , 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.JSON);

    Assert.assertEquals(4, results.size());
  }

  @Test
  public void testInsertSelectWithFilterAvro() {

    ksqlContext.sql(String.format("CREATE STREAM BIGORDERS_avro AS SELECT * FROM %s WHERE ORDERUNITS > "
                                  + "100000;", avroStreamName));
    ksqlContext.sql(String.format("INSERT INTO BIGORDERS_avro SELECT * FROM %s WHERE ORDERUNITS > 40;"
                                  + "", avroStreamName));

    final Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS_avro", dataProvider
                                                                  .schema()
        , 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                                              DataSource.DataSourceSerDe.AVRO);

    Assert.assertEquals(4, results.size());
  }

  private void createOrdersStream() {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
            + "varchar, ORDERUNITS double, TIMESTAMP varchar, PRICEARRAY array<double>,"
            + " KEYVALUEMAP "
            + "map<varchar, double>) WITH (kafka_topic='%s', "
            + "value_format='JSON', key='ordertime');",
        jsonStreamName,
        jsonTopicName));

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