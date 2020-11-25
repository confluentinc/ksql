/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;


@Category({IntegrationTest.class})
public class StreamsSelectAndProjectIntTest {

  private static final String JSON_STREAM_NAME = "orders_json";
  private static final String AVRO_STREAM_NAME = "orders_avro";
  private static final String AVRO_TIMESTAMP_STREAM_NAME = "orders_timestamp_avro";
  private static final OrderDataProvider DATA_PROVIDER = new OrderDataProvider();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .build();

  private String jsonTopicName;
  private String avroTopicName;
  private String intermediateStream;
  private String resultStream;
  private Multimap<GenericKey, RecordMetadata> producedAvroRecords;
  private Multimap<GenericKey, RecordMetadata> producedJsonRecords;

  @Before
  public void before() {
    intermediateStream = KsqlIdentifierTestUtil.uniqueIdentifierName("int");
    resultStream = KsqlIdentifierTestUtil.uniqueIdentifierName("output");
    jsonTopicName = TopicTestUtil.uniqueTopicName("json");
    avroTopicName = TopicTestUtil.uniqueTopicName("avro");

    TEST_HARNESS.ensureTopics(jsonTopicName, avroTopicName);
    producedJsonRecords = TEST_HARNESS.produceRows(jsonTopicName, DATA_PROVIDER, KAFKA, JSON);
    producedAvroRecords = TEST_HARNESS.produceRows(avroTopicName, DATA_PROVIDER, KAFKA, AVRO);

    createOrdersStream();
  }

  @Test
  public void testTimestampColumnSelectionJson() {
    testTimestampColumnSelection(JSON_STREAM_NAME, JSON, producedJsonRecords);
  }

  @Test
  public void testTimestampColumnSelectionAvro() {
    testTimestampColumnSelection(AVRO_STREAM_NAME, AVRO, producedAvroRecords);
  }

  @Test
  public void testSelectProjectKeyTimestampJson() {
    testSelectProjectKeyTimestamp(JSON_STREAM_NAME, JSON, producedJsonRecords);
  }

  @Test
  public void testSelectProjectKeyTimestampAvro() {
    testSelectProjectKeyTimestamp(AVRO_STREAM_NAME, AVRO, producedAvroRecords);
  }

  @Test
  public void testSelectProjectJson() {
    testSelectProject(JSON_STREAM_NAME, JSON);
  }

  @Test
  public void testSelectProjectAvro() {
    testSelectProject(AVRO_STREAM_NAME, AVRO);
  }

  @Test
  public void testSelectStarJson() {
    testSelectStar(JSON_STREAM_NAME, JSON);
  }

  @Test
  public void testSelectStarAvro() {
    testSelectStar(AVRO_STREAM_NAME, AVRO);
  }

  @Test
  public void testSelectWithFilterJson() {
    testSelectWithFilter(JSON_STREAM_NAME, JSON);
  }

  @Test
  public void testSelectWithFilterAvro() {
    testSelectWithFilter(AVRO_STREAM_NAME, AVRO);
  }

  @Test
  public void shouldSkipBadData() {
    ksqlContext.sql("CREATE STREAM " + intermediateStream + " AS"
        + " SELECT * FROM " + JSON_STREAM_NAME + ";");

    TEST_HARNESS
        .produceRecord(intermediateStream.toUpperCase(), "bad", "something that is not json");

    testSelectWithFilter(intermediateStream, JSON);
  }

  @Test
  public void shouldSkipBadDataAvro() {
    ksqlContext.sql("CREATE STREAM " + intermediateStream + " AS"
        + " SELECT * FROM " + AVRO_STREAM_NAME + ";");

    TEST_HARNESS
        .produceRecord(intermediateStream.toUpperCase(), "bad", "something that is not avro");

    testSelectWithFilter(intermediateStream, AVRO);
  }

  @Test
  public void shouldUseStringTimestampWithFormat() throws Exception {
    ksqlContext.sql("CREATE STREAM " + intermediateStream +
        " WITH (timestamp='TIMESTAMP', timestamp_format='yyyy-MM-dd') AS"
        + " SELECT ORDERTIME, ORDERID, TIMESTAMP FROM " + AVRO_STREAM_NAME + " WHERE ITEMID='ITEM_6';"
        + ""
        + " CREATE STREAM " + resultStream + " AS"
        + " SELECT ORDERTIME, ORDERID, TIMESTAMP from " + intermediateStream + ";");

    final List<ConsumerRecord<byte[], byte[]>> records =
        TEST_HARNESS.verifyAvailableRecords(resultStream.toUpperCase(), 1);

    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    final long timestamp = records.get(0).timestamp();
    assertThat(timestamp, equalTo(dateFormat.parse("2018-01-06").getTime()));
  }

  @Test
  public void shouldUseTimestampExtractedFromDDLStatement() throws Exception {
    ksqlContext.sql("CREATE STREAM " + resultStream + " WITH(timestamp='ordertime')"
        + " AS SELECT ORDERID, ORDERTIME FROM " + AVRO_TIMESTAMP_STREAM_NAME
        + " WHERE ITEMID='ITEM_4';");

    final List<ConsumerRecord<byte[], byte[]>> records =
        TEST_HARNESS.verifyAvailableRecords(resultStream.toUpperCase(), 1);

    final long timestamp = records.get(0).timestamp();
    assertThat(timestamp, is(4L));
  }

  private void testTimestampColumnSelection(
      final String inputStreamName,
      final Format valueFormat,
      final Multimap<GenericKey, RecordMetadata> recordMetadataMap
  ) {
    final String query1String =
        String.format("CREATE STREAM %s WITH (timestamp='RTIME') AS SELECT ORDERTIME, "
                + "ROWTIME+10000 AS "
                + "RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
                + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'; "
                + ""
                + "CREATE STREAM %s AS SELECT ORDERTIME, "
                + "ROWTIME AS NEWRTIME, RTIME, RT100, ORDERID, ITEMID "
                + "FROM %s ;", intermediateStream,
            inputStreamName, resultStream, intermediateStream);

    ksqlContext.sql(query1String);

    final Map<Long, GenericRow> expectedResults = new HashMap<>();
    final RecordMetadata order_6 = Iterables.getLast(recordMetadataMap.get(genericKey("ORDER_6")));
    expectedResults.put(8L,
        genericRow(
            null,
            null,
            "8",
            order_6.timestamp() + 10000,
            "8",
            order_6.timestamp() + 10000,
            order_6.timestamp() + 100,
            "ORDER_6",
            "ITEM_8")
    );

    TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        expectedResults.size(),
        KAFKA,
        valueFormat,
        getResultSchema());
  }

  private void testSelectProjectKeyTimestamp(
      final String inputStreamName,
      final Format valueFormat,
      final Multimap<GenericKey, RecordMetadata> recordMetadataMap
  ) {
    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ORDERID, ORDERTIME, ROWTIME AS RTIME, ITEMID "
        + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = "
        + "'ITEM_8';", resultStream, inputStreamName));

    final List<ConsumerRecord<GenericKey, GenericRow>> results = TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        1,
        KAFKA,
        valueFormat,
        getResultSchema()
    );

    assertThat(results.get(0).key(), is(genericKey("ORDER_6")));
    assertThat(results.get(0).value(),
        is(genericRow(
            8L,
            Iterables.getLast(recordMetadataMap.get(genericKey("ORDER_6"))).timestamp(),
            "ITEM_8")
        )
    );
  }

  private void testSelectProject(
      final String inputStreamName,
      final Format valueFormat
  ) {
    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT ORDERID, ORDERTIME, ITEMID, ORDERUNITS, "
                                  + "PRICEARRAY FROM %s;", resultStream, inputStreamName));

    final List<ConsumerRecord<GenericKey, GenericRow>> results = TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        DATA_PROVIDER.data().size(),
        KAFKA,
        valueFormat,
        getResultSchema());

    final GenericRow value = results.get(0).value();
    assertThat(value.get(1), is("ITEM_1"));
  }


  @Test
  public void testSelectProjectAvroJson() {

    ksqlContext.sql(String.format("CREATE STREAM %s WITH ( value_format = 'JSON') AS SELECT "
                                  + "ORDERID, "
                                  + "ORDERTIME, "
                                  + "ITEMID, "
                                  + "ORDERUNITS, "
        + "PRICEARRAY FROM %s;", resultStream, AVRO_STREAM_NAME));

    final List<ConsumerRecord<GenericKey, GenericRow>> results = TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        DATA_PROVIDER.data().size(),
        KAFKA,
        JSON,
        getResultSchema());

    final GenericRow value = results.get(0).value();
    assertThat(value.get(1), is("ITEM_1"));
  }

  private void testSelectStar(
      final String inputStreamName,
      final Format valueFormat
  ) {
    ksqlContext.sql(String.format("CREATE STREAM %s AS SELECT * FROM %s;",
                                  resultStream,
                                  inputStreamName));

    TEST_HARNESS.verifyAvailableUniqueRows(
        resultStream.toUpperCase(),
        is(DATA_PROVIDER.finalData()),
        KAFKA,
        valueFormat,
        DATA_PROVIDER.schema()
    );
  }

  private void testSelectWithFilter(
      final String inputStreamName,
      final Format valueFormat
  ) {
    ksqlContext.sql("CREATE STREAM " + resultStream + " AS "
        + "SELECT * FROM " + inputStreamName + " WHERE ORDERUNITS > 40;");

    TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        4,
        KAFKA,
        valueFormat,
        DATA_PROVIDER.schema());
  }

  @Test
  public void testInsertIntoJson() {
    givenStreamExists(resultStream, "ORDERID, ORDERTIME, ITEMID, ORDERUNITS, PRICEARRAY", JSON_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream +
        " SELECT ORDERID, ORDERTIME, ITEMID, ORDERUNITS, PRICEARRAY FROM " + JSON_STREAM_NAME + ";");

    final List<ConsumerRecord<GenericKey, GenericRow>> results = TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        DATA_PROVIDER.data().size(),
        KAFKA,
        JSON,
        getResultSchema());

    final GenericRow value = results.get(0).value();
    assertThat(value.get(1), is("ITEM_1"));
  }

  @Test
  public void testInsertIntoAvro() {

    givenStreamExists(resultStream, "ORDERID, ORDERTIME, ITEMID, ORDERUNITS, PRICEARRAY", AVRO_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream + " "
        + "SELECT ORDERID, ORDERTIME, ITEMID, ORDERUNITS, PRICEARRAY FROM " + AVRO_STREAM_NAME + ";");

    final List<ConsumerRecord<GenericKey, GenericRow>> results = TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        DATA_PROVIDER.data().size(),
        KAFKA,
        AVRO,
        getResultSchema());

    final GenericRow value = results.get(0).value();
    assertThat(value.get(1), is("ITEM_1"));
  }

  @Test
  public void testInsertSelectStarJson() {

    givenStreamExists(resultStream, "*", JSON_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream + " SELECT * FROM " + JSON_STREAM_NAME + ";");

    TEST_HARNESS.verifyAvailableUniqueRows(
        resultStream.toUpperCase(),
        is(DATA_PROVIDER.finalData()),
        KAFKA,
        JSON,
        DATA_PROVIDER.schema()
    );
  }

  @Test
  public void testInsertSelectStarAvro() {

    givenStreamExists(resultStream, "*", AVRO_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream + " SELECT * FROM " + AVRO_STREAM_NAME + ";");

    TEST_HARNESS.verifyAvailableUniqueRows(
        resultStream.toUpperCase(),
        is(DATA_PROVIDER.finalData()),
        KAFKA,
        AVRO,
        DATA_PROVIDER.schema()
    );
  }

  @Test
  public void testInsertSelectWithFilterJson() {
    givenStreamExists(resultStream, "*", JSON_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream
        + " SELECT * FROM " + JSON_STREAM_NAME + " WHERE ORDERUNITS > 40;");

    TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        4,
        KAFKA,
        JSON,
        DATA_PROVIDER.schema());
  }

  @Test
  public void testInsertSelectWithFilterAvro() {
    givenStreamExists(resultStream, "*", AVRO_STREAM_NAME);

    ksqlContext.sql("INSERT INTO " + resultStream
        + " SELECT * FROM " + AVRO_STREAM_NAME + " WHERE ORDERUNITS > 40;");

    TEST_HARNESS.verifyAvailableRows(
        resultStream.toUpperCase(),
        4,
        KAFKA,
        AVRO,
        DATA_PROVIDER.schema());
  }

  private PhysicalSchema getResultSchema() {
    final DataSource source = ksqlContext
        .getMetaStore()
        .getSource(SourceName.of(resultStream.toUpperCase()));

    return PhysicalSchema.from(source.getSchema(), SerdeFeatures.of(), SerdeFeatures.of());
  }

  private void createOrdersStream() {
    final String columns = DATA_PROVIDER.ksqlSchemaString(false);

    ksqlContext.sql("CREATE STREAM " + JSON_STREAM_NAME + " (" + columns + ") WITH "
        + "(kafka_topic='" + jsonTopicName + "', value_format='JSON');");

    ksqlContext.sql("CREATE STREAM " + AVRO_STREAM_NAME + " (" + columns + ") WITH "
        + "(kafka_topic='" + avroTopicName + "', value_format='AVRO');");

    ksqlContext.sql("CREATE STREAM " + AVRO_TIMESTAMP_STREAM_NAME + " (" + columns + ") WITH "
        + "(kafka_topic='" + avroTopicName + "', value_format='AVRO', "
        + "timestamp='timestamp', timestamp_format='yyyy-MM-dd');");
  }

  private void givenStreamExists(
      final String streamName,
      final String selectColumns,
      final String sourceStream
  ) {
    ksqlContext.sql(
        "CREATE STREAM " + streamName + " AS SELECT " + selectColumns + " FROM " + sourceStream
            + " WHERE ITEMID = 'will not find me';");
  }
}