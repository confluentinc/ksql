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
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class JoinIntTest {

  private static final String ORDER_STREAM_NAME_JSON = "Orders_json";
  private static final String ORDER_STREAM_NAME_AVRO = "Orders_avro";
  private static final String ITEM_TABLE_NAME_JSON = "Item_json";
  private static final String ITEM_TABLE_NAME_AVRO = "Item_avro";
  private static final ItemDataProvider ITEM_DATA_PROVIDER = new ItemDataProvider();
  private static final OrderDataProvider ORDER_DATA_PROVIDER = new OrderDataProvider();
  private static final long MAX_WAIT_MS = TimeUnit.SECONDS.toMillis(150);

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .build();

  private final long now = System.currentTimeMillis();
  private String itemTableTopicJson = "ItemTopicJson";
  private String orderStreamTopicJson = "OrderTopicJson";
  private String orderStreamTopicAvro = "OrderTopicAvro";
  private String itemTableTopicAvro = "ItemTopicAvro";

  @Before
  public void before() {
    itemTableTopicJson = TopicTestUtil.uniqueTopicName("ItemTopicJson");
    itemTableTopicAvro = TopicTestUtil.uniqueTopicName("ItemTopicAvro");
    orderStreamTopicJson = TopicTestUtil.uniqueTopicName("OrderTopicJson");
    orderStreamTopicAvro = TopicTestUtil.uniqueTopicName("OrderTopicAvro");

    TEST_HARNESS.ensureTopics(4, itemTableTopicJson, itemTableTopicAvro,
        orderStreamTopicJson, orderStreamTopicAvro);

    // we want the table events to always be present (less than the ts in the stream
    // including the time extractor)
    TEST_HARNESS.produceRows(itemTableTopicJson, ITEM_DATA_PROVIDER, KAFKA, JSON, () -> 0L);
    TEST_HARNESS.produceRows(itemTableTopicAvro, ITEM_DATA_PROVIDER, KAFKA, AVRO, () -> 0L);

    TEST_HARNESS.produceRows(orderStreamTopicJson, ORDER_DATA_PROVIDER, KAFKA, JSON, () -> now);
    TEST_HARNESS.produceRows(orderStreamTopicAvro, ORDER_DATA_PROVIDER, KAFKA, AVRO, () -> now);

    createStreams();
  }

  private void shouldLeftJoinOrderAndItems(final String testStreamName,
                                          final String orderStreamTopic,
                                          final String orderStreamName,
                                          final String itemTableName,
                                          final Format valueFormat) {

    final String queryString =
            "CREATE STREAM " + testStreamName + " AS "
                + "SELECT " + orderStreamName + ".ITEMID, ORDERID, ORDERUNITS, DESCRIPTION "
                + "FROM " + orderStreamName + " LEFT JOIN " + itemTableName + " "
                + "on " + orderStreamName + ".ITEMID = " + itemTableName + ".ID "
                + "WHERE " + orderStreamName + ".ITEMID = 'ITEM_1' ;";

    ksqlContext.sql(queryString);

    final DataSource source = ksqlContext.getMetaStore()
        .getSource(SourceName.of(testStreamName));

    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap.of(
        genericKey("ITEM_1"),
        genericRow("ORDER_1", 10.0, "home cinema")
    );

    assertExpectedResults(expectedResults, source, orderStreamTopic, KAFKA, valueFormat);
  }

  @Test
  public void shouldInsertLeftJoinOrderAndItems() {
    final String testStreamName = "OrderedWithDescription".toUpperCase();

    final String commonSql =
        "SELECT " + ORDER_STREAM_NAME_JSON + ".ITEMID, ORDERID, ORDERUNITS, DESCRIPTION "
            + "FROM " + ORDER_STREAM_NAME_JSON + " LEFT JOIN " + ITEM_TABLE_NAME_JSON + " "
            + " on " + ORDER_STREAM_NAME_JSON + ".ITEMID = " + ITEM_TABLE_NAME_JSON + ".ID ";

    final String csasQueryString = "CREATE STREAM " + testStreamName + " AS "
        + commonSql
        + "WHERE " + ORDER_STREAM_NAME_JSON + ".ITEMID = 'Hello' ;";

    final String insertQueryString =
        "INSERT INTO " + testStreamName + " "
            + commonSql
            + "WHERE " + ORDER_STREAM_NAME_JSON + ".ITEMID = 'ITEM_1' ;";

    ksqlContext.sql(csasQueryString);
    ksqlContext.sql(insertQueryString);

    final DataSource source = ksqlContext.getMetaStore()
        .getSource(SourceName.of(testStreamName));

    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap.of(
        genericKey("ITEM_1"),
        genericRow("ORDER_1", 10.0, "home cinema")
    );

    assertExpectedResults(expectedResults, source, orderStreamTopicJson, KAFKA, JSON);
  }

  @Test
  public void shouldLeftJoinOrderAndItemsJson() {
    shouldLeftJoinOrderAndItems(
        "ORDERWITHDESCRIPTIONJSON",
        orderStreamTopicJson,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        FormatFactory.JSON);

  }

  @Test
  public void shouldLeftJoinOrderAndItemsAvro() {
    shouldLeftJoinOrderAndItems(
        "ORDERWITHDESCRIPTIONAVRO",
        orderStreamTopicAvro,
        ORDER_STREAM_NAME_AVRO,
        ITEM_TABLE_NAME_AVRO,
        AVRO);
  }

  @Test
  public void shouldUseTimeStampFieldFromStream() {
    final String queryString = "CREATE STREAM JOINED AS "
        + "SELECT " + ORDER_STREAM_NAME_AVRO + ".ITEMID, ORDERID, ORDERUNITS, DESCRIPTION "
        + "FROM " + ORDER_STREAM_NAME_AVRO + " LEFT JOIN " + ITEM_TABLE_NAME_AVRO + " "
        + "ON " + ORDER_STREAM_NAME_AVRO + ".ITEMID = " + ITEM_TABLE_NAME_AVRO + ".ID "
        + "WHERE "  + ORDER_STREAM_NAME_AVRO + ".ITEMID = 'ITEM_1';"
        + ""
        + "CREATE STREAM OUTPUT AS "
        + "SELECT ITEMID, ORDERID, DESCRIPTION, ROWTIME AS RT "
        + "FROM JOINED;";

    ksqlContext.sql(queryString);

    final DataSource source = ksqlContext.getMetaStore()
        .getSource(SourceName.of("OUTPUT"));

    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap.of(
        genericKey("ITEM_1"),
        genericRow("ORDER_1", "home cinema", 1L)
    );

    assertExpectedResults(expectedResults, source, orderStreamTopicAvro, KAFKA, AVRO, 120000);
  }

  private void assertExpectedResults(
      final Map<GenericKey, GenericRow> expectedResults,
      final DataSource outputSource,
      final String inputTopic,
      final Format inputKeyFormat,
      final Format inputValueFormat
  ) {
    assertExpectedResults(expectedResults, outputSource, inputTopic, inputKeyFormat, inputValueFormat, MAX_WAIT_MS);
  }

  private void assertExpectedResults(
      final Map<GenericKey, GenericRow> expectedResults,
      final DataSource outputSource,
      final String inputTopic,
      final Format inputKeyFormat,
      final Format inputValueFormat,
      final long timeoutMs
  ) {
    final PhysicalSchema schema = PhysicalSchema.from(
        outputSource.getSchema(),
        outputSource.getKsqlTopic().getKeyFormat().getFeatures(),
        outputSource.getKsqlTopic().getValueFormat().getFeatures()
    );

    final Map<GenericKey, GenericRow> results = new HashMap<>();
    assertThatEventually("failed to complete join correctly", () -> {
      results.putAll(TEST_HARNESS.verifyAvailableUniqueRows(
          outputSource.getKafkaTopicName(),
          1,
          FormatFactory.of(outputSource.getKsqlTopic().getKeyFormat().getFormatInfo()),
          FormatFactory.of(outputSource.getKsqlTopic().getValueFormat().getFormatInfo()),
          schema));

      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          TEST_HARNESS.produceRows(inputTopic, ORDER_DATA_PROVIDER, inputKeyFormat, inputValueFormat, () -> now);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, is(true), timeoutMs, TimeUnit.MILLISECONDS);

    assertThat(results, is(expectedResults));
  }

  private void createStreams() {
    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERID varchar KEY, ORDERTIME bigint, "
            + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
            + "WITH (kafka_topic='%s', key_format='KAFKA', value_format='JSON');",
        ORDER_STREAM_NAME_JSON,
        orderStreamTopicJson));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar PRIMARY KEY, DESCRIPTION varchar) "
            + "WITH (kafka_topic='%s', key_format='KAFKA', value_format='JSON');",
        ITEM_TABLE_NAME_JSON,
        itemTableTopicJson));

    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERID varchar KEY, ORDERTIME bigint, ITEMID varchar, "
            + "ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='%s', key_format='KAFKA', value_format='AVRO', timestamp='ORDERTIME');",
        ORDER_STREAM_NAME_AVRO,
        orderStreamTopicAvro));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar PRIMARY KEY, DESCRIPTION varchar)"
            + " WITH (kafka_topic='%s', key_format='DELIMITED', value_format='AVRO');",
        ITEM_TABLE_NAME_AVRO,
        itemTableTopicAvro));
  }
}
