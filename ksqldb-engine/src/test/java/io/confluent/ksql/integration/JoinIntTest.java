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

import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
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
import java.util.concurrent.atomic.AtomicInteger;
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

    TEST_HARNESS.ensureTopics(itemTableTopicJson, itemTableTopicAvro,
        orderStreamTopicJson, orderStreamTopicAvro);

    // we want the table events to always be present (less than the ts in the stream
    // including the time extractor)
    TEST_HARNESS.produceRows(itemTableTopicJson, ITEM_DATA_PROVIDER, JSON, () -> 0L);
    TEST_HARNESS.produceRows(itemTableTopicAvro, ITEM_DATA_PROVIDER, AVRO, () -> 0L);

    TEST_HARNESS.produceRows(orderStreamTopicJson, ORDER_DATA_PROVIDER, JSON, () -> now);
    TEST_HARNESS.produceRows(orderStreamTopicAvro, ORDER_DATA_PROVIDER, AVRO, () -> now);

    createStreams();
  }

  private void shouldLeftJoinOrderAndItems(final String testStreamName,
                                          final String orderStreamTopic,
                                          final String orderStreamName,
                                          final String itemTableName,
                                          final Format dataSourceSerDe)
      throws Exception {

    final String queryString =
            "CREATE STREAM " + testStreamName + " AS "
                + "SELECT " + orderStreamName + ".ITEMID, ORDERID, ORDERUNITS, DESCRIPTION "
                + "FROM " + orderStreamName + " LEFT JOIN " + itemTableName + " "
                + "on " + orderStreamName + ".ITEMID = " + itemTableName + ".ID "
                + "WHERE " + orderStreamName + ".ITEMID = 'ITEM_1' ;";

    ksqlContext.sql(queryString);

    final DataSource source = ksqlContext.getMetaStore()
        .getSource(SourceName.of(testStreamName));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getSerdeOptions()
    );
    final Map<String, GenericRow> expectedResults = ImmutableMap.of(
        "ITEM_1",
        genericRow("ORDER_1", 10.0, "home cinema")
    );

    final Map<String, GenericRow> results = new HashMap<>();
    assertThatEventually("failed to complete join correctly", () -> {
          results.putAll(TEST_HARNESS.verifyAvailableUniqueRows(
              testStreamName,
              1,
              dataSourceSerDe,
              resultSchema));

      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          TEST_HARNESS
              .produceRows(orderStreamTopic, ORDER_DATA_PROVIDER, dataSourceSerDe, () -> now);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
        }, is(true), MAX_WAIT_MS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldInsertLeftJoinOrderAndItems() throws Exception {
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

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getSerdeOptions()
    );

    final Map<String, GenericRow> expectedResults = ImmutableMap.of(
        "ITEM_1",
        genericRow("ORDER_1", 10.0, "home cinema")
    );

    final Map<String, GenericRow> results = new HashMap<>();
    assertThatEventually("failed to complete join correctly", () -> {
      results.putAll(TEST_HARNESS.verifyAvailableUniqueRows(
          testStreamName,
          1,
          JSON,
          resultSchema));

      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          TEST_HARNESS.produceRows(orderStreamTopicJson, ORDER_DATA_PROVIDER, JSON, () -> now);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, is(true), MAX_WAIT_MS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void shouldLeftJoinOrderAndItemsJson() throws Exception {
    shouldLeftJoinOrderAndItems(
        "ORDERWITHDESCRIPTIONJSON",
        orderStreamTopicJson,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        FormatFactory.JSON);

  }

  @Test
  public void shouldLeftJoinOrderAndItemsAvro() throws Exception {
    shouldLeftJoinOrderAndItems(
        "ORDERWITHDESCRIPTIONAVRO",
        orderStreamTopicAvro,
        ORDER_STREAM_NAME_AVRO,
        ITEM_TABLE_NAME_AVRO,
        AVRO);
  }

  @Test
  public void shouldUseTimeStampFieldFromStream() throws Exception {
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

    final String outputStream = "OUTPUT";

    final DataSource source = ksqlContext.getMetaStore()
        .getSource(SourceName.of(outputStream));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getSerdeOptions()
    );

    final Map<String, GenericRow> expectedResults = ImmutableMap.of(
        "ITEM_1",
        genericRow("ORDER_1", "home cinema", 1L)
    );

    final AtomicInteger attempts = new AtomicInteger();
    final Map<String, GenericRow> results = new HashMap<>();
    assertThatEventually("failed to complete join correctly", () -> {
      results.putAll(TEST_HARNESS.verifyAvailableUniqueRows(
          outputStream,
          1,
          AVRO,
          resultSchema));

      final boolean success = results.equals(expectedResults);
      if (!success) {
        final int failedAttempts = attempts.getAndIncrement();
        if (failedAttempts >= 5) {
          return true;
        }
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          TEST_HARNESS.produceRows(orderStreamTopicAvro, ORDER_DATA_PROVIDER, AVRO, () -> now);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, is(true), 120000, TimeUnit.MILLISECONDS);

    assertThat(results, is(expectedResults));
  }

  private void createStreams() {
    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERID varchar KEY, ORDERTIME bigint, "
            + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
            + "WITH (kafka_topic='%s', value_format='JSON');",
        ORDER_STREAM_NAME_JSON,
        orderStreamTopicJson));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar PRIMARY KEY, DESCRIPTION varchar) "
            + "WITH (kafka_topic='%s', value_format='JSON');",
        ITEM_TABLE_NAME_JSON,
        itemTableTopicJson));

    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERID varchar KEY, ORDERTIME bigint, ITEMID varchar, "
            + "ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='%s', value_format='AVRO', timestamp='ORDERTIME');",
        ORDER_STREAM_NAME_AVRO,
        orderStreamTopicAvro));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar PRIMARY KEY, DESCRIPTION varchar)"
            + " WITH (kafka_topic='%s', value_format='AVRO');",
        ITEM_TABLE_NAME_AVRO,
        itemTableTopicAvro));
  }
}
