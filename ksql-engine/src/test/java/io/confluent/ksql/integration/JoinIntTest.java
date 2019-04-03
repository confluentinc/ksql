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

import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.AVRO;
import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.JSON;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.TestUtils;
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
  public final TestKsqlContext ksqlContext = TEST_HARNESS.buildKsqlContext();

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

    TEST_HARNESS.produceRows(itemTableTopicJson, ITEM_DATA_PROVIDER, JSON, () -> now - 500);
    TEST_HARNESS.produceRows(itemTableTopicAvro, ITEM_DATA_PROVIDER, AVRO, () -> now - 500);

    TEST_HARNESS.produceRows(orderStreamTopicJson, ORDER_DATA_PROVIDER, JSON, () -> now);
    TEST_HARNESS.produceRows(orderStreamTopicAvro, ORDER_DATA_PROVIDER, AVRO, () -> now);

    createStreams();
  }

  private void shouldLeftJoinOrderAndItems(final String testStreamName,
                                          final String orderStreamTopic,
                                          final String orderStreamName,
                                          final String itemTableName,
                                          final DataSource.DataSourceSerDe dataSourceSerDe)
      throws Exception {

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN"
            + " %s on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1' ;",
            testStreamName,
            orderStreamName,
            itemTableName,
            orderStreamName,
            itemTableName,
            orderStreamName);

    ksqlContext.sql(queryString);

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("ITEM_1",
                                 new GenericRow(Arrays.asList(
                                     null,
                                     null,
                                     "ORDER_1",
                                     "ITEM_1",
                                     10.0,
                                     "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
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
        }, MAX_WAIT_MS,
        "failed to complete join correctly");
  }

  @Test
  public void shouldInsertLeftJoinOrderAndItems() throws Exception {
    final String testStreamName = "OrderedWithDescription".toUpperCase();

    final String csasQueryString = String.format(
        "CREATE STREAM %s AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN "
        + "%s " +
        " on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'Hello' ;",
        testStreamName,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        ORDER_STREAM_NAME_JSON
    );

    final String insertQueryString = String.format(
        "INSERT INTO %s SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN "
        + "%s " +
        " on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1' ;",
        testStreamName,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        ORDER_STREAM_NAME_JSON
    );

    ksqlContext.sql(csasQueryString);
    ksqlContext.sql(insertQueryString);

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    final Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList(null, null, "ORDER_1", "ITEM_1", 10.0, "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
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
    }, MAX_WAIT_MS, "failed to complete join correctly");
  }

  @Test
  public void shouldLeftJoinOrderAndItemsJson() throws Exception {
    shouldLeftJoinOrderAndItems(
        "ORDERWITHDESCRIPTIONJSON",
        orderStreamTopicJson,
        ORDER_STREAM_NAME_JSON,
        ITEM_TABLE_NAME_JSON,
        DataSource.DataSourceSerDe.JSON);

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
    final String queryString = String.format(
        "CREATE STREAM JOINED AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN"
            + " %s on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1';"
            + "CREATE STREAM OUTPUT AS SELECT ORDERID, DESCRIPTION, ROWTIME AS RT FROM JOINED;",
        ORDER_STREAM_NAME_AVRO,
        ITEM_TABLE_NAME_AVRO,
        ORDER_STREAM_NAME_AVRO,
        ITEM_TABLE_NAME_AVRO,
        ORDER_STREAM_NAME_AVRO);

    ksqlContext.sql(queryString);

    final String outputStream = "OUTPUT";
    final Schema resultSchema = ksqlContext.getMetaStore().getSource(outputStream).getSchema();

    final Map<String, GenericRow> expectedResults =
        Collections.singletonMap("ITEM_1",
            new GenericRow(Arrays.asList(
                null,
                null,
                "ORDER_1",
                "home cinema",
                1)));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      results.putAll(TEST_HARNESS.verifyAvailableUniqueRows(
          outputStream,
          1,
          AVRO,
          resultSchema));

      final boolean success = results.equals(expectedResults);
      if (!success) {
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
    }, 120000, "failed to complete join correctly");
  }

  private void createStreams() {
    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
            + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
            + "WITH (kafka_topic='%s', value_format='JSON');",
        ORDER_STREAM_NAME_JSON,
        orderStreamTopicJson));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar, DESCRIPTION varchar) "
            + "WITH (kafka_topic='%s', value_format='JSON', key='ID');",
        ITEM_TABLE_NAME_JSON,
        itemTableTopicJson));

    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, "
            + "ORDERUNITS double, PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='%s', value_format='AVRO', timestamp='ORDERTIME');",
        ORDER_STREAM_NAME_AVRO,
        orderStreamTopicAvro));

    ksqlContext.sql(String.format(
        "CREATE TABLE %s (ID varchar, DESCRIPTION varchar)"
            + " WITH (kafka_topic='%s', value_format='AVRO', key='ID');",
        ITEM_TABLE_NAME_AVRO,
        itemTableTopicAvro));
  }
}
