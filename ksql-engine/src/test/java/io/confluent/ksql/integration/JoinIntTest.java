/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.KsqlTestContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({IntegrationTest.class})
public class JoinIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;


  private String orderStreamTopicJson = "OrderTopicJson";
  private String orderStreamNameJson = "Orders_json";
  private OrderDataProvider orderDataProvider;

  private String orderStreamTopicAvro = "OrderTopicAvro";
  private String orderStreamNameAvro = "Orders_avro";

  private String itemTableTopicJson = "ItemTopicJson";
  private String itemTableNameJson = "Item_json";
  private ItemDataProvider itemDataProvider;

  private String itemTableTopicAvro = "ItemTopicAvro";
  private String itemTableNameAvro = "Item_avro";

  private final long now = System.currentTimeMillis();

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start(Collections.emptyMap());
    final Map<String, Object> ksqlStreamConfigProps = new HashMap<>();
    ksqlStreamConfigProps.putAll(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    // turn caching off to improve join consistency
    ksqlStreamConfigProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    ksqlStreamConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    ksqlContext = KsqlTestContext.create(new KsqlConfig(ksqlStreamConfigProps),
                                     testHarness.schemaRegistryClientFactory);

    /*
     * Setup test data
     */
    testHarness.createTopic(itemTableTopicJson);
    testHarness.createTopic(itemTableTopicAvro);
    itemDataProvider = new ItemDataProvider();

    testHarness.publishTestData(itemTableTopicJson,
                                itemDataProvider,
                                now -500);
    testHarness.publishTestData(itemTableTopicAvro,
                                itemDataProvider,
                                now -500,
                                DataSource.DataSourceSerDe.AVRO);


    testHarness.createTopic(orderStreamTopicJson);
    testHarness.createTopic(orderStreamTopicAvro);
    orderDataProvider = new OrderDataProvider();
    testHarness.publishTestData(orderStreamTopicJson,
                                orderDataProvider,
                                now);
    testHarness.publishTestData(orderStreamTopicAvro,
                                orderDataProvider,
                                now,
                                DataSource.DataSourceSerDe.AVRO);
    createStreams();
  }

  @After
  public void after() {
    ksqlContext.close();
    testHarness.stop();
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
      results.putAll(testHarness.consumeData(testStreamName,
                                             resultSchema,
                                             1,
                                             new StringDeserializer(),
                                             IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
                                             dataSourceSerDe));
      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopic, orderDataProvider, now, dataSourceSerDe);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS * 2 + 30000,
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
        orderStreamNameJson,
        itemTableNameJson,
        orderStreamNameJson,
        itemTableNameJson,
        orderStreamNameJson
    );

    final String insertQueryString = String.format(
        "INSERT INTO %s SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN "
        + "%s " +
        " on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1' ;",
        testStreamName,
        orderStreamNameJson,
        itemTableNameJson,
        orderStreamNameJson,
        itemTableNameJson,
        orderStreamNameJson
    );

    ksqlContext.sql(csasQueryString);
    ksqlContext.sql(insertQueryString);

    final Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    final Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList(null, null, "ORDER_1", "ITEM_1", 10.0, "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      results.putAll(testHarness.consumeData(testStreamName, resultSchema, 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS));
      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopicJson, orderDataProvider, now);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS * 2 + 30000, "failed to complete join correctly");
  }

  @Test
  public void shouldLeftJoinOrderAndItemsJson() throws Exception {
    shouldLeftJoinOrderAndItems("ORDERWITHDESCRIPTIONJSON",
                                orderStreamTopicJson,
                                orderStreamNameJson,
                                itemTableNameJson,
                                DataSource.DataSourceSerDe.JSON);

  }

  @Test
  public void shouldLeftJoinOrderAndItemsAvro() throws Exception {
    shouldLeftJoinOrderAndItems("ORDERWITHDESCRIPTIONAVRO",
                                orderStreamTopicAvro,
                                orderStreamNameAvro,
                                itemTableNameAvro,
                                DataSource.DataSourceSerDe.AVRO);
  }

  @Test
  public void shouldUseTimeStampFieldFromStream() throws Exception {
    final String queryString = String.format(
        "CREATE STREAM JOINED AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN"
            + " %s on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1';"
            + "CREATE STREAM OUTPUT AS SELECT ORDERID, DESCRIPTION, ROWTIME AS RT FROM JOINED;",
        orderStreamNameAvro,
        itemTableNameAvro,
        orderStreamNameAvro,
        itemTableNameAvro,
        orderStreamNameAvro);

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
      results.putAll(testHarness.consumeData(outputStream,
          resultSchema,
          1,
          new StringDeserializer(),
          IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
          DataSource.DataSourceSerDe.AVRO));
      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopicAvro, orderDataProvider, now, DataSource.DataSourceSerDe.AVRO);
        } catch(final Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, 120000, "failed to complete join correctly");

  }

  private void createStreams() {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                  + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, "
                                  + "KEYVALUEMAP map<varchar, double>) "
                                  + "WITH (kafka_topic='%s', value_format='JSON');",
                                  orderStreamNameJson,
                                  orderStreamTopicJson));
    ksqlContext.sql(String.format("CREATE TABLE %s (ID varchar, DESCRIPTION varchar) "
                                  + "WITH (kafka_topic='%s', value_format='JSON', key='ID');",
                                  itemTableNameJson,
                                  itemTableTopicJson));

    ksqlContext.sql(String.format(
        "CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, "
                                  + "ORDERUNITS double, PRICEARRAY array<double>, "
                                  + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='%s', value_format='AVRO', timestamp='ORDERTIME');",
                                  orderStreamNameAvro,
                                  orderStreamTopicAvro));
    ksqlContext.sql(String.format("CREATE TABLE %s (ID varchar, DESCRIPTION varchar)"
                                  + " WITH (kafka_topic='%s', value_format='AVRO', key='ID');",
                                  itemTableNameAvro,
                                  itemTableTopicAvro));
  }

}
