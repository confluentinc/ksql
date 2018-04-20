package io.confluent.ksql.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


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
    testHarness.start();
    Map<String, Object> ksqlStreamConfigProps = new HashMap<>();
    ksqlStreamConfigProps.putAll(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    // turn caching off to improve join consistency
    ksqlStreamConfigProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    ksqlStreamConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    ksqlContext = KsqlContext.create(new KsqlConfig(ksqlStreamConfigProps),
                                     testHarness.schemaRegistryClient);

    /**
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
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }


  private void shouldLeftJoinOrderAndItems(String testStreamName,
                                          String orderStreamTopic,
                                          String orderStreamName,
                                          String itemTableName,
                                          DataSource.DataSourceSerDe dataSourceSerDe)
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

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults =
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
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS * 2 + 30000,
        "failed to complete join correctly");
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
    Schema resultSchema = ksqlContext.getMetaStore().getSource(outputStream).getSchema();

    Map<String, GenericRow> expectedResults =
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
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, 120000, "failed to complete join correctly");

  }

  private void createStreams() throws Exception {
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
