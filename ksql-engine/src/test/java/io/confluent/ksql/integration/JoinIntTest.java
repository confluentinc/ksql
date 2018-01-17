package io.confluent.ksql.integration;

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
import org.apache.kafka.test.IntegrationTest;
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


  private String orderStreamTopic = "OrderTopic";
  private String orderStreamName = "Orders_json";
  private OrderDataProvider orderDataProvider;
  private Map<String, RecordMetadata> orderRecordMetadataMap;

  private String orderStreamTopicAvro = "OrderTopicAvro";
  private String orderStreamNameAvro = "Orders_avro";
  private Map<String, RecordMetadata> orderRecordMetadataMapAvro;

  private String itemTableTopic = "ItemTopic";
  private String itemTableName = "Item_json";
  private ItemDataProvider itemDataProvider;
  private Map<String, RecordMetadata> itemRecordMetadataMap;

  private String itemTableTopicAvro = "ItemTopicAvro";
  private String itemTableNameAvro = "Item_avro";
  private Map<String, RecordMetadata> itemRecordMetadataMapAvro;

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
//    ksqlContext = KsqlContext.create(new KsqlConfig(ksqlStreamConfigProps));
    ksqlContext = KsqlContext.create(new KsqlConfig(ksqlStreamConfigProps), testHarness.schemaRegistryClient);

    /**
     * Setup test data
     */
    testHarness.createTopic(itemTableTopic);
    testHarness.createTopic(itemTableTopicAvro);
    itemDataProvider = new ItemDataProvider();

    itemRecordMetadataMap = testHarness.publishTestData(itemTableTopic, itemDataProvider, now -500);
    itemRecordMetadataMapAvro = testHarness.publishTestData(itemTableTopicAvro, itemDataProvider, now -500, DataSource.DataSourceSerDe.AVRO.name());


    testHarness.createTopic(orderStreamTopic);
    testHarness.createTopic(orderStreamTopicAvro);
    orderDataProvider = new OrderDataProvider();
    orderRecordMetadataMap = testHarness.publishTestData(orderStreamTopic, orderDataProvider, now);
    orderRecordMetadataMapAvro = testHarness.publishTestData(orderStreamTopicAvro, orderDataProvider, now, DataSource.DataSourceSerDe.AVRO.name());
    createStreams();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void shouldLeftJoinOrderAndItems() throws Exception {
    final String testStreamName = "OrderedWithDescription".toUpperCase();

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN"
            + " %s " +
                    " on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1' ;",
            testStreamName, orderStreamName, itemTableName, orderStreamName, itemTableName, orderStreamName);

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList(null, null, "ORDER_1", "ITEM_1", 10.0, "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      results.putAll(testHarness.consumeData(testStreamName, resultSchema, 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS));
      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopic, orderDataProvider, now);
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, 60000, "failed to complete join correctly");
  }


  @Test
  public void shouldLeftJoinOrderAndItemsJsonAvro() throws Exception {
    final String testStreamName = "OrderedWithDescriptionAvro".toUpperCase();

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM %s LEFT JOIN"
        + " %s on %s.ITEMID = %s.ID WHERE %s.ITEMID = 'ITEM_1' ;",
        testStreamName, orderStreamNameAvro, itemTableName, orderStreamNameAvro, itemTableName,
        orderStreamNameAvro);

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList(null, null, "ORDER_1", "ITEM_1", 10.0, "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      results.putAll(testHarness.consumeData(testStreamName, resultSchema, 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS, DataSource.DataSourceSerDe.AVRO.name()));
      final boolean success = results.equals(expectedResults);
      if (!success) {
        try {
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopicAvro, orderDataProvider, now, DataSource.DataSourceSerDe.AVRO.name());
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      return success;
    }, 60000, "failed to complete join correctly");
  }


  private void createStreams() throws Exception {
    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                           + "varchar, "
                     + "ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, "
                                  + "double>) WITH (kafka_topic='%s', "
                                  + "value_format='JSON');", orderStreamName, orderStreamTopic));
    ksqlContext.sql(String.format("CREATE TABLE %s (ID varchar, DESCRIPTION varchar) WITH "
                         + "(kafka_topic='%s', value_format='JSON', key='ID');", itemTableName,
                                  itemTableTopic));

    ksqlContext.sql(String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, ITEMID "
                                  + "varchar, "
                                  + "ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, "
                                  + "double>) WITH (kafka_topic='%s', "
                                  + "value_format='AVRO');", orderStreamNameAvro,
                                  orderStreamTopicAvro));
    ksqlContext.sql(String.format("CREATE TABLE %s (ID varchar, DESCRIPTION varchar) WITH "
                                  + "(kafka_topic='%s', value_format='AVRO', key='ID');",
                                  itemTableNameAvro,
                                  itemTableTopicAvro));
  }

}
