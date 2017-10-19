package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.OrderDataProvider;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
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
public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;
  private final String topicName = "TestTopic";
  private OrderDataProvider dataProvider;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    testHarness.createTopic(topicName);

    /**
     * Setup test data
     */
    dataProvider = new OrderDataProvider();
    recordMetadataMap = testHarness.publishTestData(topicName, dataProvider, null );
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
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
                    "ORDERS", stream2Name, stream1Name);


    ksqlContext.sql(query1String);

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", recordMetadataMap.get("8")
                    .timestamp() + 10000, "8", recordMetadataMap.get("8").timestamp() + 10000,
            recordMetadataMap.get("8").timestamp
                    () + 100, "ORDER_6", "ITEM_8")));

    Schema resultSchema = ksqlContext.getMetaStore().getSource(stream2Name).getSchema();

    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema , expectedResults.size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(results2, equalTo(expectedResults));
  }


  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {

    ksqlContext.sql("CREATE STREAM PROJECT_KEY_TIMESTAMP AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID FROM ORDERS WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_KEY_TIMESTAMP").getSchema();

    Map<String, GenericRow> results = testHarness.consumeData("PROJECT_KEY_TIMESTAMP", resultSchema , dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    Map<String, GenericRow> expectedResults = Collections.singletonMap("8", new GenericRow(Arrays.asList(null, null, "8", recordMetadataMap.get("8").timestamp(), "ITEM_8")));

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testSelectProject() throws Exception {

    ksqlContext.sql("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM ORDERS;");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM", resultSchema, dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectStar() throws Exception {

    ksqlContext.sql("CREATE STREAM EASYORDERS AS SELECT * FROM orders;");

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("EASYORDERS", dataProvider.schema(), dataProvider.data().size(), new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(easyOrdersData, equalTo(dataProvider.data()));
  }


  @Test
  public void testSelectWithFilter() throws Exception {

    ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE ORDERUNITS > 40;");

    Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS", dataProvider.schema(), 4, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    Assert.assertEquals(4, results.size());
  }

  @Test
  public void shouldSkipBadData() throws Exception {
    testHarness.createTopic(topicName);
    testHarness.produceRecord(topicName, "bad", "something that is not json");
    testSelectWithFilter();
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }

}
