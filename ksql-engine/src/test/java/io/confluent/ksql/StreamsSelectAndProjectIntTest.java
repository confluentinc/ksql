package io.confluent.ksql;

import io.confluent.ksql.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedResults;
import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedWindowedResults;

public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;
  private final String topicName = "TestTopic";

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = new KsqlContext(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    testHarness.createTopic(topicName);
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testTimestampColumnSelection() throws Exception {

    publishOrdersTopicData();

    createOrdersStream();

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

    Map<String, GenericRow> results1 = testHarness.consumeData(stream1Name, resultSchema , expectedResults.size(), new StringDeserializer());
    Map<String, GenericRow> results2 = testHarness.consumeData(stream2Name, resultSchema , expectedResults.size(), new StringDeserializer());

    System.out.println("Results:" + results1);

    Assert.assertEquals(expectedResults.size(), results2.size());
    assertExpectedResults(expectedResults, results2);
  }


  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();

    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_KEY_TIMESTAMP AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID FROM ORDERS WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_KEY_TIMESTAMP").getSchema();

    Map<String, GenericRow> results = testHarness.consumeData("PROJECT_KEY_TIMESTAMP", resultSchema , dataProvider.data().size(), new StringDeserializer());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "8", recordMetadataMap.get("8").timestamp(), "ITEM_8")));

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testSelectProject() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();
    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM ORDERS;");

    Schema resultSchema = ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema();

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM", resultSchema, dataProvider.data().size(), new StringDeserializer());

    GenericRow value = easyOrdersData.values().iterator().next();
    // skip over first to values (rowKey, rowTime)
    Assert.assertEquals( "ITEM_1", value.getColumns().get(2));
  }


  @Test
  public void testSelectStar() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();
    createOrdersStream();
    ksqlContext.sql("CREATE STREAM EASYORDERS AS SELECT * FROM orders;");

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("EASYORDERS", dataProvider.schema(), dataProvider.data().size(), new StringDeserializer());

    testHarness.assertExpectedResults(dataProvider.data(), easyOrdersData);
  }


  @Test
  public void testSelectWithFilter() throws Exception {

    OrderDataProvider orderDataProvider = publishOrdersTopicData();

    createOrdersStream();

    ksqlContext.sql("CREATE STREAM bigorders AS SELECT * FROM orders WHERE ORDERUNITS > 40;");

    Map<String, GenericRow> results = testHarness.consumeData("BIGORDERS", orderDataProvider.schema(), 4, new StringDeserializer());

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

  private OrderDataProvider publishOrdersTopicData() throws InterruptedException, TimeoutException, ExecutionException {
    OrderDataProvider dataProvider = new OrderDataProvider();
    recordMetadataMap = testHarness.produceData(topicName, dataProvider.data(), dataProvider.schema());
    return dataProvider;
  }
}
