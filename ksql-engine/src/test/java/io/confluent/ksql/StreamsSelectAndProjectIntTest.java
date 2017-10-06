package io.confluent.ksql;

import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedResults;

public class StreamsSelectAndProjectIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = new KsqlContext(testHarness.ksqlConfig.getKsqlConfigProps());
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }


  @Test
  public void testSelectProjectKeyTimestamp() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();

    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_KEY_TIMESTAMP AS SELECT ROWKEY AS RKEY, ROWTIME AS RTIME, ITEMID FROM ORDERS WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';");

    Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(ksqlContext.getMetaStore().getSource("PROJECT_KEY_TIMESTAMP").getSchema());

    Map<String, GenericRow> results = testHarness.consumeData("PROJECT_KEY_TIMESTAMP", resultSchema , dataProvider.data().size(), new StringDeserializer());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", recordMetadataMap.get("8").timestamp(), "ITEM_8")));

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(results, expectedResults);
  }

  @Test
  public void testSelectProject() throws Exception {

    OrderDataProvider dataProvider = publishOrdersTopicData();
    createOrdersStream();

    ksqlContext.sql("CREATE STREAM PROJECT_STREAM AS SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM ORDERS;");

    Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(ksqlContext.getMetaStore().getSource("PROJECT_STREAM").getSchema());

    Map<String, GenericRow> easyOrdersData = testHarness.consumeData("PROJECT_STREAM", resultSchema, dataProvider.data().size(), new StringDeserializer());

    GenericRow value = easyOrdersData.values().iterator().next();
    Assert.assertEquals( "ITEM_1", value.getColumns().get(0));
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


  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON');");
  }

  private OrderDataProvider publishOrdersTopicData() throws InterruptedException, TimeoutException, ExecutionException {
    OrderDataProvider dataProvider = new OrderDataProvider();

    String topicName = "TestTopic";
    testHarness.createTopic(topicName);
    recordMetadataMap = testHarness.produceData(topicName, dataProvider.data(), dataProvider.schema());
    return dataProvider;
  }
}
