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

public class UdfIntTest {

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
  public void testApplyUdfsToColumns() throws Exception {
    final String testStreamName = "SelectUDFsStream".toUpperCase();

    publishOrdersTopicData();
    createOrdersStream();

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
            testStreamName,
            "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000",
            "ORDERS",
            "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, "ITEM_8", 800.0, 1110.0, 12.0, true)));

    Map<String, GenericRow> results = testHarness.consumeData(testStreamName, resultSchema, 4, new StringDeserializer());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testShouldCastSelectedColumns() throws Exception {
    final String streamName = "CastExpressionStream".toUpperCase();

    publishOrdersTopicData();
    createOrdersStream();

    final String selectColumns =
            " CAST (ORDERUNITS AS INTEGER), CAST( PRICEARRAY[1]>1000 AS STRING), CAST (SUBSTRING"
                    + "(ITEMID, 5) AS DOUBLE), CAST(ORDERUNITS AS VARCHAR) ";

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
            streamName,
            selectColumns,
            "ORDERS",
            "ORDERUNITS > 20 AND ITEMID LIKE '%_8'"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(null, null, 80, "true", 8.0, "80.0")));

    Map<String, GenericRow> results = testHarness.consumeData(streamName, resultSchema, 4, new StringDeserializer());


    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(expectedResults, results);
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
