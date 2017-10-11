package io.confluent.ksql;

import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedResults;

public class UdfIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;
  String topicName = "TestTopic";

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

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(ksqlContext.getMetaStore().getSource(stream2Name).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("8", recordMetadataMap.get("8").timestamp() +
                    10000, "8", recordMetadataMap.get("8").timestamp() + 10000,
            recordMetadataMap.get("8").timestamp() + 100, "ORDER_6", "ITEM_8")));

    Map<String, GenericRow> results1 = testHarness.consumeData(stream1Name, resultSchema,expectedResults.size(), new StringDeserializer());

    Map<String, GenericRow> results = testHarness.consumeData(stream2Name, resultSchema,expectedResults.size(), new StringDeserializer());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedResults(expectedResults, results);

  }


  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON');");
  }

  private OrderDataProvider publishOrdersTopicData() throws InterruptedException, TimeoutException, ExecutionException {
    OrderDataProvider dataProvider = new OrderDataProvider();

    recordMetadataMap = testHarness.produceData(topicName, dataProvider.data(), dataProvider.schema());
    return dataProvider;
  }
}
