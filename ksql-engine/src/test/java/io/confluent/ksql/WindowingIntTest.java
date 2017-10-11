package io.confluent.ksql;

import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.TopicProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
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
import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedWindowedResults;

public class WindowingIntTest {

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
  public void testAggSelectStar() throws Exception {

    OrderDataProvider orderDataProvider = publishOrdersTopicData();
    createOrdersStream();

    TopicProducer topicProducer = new TopicProducer(testHarness.embeddedKafkaCluster);

    Map<String, RecordMetadata> newRecordsMetadata = topicProducer.produceInputData(topicName, orderDataProvider.data(), orderDataProvider.schema());

    final String streamName = "AGGTEST";
    final long windowSizeMilliseconds = 2000;

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM %s WINDOW %s WHERE ORDERUNITS > 60 GROUP BY ITEMID  HAVING %s;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS)/COUNT(ORDERUNITS), SUM(PRICEARRAY[0]+10)",
            "ORDERS",
            String.format("TUMBLING ( SIZE %d MILLISECOND)", windowSizeMilliseconds),
            "SUM(ORDERUNITS) > 150"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();


    long firstItem8Window  = recordMetadataMap.get("8").timestamp() / windowSizeMilliseconds;
    long secondItem8Window =   newRecordsMetadata.get("8").timestamp() / windowSizeMilliseconds;

    Map<Windowed<String>, GenericRow> expectedResults = new HashMap<>();
    if (firstItem8Window == secondItem8Window) {
      expectedResults.put(
              new Windowed<>("ITEM_8",new TimeWindow(0, 1)),
              new GenericRow(Arrays.asList(null, null, "ITEM_8", 2, 160.0, 80.0, 2220.0))
      );
    }

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema, expectedResults.size(), new WindowedDeserializer<>(new StringDeserializer()));

    System.out.println("Results:" + results + " size:" + results.size());

    Assert.assertEquals(expectedResults.size(), results.size());
    assertExpectedWindowedResults(results, expectedResults);
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }

  private OrderDataProvider publishOrdersTopicData() throws InterruptedException, TimeoutException, ExecutionException {
    OrderDataProvider dataProvider = new OrderDataProvider();
    testHarness.createTopic("ORDERS");
    recordMetadataMap = testHarness.produceData(topicName, dataProvider.data(), dataProvider.schema());
    return dataProvider;
  }
}
