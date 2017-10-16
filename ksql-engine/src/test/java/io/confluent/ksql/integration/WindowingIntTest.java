package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.OrderDataProvider;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.ksql.util.MetaStoreFixture.assertExpectedWindowedResults;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class WindowingIntTest {

  public static final int WINDOW_SIZE_SEC = 5;
  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> datasetOneMetaData;
  private final String topicName = "TestTopic";
  private OrderDataProvider dataProvider;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    testHarness.createTopic(topicName);

    /**
     * Setup test data - align to the next time unit to support tumbling window alignment
     */
    alignTimeToWindowSize(WINDOW_SIZE_SEC);

    testHarness.createTopic("ORDERS");
    dataProvider = new OrderDataProvider();
    datasetOneMetaData = testHarness.publishTestData(topicName, dataProvider);
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void shouldAggregateTumblingWindow() throws Exception {

    // not really required - but lets mess with  ms
    Thread.sleep(100);
    testHarness.publishTestData(topicName, dataProvider);


    final String streamName = "TUMBLING_AGGTEST";

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS)",
            "TUMBLING ( SIZE 10 SECONDS)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();


    Map<Windowed<String>, GenericRow> expectedResults = Collections.singletonMap(
              new Windowed<>("ITEM_1",new TimeWindow(0, 1)),
              new GenericRow(Arrays.asList(null, null, "ITEM_1", 2 /** 2 x items **/, 20.0))
      );

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema, expectedResults.size(), new WindowedDeserializer<>(new StringDeserializer()));

    assertExpectedWindowedResults(results, expectedResults);
  }

  @Test
  public void shouldAggregateHoppingWindow() throws Exception {

    // not really required - but lets mess with ms
    Thread.sleep(100);
    testHarness.publishTestData(topicName, dataProvider);


    final String streamName = "HOPPING_AGGTEST";

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS)",
            "HOPPING ( SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();


    Map<Windowed<String>, GenericRow> expectedResults = Collections.singletonMap(
            new Windowed<>("ITEM_1",new TimeWindow(0, 1)),
            new GenericRow(Arrays.asList(null, null, "ITEM_1", 2 /** 2 x items **/, 20.0))
    );

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema, expectedResults.size(), new WindowedDeserializer<>(new StringDeserializer()));

    assertExpectedWindowedResults(results, expectedResults);
  }

  @Test
  public void shouldAggregateSessionWindow() throws Exception {

    // not really required - but lets mess with ms
    Thread.sleep(100);
    testHarness.publishTestData(topicName, dataProvider);


    final String streamName = "SESSION_AGGTEST";

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s GROUP BY ORDERID;",
            streamName,
            "ORDERID, COUNT(*), SUM(ORDERUNITS)",
            "SESSION (10 SECONDS)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();


    GenericRow expectedResults = new GenericRow(Arrays.asList(null, null, "ORDER_6", 6 /** 2 x items **/, 420.0));

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema, datasetOneMetaData.size(), new WindowedDeserializer<>(new StringDeserializer()));

    GenericRow order6Result = null;
    for (Windowed<String> stringWindowed : results.keySet()) {
      if (stringWindowed.toString().startsWith("[ORDER_6")) {
        order6Result = results.get(stringWindowed);
      }
    }

    assertThat("Expected to get ORDER_6, instead got:" + results.keySet(), notNullValue().matches(order6Result));

    Map<String, GenericRow> results2 = new HashMap<>();
    for (GenericRow genericRow : results.values()) {
      results2.put(genericRow.getColumns().get(2).toString(), genericRow);
    }

    assertThat("Expecting sessions for all ORDERS_1 -> _6, got:" + results.keySet(), results.size(), equalTo(6 ));
    assertThat(order6Result, equalTo(expectedResults));
  }

  private int alignTimeToWindowSize(int secondOfMinuteModulus) throws InterruptedException {
    while (LocalTime.now().getSecond() % secondOfMinuteModulus != 0){
      Thread.sleep(500);
    }
    return LocalTime.now().getSecond();
  }

  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }

}
