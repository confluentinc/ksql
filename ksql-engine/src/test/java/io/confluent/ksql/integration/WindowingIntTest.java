package io.confluent.ksql.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

@Category({IntegrationTest.class})
public class WindowingIntTest {

  public static final int WINDOW_SIZE_SEC = 5;
  private static final int MAX_POLL_PER_ITERATION = 100;
  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> datasetOneMetaData;
  private final String topicName = "TestTopic";
  private OrderDataProvider dataProvider;
  private long now;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig);
    testHarness.createTopic(topicName);

    /**
     * Setup test data - align to the next time unit to support tumbling window alignment
     */
    alignTimeToWindowSize(WINDOW_SIZE_SEC);

    now = System.currentTimeMillis()+500;

    testHarness.createTopic("ORDERS");
    dataProvider = new OrderDataProvider();
    datasetOneMetaData = testHarness.publishTestData(topicName, dataProvider, now - 500);
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void shouldAggregateWithNoWindow() throws Exception {

    testHarness.publishTestData(topicName, dataProvider, now);


    final String streamName = "NOWINDOW_AGGTEST";

    final String queryString = String.format(
        "CREATE TABLE %s AS SELECT %s FROM ORDERS WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
        streamName,
        "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(KEYVALUEMAP['key2']/2)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2 /** 2 x
     items **/, 20.0, 2.0));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      final Map<String, GenericRow> aggregateResults = testHarness.consumeData(streamName,
                                                                           resultSchema, 1, new
                                                                                   StringDeserializer(), MAX_POLL_PER_ITERATION);
      final GenericRow actual = aggregateResults.get("ITEM_1");
      return expected.equals(actual);
    }, 60000, "didn't receive correct results within timeout");

    AdminClient adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    Set<String> topicBeforeCleanup = topicClient.listTopicNames();

    assertThat("Expected to have 5 topics instead have : " + topicBeforeCleanup.size(),
               topicBeforeCleanup.size(), equalTo(5));
    QueryMetadata queryMetadata = ksqlContext.getRunningQueries().iterator().next();

    queryMetadata.close();
    Set<String> topicsAfterCleanUp = topicClient.listTopicNames();

    assertThat("Expected to see 3 topics after clean up but seeing " + topicsAfterCleanUp.size
        (), topicsAfterCleanUp.size(), equalTo(3));
    assertThat(topicClient.getTopicCleanupPolicy(streamName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.COMPACT));
  }


  @Test
  public void shouldAggregateTumblingWindow() throws Exception {

    testHarness.publishTestData(topicName, dataProvider, now);


    final String streamName = "TUMBLING_AGGTEST";

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)/COUNT(*)",
            "TUMBLING ( SIZE 10 SECONDS)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2 /** 2 x
     items **/, 20.0, 100.0));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      final Map<Windowed<String>, GenericRow> windowedResults = testHarness.consumeData(streamName, resultSchema, 1, new TimeWindowedDeserializer<>(new StringDeserializer()), MAX_POLL_PER_ITERATION);
      updateResults(results, windowedResults);
      final GenericRow actual = results.get("ITEM_1");
      return expected.equals(actual);
    }, 60000, "didn't receive correct results within timeout");

    AdminClient adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    Set<String> topicBeforeCleanup = topicClient.listTopicNames();

    assertThat("Expected to have 5 topics instead have : " + topicBeforeCleanup.size(),
               topicBeforeCleanup.size(), equalTo(5));
    QueryMetadata queryMetadata = ksqlContext.getRunningQueries().iterator().next();

    queryMetadata.close();
    Set<String> topicsAfterCleanUp = topicClient.listTopicNames();

    assertThat("Expected to see 3 topics after clean up but seeing " + topicsAfterCleanUp.size
        (), topicsAfterCleanUp.size(), equalTo(3));
    assertThat(topicClient.getTopicCleanupPolicy(streamName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.DELETE));
  }

  private void updateResults(Map<String, GenericRow> results, Map<Windowed<String>, GenericRow> windowedResults) {
    for (Map.Entry<Windowed<String>, GenericRow> entry : windowedResults.entrySet()) {
      results.put(entry.getKey().key(), entry.getValue());
    }
  }

  @Test
  public void shouldAggregateHoppingWindow() throws Exception {

    testHarness.publishTestData(topicName, dataProvider, now);


    final String streamName = "HOPPING_AGGTEST";

    final String queryString = String.format(
            "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
            streamName,
            "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)",
            "HOPPING ( SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)"
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();


    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2 /** 2 x
     items **/, 20.0, 200.0));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      final Map<Windowed<String>, GenericRow> windowedResults = testHarness.consumeData(streamName, resultSchema, 1, new TimeWindowedDeserializer<>(new StringDeserializer()), 1000);
      updateResults(results, windowedResults);
      final GenericRow actual = results.get("ITEM_1");
      return expected.equals(actual);
    }, 60000, "didn't receive correct results within timeout");

    AdminClient adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    Set<String> topicBeforeCleanup = topicClient.listTopicNames();

    assertThat("Expected to have 5 topics instead have : " + topicBeforeCleanup.size(),
               topicBeforeCleanup.size(), equalTo(5));
    QueryMetadata queryMetadata = ksqlContext.getRunningQueries().iterator().next();

    queryMetadata.close();
    Set<String> topicsAfterCleanUp = topicClient.listTopicNames();

    assertThat("Expected to see 3 topics after clean up but seeing " + topicsAfterCleanUp.size
        (), topicsAfterCleanUp.size(), equalTo(3));
    assertThat(topicClient.getTopicCleanupPolicy(streamName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.DELETE));
  }

  @Test
  public void shouldAggregateSessionWindow() throws Exception {

    testHarness.publishTestData(topicName, dataProvider, now);


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

    final Map<String, GenericRow> results = new HashMap<>();

    TestUtils.waitForCondition(() -> {
      final Map<Windowed<String>, GenericRow> windowedResults = testHarness.consumeData(streamName, resultSchema, datasetOneMetaData.size(), new TimeWindowedDeserializer<>(new StringDeserializer()), 1000);
      updateResults(results, windowedResults);
      final GenericRow actual = results.get("ORDER_6");
      return expectedResults.equals(actual) && results.size() == 6;
    }, 60000, "didn't receive correct results within timeout");

    AdminClient adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    Set<String> topicBeforeCleanup = topicClient.listTopicNames();

    assertThat("Expected to have 5 topics instead have : " + topicBeforeCleanup.size(),
               topicBeforeCleanup.size(), equalTo(5));
    QueryMetadata queryMetadata = ksqlContext.getRunningQueries().iterator().next();

    queryMetadata.close();
    Set<String> topicsAfterCleanUp = topicClient.listTopicNames();

    assertThat("Expected to see 3 topics after clean up but seeing " + topicsAfterCleanUp.size
        (), topicsAfterCleanUp.size(), equalTo(3));
    assertThat(topicClient.getTopicCleanupPolicy(streamName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.DELETE));

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
