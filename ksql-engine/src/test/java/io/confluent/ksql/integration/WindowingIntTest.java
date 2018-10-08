package io.confluent.ksql.integration;

import static io.confluent.ksql.integration.IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient.TopicCleanupPolicy;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class WindowingIntTest {

  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final int WINDOW_SIZE_SEC = 5;
  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
  private static final TimeWindowedDeserializer<String> TIME_WINDOWED_DESERIALIZER =
      new TimeWindowedDeserializer<>(STRING_DESERIALIZER);

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private String streamName;
  private Schema resultSchema;

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start(Collections.emptyMap());
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig);
    testHarness.createTopic("TestTopic");
    UdfLoaderUtil.load(ksqlContext.getMetaStore());

    final long now = 1438997955143L;

    testHarness.createTopic("ORDERS");
    final OrderDataProvider dataProvider = new OrderDataProvider();
    testHarness.publishTestData("TestTopic", dataProvider, now);
    createOrdersStream();

    testHarness.publishTestData("TestTopic", dataProvider, now + 500);

    streamName = "STREAM_" + COUNTER.getAndIncrement();
  }

  @After
  public void after() {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void shouldAggregateWithNoWindow() throws Exception {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(KEYVALUEMAP['key2']/2) "
        + "FROM ORDERS WHERE ITEMID = 'ITEM_1' "
        + "GROUP BY ITEMID;");

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 2.0));

    // Then:
    assertOutputOf(1, "ITEM_1", expected, STRING_DESERIALIZER);
    assertTableCanBeUsedAsSource(1, "ITEM_1", expected, STRING_DESERIALIZER);
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT);
  }

  @Test
  public void shouldAggregateTumblingWindow() throws Exception {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)/COUNT(*) "
        + "FROM ORDERS WINDOW TUMBLING ( SIZE 10 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 100.0));

    // Then:
    assertOutputOf(1, "ITEM_1", expected, TIME_WINDOWED_DESERIALIZER);
    assertTableCanBeUsedAsSource(1, "ITEM_1", expected, TIME_WINDOWED_DESERIALIZER);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateHoppingWindow() throws Exception {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10) "
        + "FROM ORDERS WINDOW HOPPING ( SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 200.0));

    // Then:
    assertOutputOf(1, "ITEM_1", expected, TIME_WINDOWED_DESERIALIZER);
    assertTableCanBeUsedAsSource(1, "ITEM_1", expected, TIME_WINDOWED_DESERIALIZER);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateSessionWindow() throws Exception {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ORDERID, COUNT(*), SUM(ORDERUNITS) "
        + "FROM ORDERS WINDOW SESSION (10 SECONDS) "
        + "GROUP BY ORDERID;");

    final GenericRow expected = new GenericRow(Arrays.asList(null, null, "ORDER_6", 6, 420.0));

    // Then:
    assertOutputOf(6, "ORDER_6", expected, TIME_WINDOWED_DESERIALIZER);
    assertTableCanBeUsedAsSource(6, "ORDER_6", expected, TIME_WINDOWED_DESERIALIZER);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  private void assertTableCanBeUsedAsSource(
      final int expectedMsgCount,
      final String expectedKey,
      final GenericRow expected,
      final Deserializer deserializer) throws InterruptedException {
    ksqlContext.sql(String.format("CREATE STREAM %s_TWO AS SELECT * FROM %s;", streamName, streamName));
    streamName = streamName + "_TWO";
    resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    assertOutputOf(expectedMsgCount, expectedKey, expected, deserializer);
  }

  private void givenTable(final String sql) {
    ksqlContext.sql(String.format(sql, streamName));
    resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();
  }

  @SuppressWarnings("unchecked")
  private <K> Map<K, GenericRow> getOutput(
      final int expectedMsgCount,
      final Deserializer deserializer
  ) {
    return testHarness.consumeData(
        streamName, resultSchema, expectedMsgCount, deserializer, RESULTS_POLL_MAX_TIME_MS);
  }

  @SuppressWarnings("unchecked")
  private void assertOutputOf(
      final int expectedMsgCount,
      final String expectedKey,
      final GenericRow expectedValue,
      final Deserializer<?> deserializer) throws InterruptedException {

    final Map<Object, Object> allResults = new HashMap<>();
    try {
      TestUtils.waitForCondition(() -> {
        final Map<?, GenericRow> results = getOutput(expectedMsgCount, deserializer);
        allResults.putAll(results);

        final GenericRow actual;
        if (deserializer == STRING_DESERIALIZER) {
          actual = results.get(expectedKey);
        } else {
          actual = results.entrySet().stream()
              .filter(e -> ((Windowed<String>) e.getKey()).key().equals(expectedKey))
              .findAny()
              .map(Entry::getValue)
              .orElse(null);
        }

        return expectedValue.equals(actual);
      }, 60000, "didn't receive correct results within timeout.");
    } catch (final AssertionError e) {
      throw new AssertionError(e.getMessage() + " Got: " + allResults, e);
    }
  }

  private void assertTopicsCleanedUp(final TopicCleanupPolicy topicCleanupPolicy) {
    final KafkaTopicClient topicClient = testHarness.topicClient();

    assertThat("Initial topics", topicClient.listTopicNames(), hasSize(7));

    ksqlContext.getRunningQueries().forEach(QueryMetadata::close);

    assertThat("After cleanup", topicClient.listTopicNames(), hasSize(4));

    assertThat(topicClient.getTopicCleanupPolicy(streamName), is(topicCleanupPolicy));
  }

  private void createOrdersStream() {
    ksqlContext.sql("CREATE STREAM ORDERS ("
        + "ORDERTIME bigint, "
        + "ORDERID varchar, "
        + "ITEMID varchar, "
        + "ORDERUNITS double, "
        + "PRICEARRAY array<double>, "
        + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }
}
