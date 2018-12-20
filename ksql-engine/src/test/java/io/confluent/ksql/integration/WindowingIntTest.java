/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import static io.confluent.ksql.serde.DataSource.DataSourceSerDe.JSON;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient.TopicCleanupPolicy;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class WindowingIntTest {

  private static final String ORDERS_STREAM = "ORDERS";

  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

  private static final TimeWindowedDeserializer<String> TIME_WINDOWED_DESERIALIZER =
      new TimeWindowedDeserializer<>(STRING_DESERIALIZER);

  private static final SessionWindowedDeserializer<String> SESSION_WINDOWED_DESERIALIZER =
      new SessionWindowedDeserializer<>(STRING_DESERIALIZER);

  @ClassRule
  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.buildKsqlContext();

  private String sourceTopicName;
  private String resultStream0;
  private String resultStream1;
  private Schema resultSchema;
  private Set<String> preExistingTopics;
  private KafkaTopicClient topicClient;

  @Before
  public void before() {
    topicClient = ksqlContext.getServiceContext().getTopicClient();

    UdfLoaderUtil.load(ksqlContext.getMetaStore());

    sourceTopicName = TopicTestUtil.uniqueTopicName("orders");
    resultStream0 = KsqlIdentifierTestUtil.uniqueIdentifierName("FIRST");
    resultStream1 = KsqlIdentifierTestUtil.uniqueIdentifierName("SECOND");

    TEST_HARNESS.ensureTopics(sourceTopicName, ORDERS_STREAM.toUpperCase());

    final long now = 1438997955143L;

    final OrderDataProvider dataProvider = new OrderDataProvider();
    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, JSON, () -> now);

    createOrdersStream();

    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, JSON, () -> now + 500);

    preExistingTopics = topicClient.listTopicNames();
  }

  @Test
  public void shouldAggregateWithNoWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(KEYVALUEMAP['key2']/2) "
        + "FROM " + ORDERS_STREAM + " WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final Map<String, GenericRow> expected = ImmutableMap.of(
        "ITEM_1",
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 2.0)));

    // Then:
    assertOutputOf(resultStream0, expected, AssertType.EqualTo);
    assertTableCanBeUsedAsSource(expected, AssertType.EqualTo);
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT);
  }

  @Test
  public void shouldAggregateTumblingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)/COUNT(*) "
        + "FROM " + ORDERS_STREAM + " WINDOW TUMBLING ( SIZE 10 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(1438997950000L, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 100.0)));

    // Then:
    assertOutputOf(resultStream0, expected, AssertType.EqualTo);
    assertTableCanBeUsedAsSource(expected, AssertType.EqualTo);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateHoppingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10) "
        + "FROM " + ORDERS_STREAM + " WINDOW HOPPING ( SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(1438997950000L, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 200.0)),
        new Windowed<>("ITEM_1", new TimeWindow(1438997955000L, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 200.0)));

    // Then:
    assertOutputOf(resultStream0, expected, AssertType.EqualTo);
    assertTableCanBeUsedAsSource(expected, AssertType.EqualTo);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateSessionWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ORDERID, COUNT(*), SUM(ORDERUNITS) "
        + "FROM " + ORDERS_STREAM + " WINDOW SESSION (10 SECONDS) "
        + "GROUP BY ORDERID;");

    final ImmutableMap<Windowed<String>, GenericRow> expected = ImmutableMap
        .<Windowed<String>, GenericRow>builder()
        .put(new Windowed<>("ORDER_1", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_1", 2, 20.0)))
        .put(new Windowed<>("ORDER_2", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_2", 2, 40.0)))
        .put(new Windowed<>("ORDER_3", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_3", 2, 60.0)))
        .put(new Windowed<>("ORDER_4", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_4", 2, 80.0)))
        .put(new Windowed<>("ORDER_5", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_5", 2, 100.0)))
        .put(new Windowed<>("ORDER_6", new SessionWindow(1438997955143L, 1438997955643L)),
            new GenericRow(Arrays.asList(null, null, "ORDER_6", 6, 420.0)))
        .build();

    // Then:
    assertOutputOf(resultStream0, expected, AssertType.HasItems);
    assertTableCanBeUsedAsSource(expected, AssertType.HasItems);
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  private void givenTable(final String sql) {
    ksqlContext.sql(String.format(sql, resultStream0));
    resultSchema = ksqlContext.getMetaStore().getSource(resultStream0).getSchema();
  }

  @SuppressWarnings("unchecked")
  private Map<?, GenericRow> getOutput(
      final String streamName,
      final Matcher<Integer> expectedMsgCount,
      final Deserializer keyDeserializer
  ) {
    return TEST_HARNESS.verifyAvailableUniqueRows(
        streamName, expectedMsgCount, JSON, resultSchema, keyDeserializer);
  }

  private enum AssertType {
    EqualTo(Matchers::is, (expected, actual) ->
        Matchers.is(expected).matches(actual)),
    HasItems(Matchers::greaterThanOrEqualTo, (expected, actual) ->
        expected.entrySet().stream().reduce(true,
            (acc, e) -> acc && Matchers.is(e.getValue()).matches(actual.get(e.getKey())),
            (b1, b2) -> b1 & b2));

    private final Function<Integer, Matcher<Integer>> sizeMatcher;
    private final BiFunction<Map<?, GenericRow>, Map<?, GenericRow>, Boolean> rowMatcher;

    AssertType(
        final Function<Integer, Matcher<Integer>> sizeMatcher,
        final BiFunction<Map<?, GenericRow>, Map<?, GenericRow>, Boolean> rowMatcher
    ) {
      this.sizeMatcher = sizeMatcher;
      this.rowMatcher = rowMatcher;
    }
  }

  @SuppressWarnings("unchecked")
  private void assertOutputOf(
      final String streamName,
      final Map<?, GenericRow> expected,
      final AssertType type
  ) {
    final Deserializer keyDeserializer = getKeyDeserializerFor(expected.keySet().iterator().next());

    final Map<Object, GenericRow> actual = new HashMap<>();
    try {
      TestUtils.waitForCondition(() -> {
        actual.putAll(getOutput(
            streamName,
            type.sizeMatcher.apply(expected.size()),
            keyDeserializer));

        return type.rowMatcher.apply(expected, actual);
      }, 60000, "didn't receive correct results within timeout.");
    } catch (final AssertionError e) {
      throw new AssertionError(e.getMessage() + " Got: " + actual, e);
    } catch (final InterruptedException e) {
      throw new AssertionError("Invalid test", e);
    }
  }

  private void assertTableCanBeUsedAsSource(
      final Map<?, GenericRow> expected,
      final AssertType type
  ) {
    ksqlContext.sql("CREATE TABLE " + resultStream1 + " AS SELECT * FROM " + resultStream0 + ";");
    resultSchema = ksqlContext.getMetaStore().getSource(resultStream1).getSchema();

    assertOutputOf(resultStream1, expected, type);
  }

  private void assertTopicsCleanedUp(final TopicCleanupPolicy topicCleanupPolicy) {
    assertThat("Initial topics", getTopicNames(), hasSize(5));

    ksqlContext.getPersistentQueries().forEach(QueryMetadata::close);

    assertThatEventually("After cleanup", this::getTopicNames,
        containsInAnyOrder(resultStream0, resultStream1));

    assertThat(topicClient.getTopicCleanupPolicy(resultStream0),
        is(topicCleanupPolicy));
  }

  private Set<String> getTopicNames() {
    final Set<String> names = topicClient.listTopicNames();
    names.removeAll(preExistingTopics);
    return names;
  }

  private static Deserializer getKeyDeserializerFor(final Object key) {
    if (key instanceof Windowed) {
      if (((Windowed) key).window() instanceof SessionWindow) {
        return SESSION_WINDOWED_DESERIALIZER;
      }
      return TIME_WINDOWED_DESERIALIZER;
    }

    return STRING_DESERIALIZER;
  }

  private void createOrdersStream() {
    ksqlContext.sql("CREATE STREAM " + ORDERS_STREAM + " ("
        + "ORDERTIME bigint, "
        + "ORDERID varchar, "
        + "ITEMID varchar, "
        + "ORDERUNITS double, "
        + "PRICEARRAY array<double>, "
        + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='" + sourceTopicName + "', value_format='JSON', key='ordertime');");
  }
}
