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
import static io.confluent.ksql.test.util.ConsumerTestUtil.hasUniqueRecords;
import static io.confluent.ksql.test.util.MapMatchers.mapHasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient.TopicCleanupPolicy;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.hamcrest.Matcher;
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

  private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(60);

  private static final long BATCH_0_SENT_MS = 1000000005001L;
  private static final long BATCH_1_DELAY = 500;
  private static final long TEN_SEC_WINDOW_START_MS =
      BATCH_0_SENT_MS - (BATCH_0_SENT_MS % TimeUnit.SECONDS.toMillis(10));

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

    UdfLoaderUtil.load(ksqlContext.getFunctionRegistry());

    sourceTopicName = TopicTestUtil.uniqueTopicName("orders");
    resultStream0 = KsqlIdentifierTestUtil.uniqueIdentifierName("FIRST");
    resultStream1 = KsqlIdentifierTestUtil.uniqueIdentifierName("SECOND");

    TEST_HARNESS.ensureTopics(sourceTopicName, ORDERS_STREAM.toUpperCase());

    final OrderDataProvider dataProvider = new OrderDataProvider();
    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, JSON, () -> BATCH_0_SENT_MS);
    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, JSON, () -> BATCH_0_SENT_MS + BATCH_1_DELAY);

    createOrdersStream();

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
    assertOutputOf(resultStream0, expected, is(expected));
    assertTableCanBeUsedAsSource(expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT);
  }

  @Test
  public void shouldAggregateTumblingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)/COUNT(*) "
        + "FROM " + ORDERS_STREAM + " WINDOW TUMBLING (SIZE 10 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(TEN_SEC_WINDOW_START_MS, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 100.0)));

    // Then:
    assertOutputOf(resultStream0, expected, is(expected));
    assertTableCanBeUsedAsSource(expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateHoppingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10) "
        + "FROM " + ORDERS_STREAM + " WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final long firstWindowStart = TEN_SEC_WINDOW_START_MS;
    final long secondWindowStart = firstWindowStart + TimeUnit.SECONDS.toMillis(5);

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(firstWindowStart, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 200.0)),
        new Windowed<>("ITEM_1", new TimeWindow(secondWindowStart, Long.MAX_VALUE)),
        new GenericRow(Arrays.asList(null, null, "ITEM_1", 2, 20.0, 200.0)));

    // Then:
    assertOutputOf(resultStream0, expected, is(expected));
    assertTableCanBeUsedAsSource(expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  @Test
  public void shouldAggregateSessionWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ORDERID, COUNT(*), SUM(ORDERUNITS) "
        + "FROM " + ORDERS_STREAM + " WINDOW SESSION (10 SECONDS) "
        + "GROUP BY ORDERID;");

    final long sessionEnd = BATCH_0_SENT_MS + BATCH_1_DELAY;

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap
        .<Windowed<String>, GenericRow>builder()
        .put(new Windowed<>("ORDER_1", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_1", 2, 20.0)))
        .put(new Windowed<>("ORDER_2", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_2", 2, 40.0)))
        .put(new Windowed<>("ORDER_3", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_3", 2, 60.0)))
        .put(new Windowed<>("ORDER_4", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_4", 2, 80.0)))
        .put(new Windowed<>("ORDER_5", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_5", 2, 100.0)))
        .put(new Windowed<>("ORDER_6", new SessionWindow(BATCH_0_SENT_MS, sessionEnd)),
            new GenericRow(Arrays.asList(null, null, "ORDER_6", 6, 420.0)))
        .build();

    // Then:
    assertOutputOf(resultStream0, expected, mapHasItems(expected));
    assertTableCanBeUsedAsSource(expected, mapHasItems(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.DELETE);
  }

  private void givenTable(final String sql) {
    ksqlContext.sql(String.format(sql, resultStream0));
    resultSchema = ksqlContext.getMetaStore().getSource(resultStream0).getSchema();
  }

  @SuppressWarnings("unchecked")
  private <K> void assertOutputOf(
      final String streamName,
      final Map<K, GenericRow> expected,
      final Matcher<? super Map<K, GenericRow>> tableRowMatcher
  ) {
    final Deserializer keyDeserializer = getKeyDeserializerFor(expected.keySet().iterator().next());

    TEST_HARNESS.verifyAvailableRows(
        streamName, hasUniqueRecords(tableRowMatcher), JSON, resultSchema, keyDeserializer,
        VERIFY_TIMEOUT);
  }

  private <K> void assertTableCanBeUsedAsSource(
      final Map<K, GenericRow> expected,
      final Matcher<? super Map<K, GenericRow>> tableRowMatcher
  ) {
    ksqlContext.sql("CREATE TABLE " + resultStream1 + " AS SELECT * FROM " + resultStream0 + ";");
    resultSchema = ksqlContext.getMetaStore().getSource(resultStream1).getSchema();

    assertOutputOf(resultStream1, expected, tableRowMatcher);
  }

  private void assertTopicsCleanedUp(final TopicCleanupPolicy topicCleanupPolicy) {
    assertThat("Initial topics", getTopicNames(), hasSize(5));

    ksqlContext.getPersistentQueries().stream()
        .map(PersistentQueryMetadata::getQueryId)
        .forEach(ksqlContext::terminateQuery);

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
