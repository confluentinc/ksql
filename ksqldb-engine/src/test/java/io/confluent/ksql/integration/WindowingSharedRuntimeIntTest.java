/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient.TopicCleanupPolicy;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.ConsumerTestUtil.hasUniqueRecords;
import static io.confluent.ksql.test.util.MapMatchers.mapHasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@Category({IntegrationTest.class})
public class WindowingSharedRuntimeIntTest {

  private static final String ORDERS_STREAM = "ORDERS";

  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

  private static final TimeWindowedDeserializer<String> TIME_WINDOWED_DESERIALIZER =
      new TimeWindowedDeserializer<>(STRING_DESERIALIZER);

  private static final SessionWindowedDeserializer<String> SESSION_WINDOWED_DESERIALIZER =
      new SessionWindowedDeserializer<>(STRING_DESERIALIZER);

  private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(60);

  private final long batch0SentMs;
  private final long batch1Delay;
  private final long tenSecWindowStartMs;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true)
      .build();;

  private String sourceTopicName;
  private String resultStream0;
  private String resultStream1;
  private PhysicalSchema resultSchema;
  private Set<String> preExistingTopics;
  private KafkaTopicClient topicClient;

  public WindowingSharedRuntimeIntTest() {
    final long currentTimeMillis = System.currentTimeMillis();
    // set the batch to be in the middle of a ten second window
    batch0SentMs = currentTimeMillis - (currentTimeMillis % TimeUnit.SECONDS.toMillis(10)) + (5001);
    tenSecWindowStartMs = batch0SentMs - (batch0SentMs % TimeUnit.SECONDS.toMillis(10));
    batch1Delay = 500;
  }

  @Before
  public void before() {
    topicClient = ksqlContext.getServiceContext().getTopicClient();

    sourceTopicName = TopicTestUtil.uniqueTopicName("orders");
    resultStream0 = KsqlIdentifierTestUtil.uniqueIdentifierName("FIRST");
    resultStream1 = KsqlIdentifierTestUtil.uniqueIdentifierName("SECOND");

    TEST_HARNESS.ensureTopics(sourceTopicName, ORDERS_STREAM.toUpperCase());

    final OrderDataProvider dataProvider = new OrderDataProvider();
    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, KAFKA, JSON, () -> batch0SentMs);
    TEST_HARNESS.produceRows(sourceTopicName, dataProvider, KAFKA, JSON, () -> batch0SentMs + batch1Delay);

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
        genericRow(2L, 20.0, 2.0)
    );

    // Then:
    assertOutputOf(resultStream0, expected, is(expected));
    assertTableCanBeUsedAsSource(expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT, 5, resultStream0, resultStream1);
  }

  @Test
  public void shouldAggregateTumblingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10)/COUNT(*) "
        + "FROM " + ORDERS_STREAM + " WINDOW TUMBLING (SIZE 10 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(tenSecWindowStartMs, Long.MAX_VALUE)),
        genericRow(2L, 20.0, 100.0)
    );

    // Then:
    assertOutputOf(resultStream0, expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT_DELETE, 3, resultStream0);
  }

  @Test
  public void shouldAggregateHoppingWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ITEMID, COUNT(ITEMID), SUM(ORDERUNITS), SUM(ORDERUNITS * 10) "
        + "FROM " + ORDERS_STREAM + " WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS) "
        + "WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;");

    final long firstWindowStart = tenSecWindowStartMs;
    final long secondWindowStart = firstWindowStart + TimeUnit.SECONDS.toMillis(5);

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap.of(
        new Windowed<>("ITEM_1", new TimeWindow(firstWindowStart, Long.MAX_VALUE)),
        genericRow(2L, 20.0, 200.0),
        new Windowed<>("ITEM_1", new TimeWindow(secondWindowStart, Long.MAX_VALUE)),
        genericRow(2L, 20.0, 200.0)
    );

    // Then:
    assertOutputOf(resultStream0, expected, is(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT_DELETE, 3, resultStream0);
  }

  @Test
  public void shouldAggregateSessionWindow() {
    // Given:
    givenTable("CREATE TABLE %s AS "
        + "SELECT ORDERID, COUNT(*), SUM(ORDERUNITS) "
        + "FROM " + ORDERS_STREAM + " WINDOW SESSION (10 SECONDS) "
        + "GROUP BY ORDERID;");

    final long sessionEnd = batch0SentMs + batch1Delay;

    final Map<Windowed<String>, GenericRow> expected = ImmutableMap
        .<Windowed<String>, GenericRow>builder()
        .put(new Windowed<>("ORDER_1", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(2L, 20.0))
        .put(new Windowed<>("ORDER_2", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(2L, 40.0))
        .put(new Windowed<>("ORDER_3", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(2L, 60.0))
        .put(new Windowed<>("ORDER_4", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(2L, 80.0))
        .put(new Windowed<>("ORDER_5", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(2L, 100.0))
        .put(new Windowed<>("ORDER_6", new SessionWindow(batch0SentMs, sessionEnd)),
            genericRow(6L, 420.0))
        .build();

    // Then:
    assertOutputOf(resultStream0, expected, mapHasItems(expected));
    assertTopicsCleanedUp(TopicCleanupPolicy.COMPACT_DELETE, 2, resultStream0);
  }

  private void givenTable(final String sql) {
    ksqlContext.sql(String.format(sql, resultStream0));
    final DataSource source = ksqlContext.getMetaStore().getSource(SourceName.of(resultStream0));
    resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getKsqlTopic().getKeyFormat().getFeatures(),
        source.getKsqlTopic().getValueFormat().getFeatures()
    );
  }

  @SuppressWarnings("unchecked")
  private <K> void assertOutputOf(
      final String streamName,
      final Map<K, GenericRow> expected,
      final Matcher<? super Map<K, GenericRow>> tableRowMatcher
  ) {
    final Deserializer<K> keyDeserializer =
        (Deserializer<K>) getKeyDeserializerFor(expected.keySet().iterator().next());

    TEST_HARNESS.verifyAvailableRows(
        streamName, hasUniqueRecords(tableRowMatcher), JSON, resultSchema, keyDeserializer,
        VERIFY_TIMEOUT);
  }

  private <K> void assertTableCanBeUsedAsSource(
      final Map<K, GenericRow> expected,
      final Matcher<? super Map<K, GenericRow>> tableRowMatcher
  ) {
    ksqlContext.sql("CREATE TABLE " + resultStream1 + " AS SELECT * FROM " + resultStream0 + ";");

    final DataSource source = ksqlContext.getMetaStore().getSource(SourceName.of(resultStream1));

    resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getKsqlTopic().getKeyFormat().getFeatures(),
        source.getKsqlTopic().getValueFormat().getFeatures()
    );

    assertOutputOf(resultStream1, expected, tableRowMatcher);
  }

  private void assertTopicsCleanedUp(
      final TopicCleanupPolicy topicCleanupPolicy,
      final int nTopics,
      final String ...sinkTopics
  ) {
    assertThat("Initial topics", getTopicNames(), hasSize(nTopics));

    ksqlContext.getPersistentQueries().forEach(QueryMetadata::close);

    assertThatEventually("After cleanup", this::getTopicNames,
        containsInAnyOrder(
            Arrays.stream(sinkTopics).map(Matchers::equalTo).collect(Collectors.toList())));

    assertThat(topicClient.getTopicCleanupPolicy(resultStream0),
        is(topicCleanupPolicy));
  }

  private Set<String> getTopicNames() {
    final Set<String> names = topicClient.listTopicNames();
    names.removeAll(preExistingTopics);
    return names;
  }

  private static Deserializer<?> getKeyDeserializerFor(final Object key) {
    if (key instanceof Windowed) {
      if (((Windowed<?>) key).window() instanceof SessionWindow) {
        return SESSION_WINDOWED_DESERIALIZER;
      }
      return TIME_WINDOWED_DESERIALIZER;
    }

    return STRING_DESERIALIZER;
  }

  private void createOrdersStream() {
    ksqlContext.sql("CREATE STREAM " + ORDERS_STREAM + " ("
        + "ORDERID varchar KEY, "
        + "ORDERTIME bigint, "
        + "ITEMID varchar, "
        + "ORDERUNITS double, "
        + "PRICEARRAY array<double>, "
        + "KEYVALUEMAP map<varchar, double>) "
        + "WITH (kafka_topic='" + sourceTopicName + "', value_format='JSON');");
  }
}
