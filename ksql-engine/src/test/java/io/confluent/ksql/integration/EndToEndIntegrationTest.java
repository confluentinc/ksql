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

import static io.confluent.ksql.serde.Format.JSON;
import static io.confluent.ksql.util.KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX;
import static java.lang.String.format;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.UserDataProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test emulates the end to end flow in the quick start guide and ensures that the outputs at each stage
 * are what we expect. This tests a broad set of KSQL functionality and is a good catch-all.
 */
@Category({IntegrationTest.class})
public class EndToEndIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String USERS_TOPIC = "users";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final String USER_TABLE = "users_original";

  private static final AtomicInteger CONSUMED_COUNT = new AtomicInteger();
  private static final AtomicInteger PRODUCED_COUNT = new AtomicInteger();
  private static final PageViewDataProvider PAGE_VIEW_DATA_PROVIDER = new PageViewDataProvider();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(
          StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
          DummyProducerInterceptor.class.getName()
      )
      .withAdditionalConfig(
          StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
          DummyConsumerInterceptor.class.getName()
      )
      .withAdditionalConfig(
          KSQL_FUNCTIONS_PROPERTY_PREFIX + "e2econfigurableudf.some.setting",
          "foo-bar"
      )
      .withAdditionalConfig(
          KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.expected-param",
          "expected-value"
      )
      .build();

  @Rule
  public final Timeout timeout = Timeout.seconds(120);

  private QueryMetadata toClose;

  @Before
  public void before() {
    ConfigurableUdf.PASSED_CONFIG = null;
    PRODUCED_COUNT.set(0);
    CONSUMED_COUNT.set(0);

    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC, USERS_TOPIC);

    TEST_HARNESS
        .produceRows(USERS_TOPIC, new UserDataProvider(), JSON,
            () -> System.currentTimeMillis() - 10000);
    TEST_HARNESS
        .produceRows(PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, JSON, System::currentTimeMillis);

    ksqlContext.sql("CREATE TABLE " + USER_TABLE
        + " (registertime bigint, gender varchar, regionid varchar, userid varchar)"
        + " WITH (kafka_topic='" + USERS_TOPIC + "', value_format='JSON', key = 'userid');");

    ksqlContext.sql("CREATE STREAM " + PAGE_VIEW_STREAM
        + " (viewtime bigint, userid varchar, pageid varchar) "
        + "WITH (kafka_topic='" + PAGE_VIEW_TOPIC + "', value_format='JSON');");
  }

  @After
  public void after() {
    if (toClose != null) {
      toClose.close();
    }
  }

  @Test
  public void shouldSelectAllFromUsers() throws Exception {
    final TransientQueryMetadata queryMetadata = executeQuery(
        "SELECT * from %s;", USER_TABLE);

    final Set<String> expectedUsers = ImmutableSet
        .of("USER_0", "USER_1", "USER_2", "USER_3", "USER_4");

    final List<GenericRow> rows = verifyAvailableRows(queryMetadata, expectedUsers.size());

    final Set<Object> actualUsers = rows.stream()
        .filter(Objects::nonNull)
        .peek(row -> assertThat(row.getColumns(), hasSize(6)))
        .map(row -> row.getColumns().get(1))
        .collect(Collectors.toSet());

    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(actualUsers, is(expectedUsers));
  }

  @Test
  public void shouldSelectFromPageViewsWithSpecificColumn() throws Exception {
    final TransientQueryMetadata queryMetadata =
        executeQuery("SELECT pageid from %s;", PAGE_VIEW_STREAM);

    final List<String> expectedPages =
        Arrays.asList("PAGE_1", "PAGE_2", "PAGE_3", "PAGE_4", "PAGE_5", "PAGE_5", "PAGE_5");

    final List<GenericRow> rows = verifyAvailableRows(queryMetadata, expectedPages.size());

    final List<Object> actualPages = rows.stream()
        .filter(Objects::nonNull)
        .peek(row -> assertThat(row.getColumns(), hasSize(1)))
        .map(row -> row.getColumns().get(0))
        .collect(Collectors.toList());

    assertThat(actualPages.subList(0, expectedPages.size()), is(expectedPages));
    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
  }

  @Test
  public void shouldSelectAllFromDerivedStream() throws Exception {
    executeStatement(
        "CREATE STREAM pageviews_female"
        + " AS SELECT %s.userid AS userid, pageid, regionid, gender "
        + " FROM %s "
        + " LEFT JOIN %s ON %s.userid = %s.userid"
        + " WHERE gender = 'FEMALE';",
        USER_TABLE, PAGE_VIEW_STREAM, USER_TABLE, PAGE_VIEW_STREAM,
        USER_TABLE);

    final TransientQueryMetadata queryMetadata = executeQuery(
        "SELECT * from pageviews_female;");

    final List<KeyValue<String, GenericRow>> results = new ArrayList<>();
    final BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    // From the mock data, we expect exactly 3 page views from female users.
    final List<String> expectedPages = ImmutableList.of("PAGE_2", "PAGE_5", "PAGE_5");
    final List<String> expectedUsers = ImmutableList.of("USER_2", "USER_0", "USER_2");

    TestUtils.waitForCondition(() -> {
      try {
        log.debug("polling from pageviews_female");
        final KeyValue<String, GenericRow> nextRow = rowQueue.poll(1, TimeUnit.SECONDS);
        if (nextRow != null) {
          results.add(nextRow);
        } else {
          // If we didn't receive any records on the output topic for 8 seconds, it probably means that the join
          // failed because the table data wasn't populated when the stream data was consumed. We should just
          // re populate the stream data to try the join again.
          log.warn("repopulating {} because the join returned no results.", PAGE_VIEW_TOPIC);
          TEST_HARNESS.produceRows(
              PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, JSON, System::currentTimeMillis);
        }
      } catch (final Exception e) {
        throw new RuntimeException("Got exception when polling from pageviews_female", e);
      }
      return expectedPages.size() <= results.size();
    }, 30000, "Could not consume any records from " + PAGE_VIEW_TOPIC + " for 30 seconds");

    final List<String> actualPages = new ArrayList<>();
    final List<String> actualUsers = new ArrayList<>();

    for (final KeyValue<String, GenericRow> result : results) {
      final List<Object> columns = result.value.getColumns();
      log.debug("pageview join: {}", columns);

      assertThat(columns, hasSize(6));

      final String user = (String) columns.get(2);
      actualUsers.add(user);

      final String page = (String) columns.get(3);
      actualPages.add(page);
    }

    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(PRODUCED_COUNT.get(), greaterThan(0));
    assertThat(actualPages, is(expectedPages));
    assertThat(actualUsers, is(expectedUsers));
  }

  @Test
  public void shouldCreateStreamUsingLikeClause() throws Exception {

    executeStatement(
        "CREATE STREAM pageviews_like_p5"
        + " WITH (kafka_topic='pageviews_enriched_r0', value_format='DELIMITED')"
        + " AS SELECT * FROM %s"
        + " WHERE pageId LIKE '%%_5';",
        PAGE_VIEW_STREAM);

    final TransientQueryMetadata queryMetadata =
        executeQuery("SELECT userid, pageid from pageviews_like_p5;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(columns.get(1), is("PAGE_5"));
  }

  @Test
  public void shouldRetainSelectedColumnsInPartitionBy() throws Exception {

    executeStatement(
        "CREATE STREAM pageviews_by_viewtime "
        + "AS SELECT viewtime, pageid, userid "
        + "from %s "
        + "partition by viewtime;",
        PAGE_VIEW_STREAM);

    final TransientQueryMetadata queryMetadata = executeQuery(
        "SELECT * from pageviews_by_viewtime;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(PRODUCED_COUNT.get(), greaterThan(0));
    assertThat(columns.get(3).toString(), startsWith("PAGE_"));
    assertThat(columns.get(4).toString(), startsWith("USER_"));
  }

  @Test
  public void shouldSupportDroppingAndRecreatingJoinQuery() throws Exception {
    final String createStreamStatement = format(
        "create stream cart_event_product as "
        + "select pv.pageid, u.gender "
        + "from %s pv left join %s u on pv.userid=u.userid;",
        PAGE_VIEW_STREAM, USER_TABLE);

    executeStatement(createStreamStatement);

    ksqlContext.terminateQuery(new QueryId("CSAS_CART_EVENT_PRODUCT_0"));

    executeStatement("DROP STREAM CART_EVENT_PRODUCT;");

    executeStatement(createStreamStatement);

    final TransientQueryMetadata queryMetadata = executeQuery(
        "SELECT * from cart_event_product;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(PRODUCED_COUNT.get(), greaterThan(0));
    assertThat(columns.get(1).toString(), startsWith("USER_"));
    assertThat(columns.get(2).toString(), startsWith("PAGE_"));
    assertThat(columns.get(3).toString(), either(is("FEMALE")).or(is("MALE")));
  }

  @Test
  public void shouldCleanUpAvroSchemaOnDropSource() throws Exception {
    final String topicName = "avro_stream_topic";

    executeStatement(format(
        "create stream avro_stream with (kafka_topic='%s',value_format='avro') as select * from %s;",
        topicName,
        PAGE_VIEW_STREAM));

    TEST_HARNESS.produceRows(
        PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, JSON, System::currentTimeMillis);

    TEST_HARNESS.waitForSubjectToBePresent(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

    ksqlContext.terminateQuery(new QueryId("CSAS_AVRO_STREAM_0"));

    executeStatement("DROP STREAM avro_stream DELETE TOPIC;");

    TEST_HARNESS.waitForSubjectToBeAbsent(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldSupportConfigurableUdfs() throws Exception {
    // When:
    final TransientQueryMetadata queryMetadata = executeQuery(
        "SELECT E2EConfigurableUdf(registertime) AS x from %s;", USER_TABLE);

    // Then:
    final List<GenericRow> rows = verifyAvailableRows(queryMetadata, 5);

    assertThat(ConfigurableUdf.PASSED_CONFIG, is(ImmutableMap.of(
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "e2econfigurableudf.some.setting",
        "foo-bar",
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.expected-param",
        "expected-value"
    )));

    rows.forEach(row -> assertThat(row.getColumns().get(0), is(-1L)));
  }

  private QueryMetadata executeStatement(final String statement,
      final String... args) {
    final String formatted = String.format(statement, (Object[])args);
    log.debug("Sending statement: {}", formatted);

    final List<QueryMetadata> queries = ksqlContext.sql(formatted);

    queries.stream()
        .filter(q -> !(q instanceof PersistentQueryMetadata))
        .forEach(QueryMetadata::start);

    return queries.isEmpty() ? null : queries.get(0);
  }

  private TransientQueryMetadata executeQuery(final String statement,
      final String... args) {
    final QueryMetadata queryMetadata = executeStatement(statement, args);
    assertThat(queryMetadata, instanceOf(TransientQueryMetadata.class));
    toClose = queryMetadata;
    return (TransientQueryMetadata) queryMetadata;
  }

  private static List<Object> waitForFirstRow(
      final TransientQueryMetadata queryMetadata) throws Exception {
    return verifyAvailableRows(queryMetadata, 1).get(0).getColumns();
  }

  private static List<GenericRow> verifyAvailableRows(
      final TransientQueryMetadata queryMetadata,
      final int expectedRows
  ) throws Exception {
    final BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    TestUtils.waitForCondition(
        () -> rowQueue.size() >= expectedRows,
        30_000,
        expectedRows + " rows were not available after 30 seconds");

    final List<KeyValue<String, GenericRow>> rows = new ArrayList<>();
    rowQueue.drainTo(rows);

    return rows.stream()
        .map(kv -> kv.value)
        .collect(Collectors.toList());
  }

  public static class DummyConsumerInterceptor implements ConsumerInterceptor {

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public ConsumerRecords onConsume(final ConsumerRecords consumerRecords) {
      CONSUMED_COUNT.addAndGet(consumerRecords.count());
      return consumerRecords;
    }

    public void close() {
    }

    public void onCommit(final Map map) {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  public static class DummyProducerInterceptor implements ProducerInterceptor {

    public void onAcknowledgement(final RecordMetadata rm, final Exception e) {
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public ProducerRecord onSend(final ProducerRecord producerRecord) {
      PRODUCED_COUNT.incrementAndGet();
      return producerRecord;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> map) {
    }
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection in test.
  @UdfDescription(
      name = "E2EConfigurableUdf",
      description = "A test-only UDF for testing udfs work end-to-end and configure() is called")
  public static class ConfigurableUdf implements Configurable {
    private static Map<String, ?> PASSED_CONFIG = null;

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    @Override
    public void configure(final Map<String, ?> map) {
      PASSED_CONFIG = map;
    }

    @Udf
    public long foo(final long bar) {
      return -1L;
    }
  }
}
