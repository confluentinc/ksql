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

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_FUNCTIONS_PROPERTY_PREFIX;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.util.UserDataProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test emulates the end to end flow in the quick start guide and ensures that the outputs at each stage
 * are what we expect. This tests a broad set of KSQL functionality and is a good catch-all.
 */
@SuppressWarnings("ConstantConditions")
@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class EndToEndIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String USERS_TOPIC = "users";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final String USER_TABLE = "users_original";

  private static final Format KEY_FORMAT = KAFKA;
  private static final Format VALUE_FORMAT = JSON;
  private static final UserDataProvider USER_DATA_PROVIDER = new UserDataProvider();

  private static final AtomicInteger CONSUMED_COUNT = new AtomicInteger();
  private static final AtomicInteger PRODUCED_COUNT = new AtomicInteger();
  private static final PageViewDataProvider PAGE_VIEW_DATA_PROVIDER = new PageViewDataProvider();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @SuppressWarnings("deprecation")
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(
        false, true
    );
  }

  @Parameterized.Parameter
  public boolean sharedRuntimes;

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  public TestKsqlContext ksqlContext;

  @Rule
  public final Timeout timeout = Timeout.seconds(120);

  private final List<QueryMetadata> toClose = new ArrayList<>();

  @Before
  public void before() throws Exception {
    TEST_HARNESS.before();
    ksqlContext  = TEST_HARNESS.ksqlContextBuilder()
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
        .withAdditionalConfig(
            KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
            "http://foo:8080")
        .withAdditionalConfig(
            KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED,
            sharedRuntimes)
        .build();

    ksqlContext.before();

    ConfigurableUdf.PASSED_CONFIG = null;
    PRODUCED_COUNT.set(0);
    CONSUMED_COUNT.set(0);
    toClose.clear();

    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC, USERS_TOPIC);

    TEST_HARNESS.produceRows(
        USERS_TOPIC,
        USER_DATA_PROVIDER,
        KEY_FORMAT,
        VALUE_FORMAT,
        () -> System.currentTimeMillis() - 10000
    );

    TEST_HARNESS.produceRows(
        PAGE_VIEW_TOPIC,
        PAGE_VIEW_DATA_PROVIDER,
        KEY_FORMAT,
        VALUE_FORMAT,
        System::currentTimeMillis
    );

    ksqlContext.sql("CREATE TABLE " + USER_TABLE
        + " (userid varchar PRIMARY KEY, registertime bigint, gender varchar, regionid varchar)"
        + " WITH (kafka_topic='" + USERS_TOPIC + "', value_format='JSON');");

    ksqlContext.sql("CREATE STREAM " + PAGE_VIEW_STREAM
        + " (pageid varchar KEY, viewtime bigint, userid varchar) "
        + "WITH (kafka_topic='" + PAGE_VIEW_TOPIC + "', value_format='JSON');");
  }

  @After
  public void after() {
    toClose.forEach(QueryMetadata::close);
    ksqlContext.after();
    TEST_HARNESS.deleteTopics(Arrays.asList(PAGE_VIEW_TOPIC, USERS_TOPIC));
    TEST_HARNESS.after();
  }

  @Test
  public void shouldSelectAllFromUsers() throws Exception {
    final TransientQueryMetadata queryMetadata = executeStatement("SELECT * from %s EMIT CHANGES;", USER_TABLE);

    final Set<String> expectedUsers = USER_DATA_PROVIDER.data().keySet().stream()
        .map(s -> (String) s.get(0))
        .collect(Collectors.toSet());

    final List<GenericRow> rows = verifyAvailableRows(queryMetadata, expectedUsers.size());

    final Set<Object> actualUsers = rows.stream()
        .filter(Objects::nonNull)
        .peek(row -> assertThat(row.values(), hasSize(4)))
        .map(row -> row.get(0))
        .collect(Collectors.toSet());

    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(actualUsers, is(expectedUsers));
  }

  @Test
  public void shouldSupportDroppingAndRecreatingJoinQuery() throws Exception {
    final String createStreamStatement = format(
        "create stream cart_event_product as "
        + "select pv.userid, pv.pageid, u.gender "
        + "from %s pv left join %s u on pv.userid=u.userid;",
        PAGE_VIEW_STREAM, USER_TABLE);

    executeStatement(createStreamStatement);

    ksqlContext.terminateQuery(new QueryId("CSAS_CART_EVENT_PRODUCT_0"));

    executeStatement("DROP STREAM CART_EVENT_PRODUCT;");

    executeStatement(createStreamStatement);

    final TransientQueryMetadata queryMetadata = executeStatement(
        "SELECT * from cart_event_product EMIT CHANGES;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    if (sharedRuntimes) {
      assertThat(TEST_HARNESS.getKafkaCluster().getTopics(),
          hasItem("_confluent-ksql-default_query-CSAS_CART_EVENT_PRODUCT_1-Join-repartition"));
    } else {
      assertThat(TEST_HARNESS.getKafkaCluster().getTopics(),
          hasItem("_confluent-ksql-default_query_CSAS_CART_EVENT_PRODUCT_1-Join-repartition"));
    }
    assertThat(CONSUMED_COUNT.get(), greaterThan(0));
    assertThat(PRODUCED_COUNT.get(), greaterThan(0));
    assertThat(columns.get(0).toString(), startsWith("USER_"));
    assertThat(columns.get(1).toString(), startsWith("PAGE_"));
    assertThat(columns.get(2).toString(), either(is("FEMALE")).or(is("MALE")));
  }

  @Test
  public void shouldCleanUpAvroSchemaOnDropSource() throws Exception {
    final String topicName = "avro_stream_topic";

    executeStatement(format(
        "create stream avro_stream with (kafka_topic='%s',format='avro') as select * from %s;",
        topicName,
        PAGE_VIEW_STREAM));

    TEST_HARNESS.produceRows(
        PAGE_VIEW_TOPIC, PAGE_VIEW_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT, System::currentTimeMillis);

    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(topicName, true));
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(topicName, false));

    ksqlContext.terminateQuery(new QueryId("CSAS_AVRO_STREAM_0"));

    executeStatement("DROP STREAM avro_stream DELETE TOPIC;");

    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(topicName, true));
    TEST_HARNESS.waitForSubjectToBeAbsent(KsqlConstants.getSRSubject(topicName, false));
  }

  @Test
  public void shouldRegisterCorrectPrimitiveSchemaForCreateStatements() throws Exception {
    // Given:
    final String topicName = "create_stream_topic";

    // When:
    executeStatement("create stream s ("
        + "  K INT KEY,"
        + "  VAL INT"
        + ") with ("
        + "  kafka_topic = '" + topicName + "',"
        + "  partitions = 1,"
        + "  format = 'avro',"
        + "  wrap_single_value = false);"
    );

    // Then:
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(topicName, false));
    TEST_HARNESS.waitForSubjectToBePresent(KsqlConstants.getSRSubject(topicName, true));

    assertThat(TEST_HARNESS.getSchema(KsqlConstants.getSRSubject(topicName, true)), is(new AvroSchema("{\"type\":\"int\"}")));
    assertThat(TEST_HARNESS.getSchema(KsqlConstants.getSRSubject(topicName, false)), is(new AvroSchema("{\"type\":\"int\"}")));
  }

  @Test
  public void shouldRegisterCorrectPrimitiveSchemaForCreateAsStatements() throws Exception {
    // Given:
    final String topicName = "create_as_stream_topic";

    executeStatement("create stream s with ("
        + "  kafka_topic = '" + topicName + "',"
        + "  format = 'avro',"
        + "  wrap_single_value = false"
        + ") as "
        + "select pageid, viewtime from " + PAGE_VIEW_STREAM + ";"
    );

    // Then:
    final String valSubject = KsqlConstants.getSRSubject(topicName, false);
    final String keySubject = KsqlConstants.getSRSubject(topicName, true);

    TEST_HARNESS.waitForSubjectToBePresent(keySubject);
    TEST_HARNESS.waitForSubjectToBePresent(valSubject);

    assertThat(TEST_HARNESS.getSchema(keySubject), is(new AvroSchema("{\"type\":\"string\"}")));
    assertThat(TEST_HARNESS.getSchema(valSubject), is(new AvroSchema("{\"type\":\"long\"}")));
  }

  @Test
  public void shouldSupportConfigurableUdfs() throws Exception {
    // When:
    final TransientQueryMetadata queryMetadata = executeStatement(
        "SELECT E2EConfigurableUdf(registertime) AS x from %s EMIT CHANGES;", USER_TABLE);

    // Then:
    final List<GenericRow> rows = verifyAvailableRows(queryMetadata, 5);

    assertThat(ConfigurableUdf.PASSED_CONFIG, is(ImmutableMap.of(
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "e2econfigurableudf.some.setting",
        "foo-bar",
        KSQL_FUNCTIONS_PROPERTY_PREFIX + "_global_.expected-param",
        "expected-value"
    )));

    rows.forEach(row -> assertThat(row.get(0), is(-1L)));
  }

  @SuppressWarnings("unchecked")
  private <T extends QueryMetadata> T executeStatement(
      final String statement,
      final String... args
  ) {
    final String formatted = format(statement, (Object[])args);

    final List<QueryMetadata> queries = ksqlContext.sql(formatted);

    final List<QueryMetadata> newQueries = queries.stream()
        .filter(q -> !(q instanceof PersistentQueryMetadata))
        .collect(Collectors.toList());

    newQueries.forEach(QueryMetadata::start);

    toClose.addAll(newQueries);

    return queries.isEmpty() ? null : (T) queries.get(0);
  }

  private static List<Object> waitForFirstRow(
      final TransientQueryMetadata queryMetadata
  ) {
    return verifyAvailableRows(queryMetadata, 1).get(0).values();
  }

  private static List<GenericRow> verifyAvailableRows(
      final TransientQueryMetadata queryMetadata,
      final int expectedRows
  ) {
    final BlockingRowQueue rowQueue = queryMetadata.getRowQueue();

    assertThatEventually(
        expectedRows + " rows were not available after 30 seconds",
        () -> rowQueue.size() >= expectedRows,
        is(true),
        30,
        TimeUnit.SECONDS
    );

    final List<KeyValueMetadata<List<?>, GenericRow>> rows = new ArrayList<>();
    rowQueue.drainTo(rows);

    return rows.stream()
        .map(kvm -> kvm.getKeyValue().value())
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
