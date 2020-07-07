/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer0;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer1;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer2;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.UserDataProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to ensure pull queries route across multiple KSQL nodes correctly.
 *
 * <p>For tests on general syntax and handled see RestQueryTranslationTest's
 * materialized-aggregate-static-queries.json
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
public class PullQueryRoutingFunctionalTest {
  private static final Logger LOG = LoggerFactory.getLogger(PullQueryRoutingFunctionalTest.class);

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query with ID (\\S+)");
  private static final String USER_TOPIC = "user_topic_";
  private static final String USERS_STREAM = "users";
  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final int HEADER = 1;
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final int BASE_TIME = 1_000_000;
  private final static String KEY = Iterables.get(USER_PROVIDER.data().keySet(), 0);
  private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
  private String output;
  private String QUERY_ID;
  private String sql;
  private String topic;

  private static final Map<String, ?> LAG_FILTER_6 =
      ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG, "6");
  private static final Map<String, ?> LAG_FILTER_3 =
      ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG, "3");

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final Map<String, Object> COMMON_CONFIG = ImmutableMap.<String, Object>builder()
      .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 500)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .put(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
      .put(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
      .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      .build();

  private static final Shutoffs APP_SHUTOFFS_0 = new Shutoffs();
  private static final Shutoffs APP_SHUTOFFS_1 = new Shutoffs();
  private static final Shutoffs APP_SHUTOFFS_2 = new Shutoffs();

  private static final int INT_PORT_0 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT_1 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT_2 = TestUtils.findFreeLocalPort();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", INT_PORT_0);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", INT_PORT_1);
  private static final KsqlHostInfoEntity host2 = new KsqlHostInfoEntity("localhost", INT_PORT_2);

  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
      .withFaultyKsqlClient(APP_SHUTOFFS_0.ksqlOutgoing::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
          + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer0.class.getName())
      .withProperties(COMMON_CONFIG)
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_1)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_1)
      .withFaultyKsqlClient(APP_SHUTOFFS_1.ksqlOutgoing::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
          + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer1.class.getName())
      .withProperties(COMMON_CONFIG)
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_2 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_2)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_2)
      .withFaultyKsqlClient(APP_SHUTOFFS_2.ksqlOutgoing::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
          + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer2.class.getName())
      .withProperties(COMMON_CONFIG)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(1, TimeUnit.MINUTES)
      .withLookingForStuckThread(true)
      .build();

  @BeforeClass
  public static void setUpClass() {
    FaultyKafkaConsumer0.setDisable(APP_SHUTOFFS_0.kafkaIncoming::get);
    FaultyKafkaConsumer1.setDisable(APP_SHUTOFFS_1.kafkaIncoming::get);
    FaultyKafkaConsumer2.setDisable(APP_SHUTOFFS_2.kafkaIncoming::get);
  }

  @Before
  public void setUp() {
    //Create topic with 1 partition to control who is active and standby
    topic = USER_TOPIC + KsqlIdentifierTestUtil.uniqueIdentifierName();
    TEST_HARNESS.ensureTopics(1, topic);

    TEST_HARNESS.produceRows(
        topic,
        USER_PROVIDER,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );

    //Create stream
    makeAdminRequest(
        REST_APP_0,
        "CREATE STREAM " + USERS_STREAM
            + " (" + USER_PROVIDER.ksqlSchemaString(false) + ")"
            + " WITH ("
            + "   kafka_topic='" + topic + "', "
            + "   value_format='JSON');"
    );
    //Create table
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();
    sql = "SELECT * FROM " + output + " WHERE USERID = '" + KEY + "';";
    List<KsqlEntity> res = makeAdminRequestWithResponse(
        REST_APP_0,
        "CREATE TABLE " + output + " AS"
            + " SELECT " + USER_PROVIDER.key() +  ", COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    QUERY_ID = extractQueryId(res.get(0).toString());
    QUERY_ID = QUERY_ID.substring(0, QUERY_ID.length() - 1);
    waitForTableRows();
    waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0, host1, host2), QUERY_ID);
  }

  @After
  public void cleanUp() {
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept();
    APP_SHUTOFFS_0.reset();
    APP_SHUTOFFS_1.reset();
    APP_SHUTOFFS_2.reset();
  }

  @AfterClass
  public static void classTearDown() {
    TMP.delete();
  }

  @Test
  public void shouldQueryActiveWhenActiveAliveQueryIssuedToStandby() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.standBy.getApp(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest::lagsExist);

    waitForRemoteServerToChangeStatus(
        clusterFormation.standBy.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    List<StreamedRow> rows_0 =
        makePullQueryRequest(clusterFormation.standBy.getApp(), sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    KsqlHostInfoEntity host = rows_0.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(clusterFormation.active.getHost().getHost()));
    assertThat(host.getPort(), is(clusterFormation.active.getHost().getPort()));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }


  @Test
  public void shouldQueryActiveWhenActiveAliveStandbyDeadQueryIssuedToRouter() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest::lagsExist);

    // Partition off the standby
    clusterFormation.standBy.getShutoffs().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.getApp(), sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    KsqlHostInfoEntity host = rows_0.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(clusterFormation.active.getHost().getHost()));
    assertThat(host.getPort(), is(clusterFormation.active.getHost().getPort()));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }

  @Test
  public void shouldQueryStandbyWhenActiveDeadStandbyAliveQueryIssuedToRouter() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest::lagsExist);

    // Partition off the active
    clusterFormation.active.getShutoffs().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.getApp(), sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    KsqlHostInfoEntity host = rows_0.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(clusterFormation.standBy.getHost().getHost()));
    assertThat(host.getPort(), is(clusterFormation.standBy.getHost().getPort()));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }

  @Test
  public void shouldFilterLaggyServers() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest::lagsExist);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);

    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest.lagsExist(clusterFormation.standBy.getHost(), 5));

    // Cut off standby from Kafka to simulate lag
    clusterFormation.standBy.getShutoffs().kafkaIncoming.set(true);
    Thread.sleep(2000);

    // Produce more data that will now only be available on active since standby is cut off
    TEST_HARNESS.produceRows(
        topic,
        USER_PROVIDER,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );

    // Make sure that the lags get reported before we kill active
    waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        PullQueryRoutingFunctionalTest.lagsExist(clusterFormation.active.getHost(), 10));

    // Partition active off
    clusterFormation.active.getShutoffs().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(
        clusterFormation.router.getApp(), sql,
        LAG_FILTER_6);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    KsqlHostInfoEntity host = rows_0.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(clusterFormation.standBy.getHost().getHost()));
    assertThat(host.getPort(), is(clusterFormation.standBy.getHost().getPort()));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    // This line ensures that we've not processed the new data
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));

    KsqlErrorMessage errorMessage = makePullQueryRequestWithError(clusterFormation.router.getApp(),
        sql, LAG_FILTER_3);
    Assert.assertEquals(40001, errorMessage.getErrorCode());
    Assert.assertTrue(errorMessage.getMessage().contains("All nodes are dead or exceed max allowed lag."));
  }

  private List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty(),
        null, ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true));
  }

  private static void makeAdminRequest(TestKsqlRestApp restApp, final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private List<KsqlEntity> makeAdminRequestWithResponse(
      TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty(),
        properties, ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true));
  }

  private static KsqlErrorMessage makePullQueryRequestWithError(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequestWithError(target, sql, Optional.empty(),
        properties);
  }

  private ClusterFormation findClusterFormation(
      TestKsqlRestApp restApp0, TestKsqlRestApp restApp1, TestKsqlRestApp restApp2) {
    ClusterFormation clusterFormation = new ClusterFormation();
    ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil.sendClusterStatusRequest(restApp0);
    ActiveStandbyEntity entity0 = clusterStatusResponse.getClusterStatus().get(host0)
        .getActiveStandbyPerQuery().get(QUERY_ID);
    ActiveStandbyEntity entity1 = clusterStatusResponse.getClusterStatus().get(host1)
        .getActiveStandbyPerQuery().get(QUERY_ID);

    // find active
    if (!entity0.getActiveStores().isEmpty() && !entity0.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(new TestApp(host0, restApp0, APP_SHUTOFFS_0));
    } else if (!entity1.getActiveStores().isEmpty() && !entity1.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(new TestApp(host1, restApp1, APP_SHUTOFFS_1));
    } else {
      clusterFormation.setActive(new TestApp(host2, restApp2, APP_SHUTOFFS_2));
    }

    //find standby
    if (!entity0.getStandByStores().isEmpty() && !entity0.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(new TestApp(host0, restApp0, APP_SHUTOFFS_0));
    } else if (!entity1.getStandByStores().isEmpty() && !entity1.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(new TestApp(host1, restApp1, APP_SHUTOFFS_1));
    } else {
      clusterFormation.setStandBy(new TestApp(host2, restApp2, APP_SHUTOFFS_2));
    }

    //find router
    if (entity0.getStandByStores().isEmpty() && entity0.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(new TestApp(host0, restApp0, APP_SHUTOFFS_0));
    } else if (entity1.getStandByStores().isEmpty() && entity1.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(new TestApp(host1, restApp1, APP_SHUTOFFS_1));
    } else {
      clusterFormation.setRouter(new TestApp(host2, restApp2, APP_SHUTOFFS_2));
    }

    return clusterFormation;
  }

  static class ClusterFormation {
    TestApp active;
    TestApp standBy;
    TestApp router;

    ClusterFormation() {
    }

    public void setActive(final TestApp active) {
      this.active = active;
    }

    public void setStandBy(final TestApp standBy) {
      this.standBy = standBy;
    }

    public void setRouter(final TestApp router) {
      this.router = router;
    }

    public String toString() {
      return new StringBuilder()
          .append("Active = ").append(active.getHost())
          .append(", Standby = ").append(standBy.getHost())
          .append(", Router = ").append(router.getHost())
          .toString();
    }
  }

  private void waitForTableRows() {
    TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_PROVIDER.data().size(),
        FormatFactory.JSON,
        AGGREGATE_SCHEMA
    );
  }

  private String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }

  private String extractQueryId(final String outputString) {
    final java.util.regex.Matcher matcher = QUERY_ID_PATTERN.matcher(outputString);
    assertThat("Could not find query id in: " + outputString, matcher.find());
    return matcher.group(1);
  }

  public static class TestApp {

    private final KsqlHostInfoEntity host;
    private final TestKsqlRestApp app;
    private final Shutoffs shutoffs;

    public TestApp(KsqlHostInfoEntity host, TestKsqlRestApp app, Shutoffs shutoffs) {
      this.host = host;
      this.app = app;
      this.shutoffs = shutoffs;
    }

    public KsqlHostInfoEntity getHost() {
      return host;
    }

    public TestKsqlRestApp getApp() {
      return app;
    }

    public Shutoffs getShutoffs() {
      return shutoffs;
    }
  }

  public static class Shutoffs {
    private final AtomicBoolean ksqlOutgoing = new AtomicBoolean(false);
    private final AtomicBoolean kafkaIncoming = new AtomicBoolean(false);

    public void shutOffAll() {
      ksqlOutgoing.set(true);
      kafkaIncoming.set(true);
    }

    public void reset() {
      ksqlOutgoing.set(false);
      kafkaIncoming.set(false);
    }
  }

  static boolean lagsExist(
      final KsqlHostInfoEntity remoteServer,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    if (clusterStatus.size() == 3) {
      int numWithLag = 0;
      for (Map.Entry<KsqlHostInfoEntity, HostStatusEntity> e : clusterStatus.entrySet()) {
        if (e.getValue().getHostStoreLags().getStateStoreLags().size() > 0) {
          numWithLag++;
        }
      }
      if (numWithLag >= 2) {
        LOG.info("Found expected lags: {}", clusterStatus.toString());
        return true;
      }
    }
    LOG.info("Didn't yet find expected lags: {}", clusterStatus.toString());
    return false;
  }

  static BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean>
  lagsExist(
      final KsqlHostInfoEntity server,
      final long endOffset
  ) {
    return (remote, clusterStatus) -> {
      if (clusterStatus.size() == 3) {
        HostStatusEntity hostStatusEntity = clusterStatus.get(server);
        if (hostStatusEntity == null) {
          LOG.info("Didn't find {}", server.toString());
          return false;
        }
        long end = hostStatusEntity.getHostStoreLags().getStateStoreLags().values().stream()
            .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition().values().stream())
            .mapToLong(LagInfoEntity::getEndOffsetPosition)
            .max()
            .orElse(0);
        if (end >= endOffset) {
          LOG.info("Found expected end offset {} for {}: {}", endOffset, server,
              clusterStatus.toString());
          return true;
        }
      }
      LOG.info("Didn't yet find expected end offset {} for {}: {}", endOffset, server,
          clusterStatus.toString());
      return false;
    };
  }
}

