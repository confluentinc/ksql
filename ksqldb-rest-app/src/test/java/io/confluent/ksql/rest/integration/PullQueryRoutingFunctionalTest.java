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
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.FaultyKafka.FaultyKafka0;
import io.confluent.ksql.rest.integration.FaultyKafka.FaultyKafka1;
import io.confluent.ksql.rest.integration.FaultyKafka.FaultyKafka2;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.UserDataProvider;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.commons.lang3.tuple.Triple;
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
//    TestProxy.runInBackground("localhost", 9093, "localhost", 9092, () -> false);
  }

  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query with ID (\\S+)");
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8188);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8189);
  private static final KsqlHostInfoEntity host2 = new KsqlHostInfoEntity("localhost", 8187);
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

  private static final String STATE_STORE = "Aggregate-Aggregate-Materialize";
  private static final long HOST_CURRENT_OFFSET = 25;
  private static final long HOST_END_OFFSET = 75;
  private static final long HOST_LAG = 50;
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

  private static final Shutoffs APP0_SHUTOFFS = new Shutoffs();
  private static final Shutoffs APP1_SHUTOFFS = new Shutoffs();
  private static final Shutoffs APP2_SHUTOFFS = new Shutoffs();

  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8188")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8188")
      .withProperties(COMMON_CONFIG)
      .withEnabledKsqlClient(APP0_SHUTOFFS.ksqlOutgoing::get)
//      .withTestProxy("localhost", 8288, "localhost", 8188, APP0_CUTOFF::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafka0.class.getName())
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8189")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8189")
      .withProperties(COMMON_CONFIG)
      .withEnabledKsqlClient(APP1_SHUTOFFS.ksqlOutgoing::get)
//      .withTestProxy("localhost", 8289, "localhost", 8189, APP1_CUTOFF::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafka1.class.getName())
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_2 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8087")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:8187")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8187")
      .withProperties(COMMON_CONFIG)
      .withEnabledKsqlClient(APP2_SHUTOFFS.ksqlOutgoing::get)
//      .withTestProxy("localhost", 8287, "localhost", 8187, APP2_CUTOFF::get)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafka2.class.getName())
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
    FaultyKafka0.DISABLE = APP0_SHUTOFFS.kafkaIncoming::get;
    FaultyKafka1.DISABLE = APP1_SHUTOFFS.kafkaIncoming::get;
    FaultyKafka2.DISABLE = APP2_SHUTOFFS.kafkaIncoming::get;
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
    APP0_SHUTOFFS.reset();
    APP1_SHUTOFFS.reset();
    APP2_SHUTOFFS.reset();
  }

  @AfterClass
  public static void classTearDown() {
    TMP.delete();
  }

  @Test
  public void shouldQueryActiveWhenActiveAliveQueryIssuedToStandby() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.standBy.getMiddle(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest::lagsExist);

    waitForRemoteServerToChangeStatus(
        clusterFormation.standBy.getMiddle(),
        clusterFormation.active.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    Pair<URI, List<StreamedRow>> result =
        makePullQueryRequest(clusterFormation.standBy.getMiddle(), sql);
    final List<StreamedRow> rows_0 = result.getRight();
    final URI host = result.getLeft();

    // Then:
    assertThat(host.getHost(), is(clusterFormation.active.getLeft().getHost()));
    assertThat(host.getPort(), is(clusterFormation.active.getLeft().getPort()));
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }


  @Test
  public void shouldQueryActiveWhenActiveAliveStandbyDeadQueryIssuedToRouter() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getMiddle(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest::lagsExist);

    // Partition off the standby
    clusterFormation.standBy.getRight().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.active.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.standBy.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final Pair<URI, List<StreamedRow>> result = makePullQueryRequest(clusterFormation.router.getMiddle(), sql);
    final List<StreamedRow> rows_0 = result.getRight();
    final URI host = result.getLeft();

    // Then:
    assertThat(host.getHost(), is(clusterFormation.active.getLeft().getHost()));
    assertThat(host.getPort(), is(clusterFormation.active.getLeft().getPort()));
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }

  @Test
  public void shouldQueryStandbyWhenActiveDeadStandbyAliveQueryIssuedToRouter() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getMiddle(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest::lagsExist);

    // Partition off the active
    clusterFormation.active.getRight().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.standBy.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.active.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final Pair<URI, List<StreamedRow>> result = makePullQueryRequest(clusterFormation.router.getMiddle(), sql);
    final List<StreamedRow> rows_0 = result.getRight();
    final URI host = result.getLeft();

    // Then:
    assertThat(host.getHost(), is(clusterFormation.standBy.getLeft().getHost()));
    assertThat(host.getPort(), is(clusterFormation.standBy.getLeft().getPort()));
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));
  }

  @Test
  public void shouldFilterLaggyServers() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    System.out.println("clusterFormation " + clusterFormation.toString());
    waitForClusterToBeDiscovered(clusterFormation.router.getMiddle(), 3);
    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest::lagsExist);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.active.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.standBy.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);

    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest.lagsExist(clusterFormation.standBy.getLeft(), 5));

    // Cut off standby from Kafka to simulate lag
    System.out.println("Setting kafkaIncoming true for " + clusterFormation.standBy.getLeft().toString());
    clusterFormation.standBy.getRight().kafkaIncoming.set(true);
    Thread.sleep(2000);

    // Produce more data that will now only be available on active since standby is cut off
    TEST_HARNESS.produceRows(
        topic,
        USER_PROVIDER,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );

    // Make sure that the lags get reported before we kill active
    waitForRemoteServerToChangeStatus(clusterFormation.router.getMiddle(),
        clusterFormation.router.getLeft(),
        PullQueryRoutingFunctionalTest.lagsExist(clusterFormation.active.getLeft(), 10));

    // Partition active off
    clusterFormation.active.getRight().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.standBy.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getMiddle(),
        clusterFormation.active.getLeft(),
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final Pair<URI, List<StreamedRow>> result = makePullQueryRequest(
        clusterFormation.router.getMiddle(), sql,
        LAG_FILTER_6);
    final List<StreamedRow> rows_0 = result.getRight();
    final URI host = result.getLeft();

    // Then:
    assertThat(host.getHost(), is(clusterFormation.standBy.getLeft().getHost()));
    assertThat(host.getPort(), is(clusterFormation.standBy.getLeft().getPort()));
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    // This line ensures that we've not processed the new data
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, 1)));

    KsqlErrorMessage errorMessage = makePullQueryRequestWithError(clusterFormation.router.getMiddle(),
        sql, LAG_FILTER_3);
    Assert.assertEquals(40001, errorMessage.getErrorCode());
    Assert.assertTrue(errorMessage.getMessage().contains("All nodes are dead or exceed max allowed lag."));
  }

  private Pair<URI, List<StreamedRow>> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeQueryRequestWithRespondingHost(target, sql, Optional.empty(),
        null);
  }

  private static void makeAdminRequest(TestKsqlRestApp restApp, final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private List<KsqlEntity> makeAdminRequestWithResponse(
      TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private static Pair<URI, List<StreamedRow>> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequestWithRespondingHost(target, sql, Optional.empty(), properties);
  }

  private static KsqlErrorMessage makePullQueryRequestWithError(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequestWithError(target, sql, Optional.empty(),
        properties);
  }

  private Map<QueryStateStoreId, StateStoreLags> createLagMap(long current, long end, long lag) {
    final String queryId = "_confluent-ksql-default_query_" + QUERY_ID;
    final QueryStateStoreId queryStateStoreId =
        QueryStateStoreId.of(queryId, STATE_STORE);
    return ImmutableMap.<QueryStateStoreId, StateStoreLags>builder()
        .put(queryStateStoreId, new StateStoreLags(ImmutableMap.<Integer, LagInfoEntity>builder()
            .put(0, new LagInfoEntity(current, end, lag))
            .build()))
        .build();
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
      clusterFormation.setActive(Triple.of(host0, restApp0, APP0_SHUTOFFS));
    } else if (!entity1.getActiveStores().isEmpty() && !entity1.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(Triple.of(host1, restApp1, APP1_SHUTOFFS));
    } else {
      clusterFormation.setActive(Triple.of(host2, restApp2, APP2_SHUTOFFS));
    }

    //find standby
    if (!entity0.getStandByStores().isEmpty() && !entity0.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Triple.of(host0, restApp0, APP0_SHUTOFFS));
    } else if (!entity1.getStandByStores().isEmpty() && !entity1.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Triple.of(host1, restApp1, APP1_SHUTOFFS));
    } else {
      clusterFormation.setStandBy(Triple.of(host2, restApp2, APP2_SHUTOFFS));
    }

    //find router
    if (entity0.getStandByStores().isEmpty() && entity0.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Triple.of(host0, restApp0, APP0_SHUTOFFS));
    } else if (entity1.getStandByStores().isEmpty() && entity1.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Triple.of(host1, restApp1, APP1_SHUTOFFS));
    } else {
      clusterFormation.setRouter(Triple.of(host2, restApp2, APP2_SHUTOFFS));
    }

    return clusterFormation;
  }

  static class ClusterFormation {
    Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> active;
    Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> standBy;
    Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> router;

    ClusterFormation() {
    }

    public void setActive(final Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> active) {
      this.active = active;
    }

    public void setStandBy(final Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> standBy) {
      this.standBy = standBy;
    }

    public void setRouter(final Triple<KsqlHostInfoEntity, TestKsqlRestApp, Shutoffs> router) {
      this.router = router;
    }

    public String toString() {
      return new StringBuilder()
          .append("Active = ").append(active.getLeft())
          .append(", Standby = ").append(standBy.getLeft())
          .append(", Router = ").append(router.getLeft())
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

