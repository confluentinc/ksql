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

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.sendHeartbeartsForWindowLength;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.sendLagReportingRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
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
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.entity.StreamedRow;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import kafka.zookeeper.ZooKeeperClientException;
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
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Test to ensure pull queries route across multiple KSQL nodes correctly.
 *
 * <p>For tests on general syntax and handled see RestQueryTranslationTest's
 * materialized-aggregate-static-queries.json
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
public class PullQueryRoutingFunctionalTest {

  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("with query ID: (\\S+)");
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8089);
  private static final KsqlHostInfoEntity host2 = new KsqlHostInfoEntity("localhost", 8087);
  private static final String USER_TOPIC = "user_topic";
  private static final String USERS_STREAM = "users";
  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final int HEADER = 1;
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final int BASE_TIME = 1_000_000;
  private final static String KEY = Iterables.get(USER_PROVIDER.data().keySet(), 0);
  private String output;
  private String QUERY_ID;
  private String sql;

  private static final String STATE_STORE = "Aggregate-Aggregate-Materialize";
  private static final long HOST_CURRENT_OFFSET = 25;
  private static final long HOST_END_OFFSET = 75;
  private static final long HOST_LAG = 50;
  private static final Map<String, ?> LAG_FILTER_100 =
      ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_STALE_READS_LAG_MAX_OFFSETS_CONFIG, "100");
  private static final Map<String, ?> LAG_FILTER_25 =
      ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_STALE_READS_LAG_MAX_OFFSETS_CONFIG, "25");

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final Map<String, Object> COMMON_CONFIG = ImmutableMap.<String, Object>builder()
      .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 600000)
      .put(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
      .put(KsqlConfig.KSQL_STREAMS_PREFIX + "num.standby.replicas", 1)
      .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperties(COMMON_CONFIG)
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .withProperties(COMMON_CONFIG)
      .build();

  @Rule
  public final TestKsqlRestApp REST_APP_2 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8087")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8087")
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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setUpClass() {
    //Create topic with 1 partition to control who is active and standby
    TEST_HARNESS.ensureTopics(1, USER_TOPIC);

    final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);

    TEST_HARNESS.produceRows(
        USER_TOPIC,
        USER_PROVIDER,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );
  }

  @Before
  public void setUp() {
    //Create stream
    makeAdminRequest(
        REST_APP_0,
        "CREATE STREAM " + USERS_STREAM
            + " (" + USER_PROVIDER.ksqlSchemaString() + ")"
            + " WITH ("
            + "   kafka_topic='" + USER_TOPIC + "', "
            + "   key='" + USER_PROVIDER.key() + "', "
            + "   value_format='JSON');"
    );
    //Create table
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();
    sql = "SELECT * FROM " + output + " WHERE ROWKEY = '" + KEY + "';";
    List<KsqlEntity> res = makeAdminRequestWithResponse(
        REST_APP_0,
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    QUERY_ID = extractQueryId(res.get(0).toString());
    QUERY_ID = QUERY_ID.substring(0, QUERY_ID.length()-1);
    waitForTableRows();
    waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0, host1, host2), QUERY_ID);
  }

  @After
  public void cleanUp() {
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept();
  }

  @AfterClass
  public static void classTearDown() {
    TMP.delete();
  }

  @Test
  public void shouldQueryActiveWhenActiveAliveQueryIssuedToStandby() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.standBy.right, 3);
    sendHeartbeartsForWindowLength(
        clusterFormation.standBy.right, clusterFormation.active.left, 2000);
    waitForRemoteServerToChangeStatus(
        clusterFormation.standBy.right, clusterFormation.active.left, HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.standBy.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, BASE_TIME, 1)));
  }


  @Test
  public void shouldQueryActiveWhenActiveAliveStandbyDeadQueryIssuedToRouter() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.right, 3);
    sendHeartbeartsForWindowLength(
        clusterFormation.router.right, clusterFormation.active.left, 2000);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.right,
        clusterFormation.active.left,
        HighAvailabilityTestUtil::remoteServerIsUp);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.right,
        clusterFormation.standBy.left,
        HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, BASE_TIME, 1)));
  }

  @Test
  public void shouldQueryStandbyWhenActiveDeadStandbyAliveQueryIssuedToRouter() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.right, 3);
    sendHeartbeartsForWindowLength(
        clusterFormation.router.right, clusterFormation.standBy.left, 2000);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.right,
        clusterFormation.standBy.left,
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.right, sql);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, BASE_TIME, 1)));
  }

  @Test(timeout = 60000)
  public void shouldFilterLaggyServers() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(REST_APP_0, REST_APP_1, REST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.right, 3);
    sendLagReportingRequest(clusterFormation.router.right,
        makeLagMessage(clusterFormation.standBy.left, createLagMap(HOST_CURRENT_OFFSET,
            HOST_END_OFFSET, HOST_LAG)));
    sendHeartbeartsForWindowLength(
        clusterFormation.router.right, clusterFormation.standBy.left, 2000);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.right, clusterFormation.standBy.left,
        HighAvailabilityTestUtil::remoteServerIsUp);

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.right, sql,
        LAG_FILTER_100);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().values(), is(ImmutableList.of(KEY, BASE_TIME, 1)));

    KsqlErrorMessage errorMessage = makePullQueryRequestWithError(clusterFormation.router.right,
        sql, LAG_FILTER_25);
    Assert.assertEquals(40001, errorMessage.getErrorCode());
    Assert.assertTrue(errorMessage.getMessage()
        .contains("All nodes are dead or exceed max allowed lag"));
  }

  private List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty());
  }

  private static void makeAdminRequest(TestKsqlRestApp restApp, final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private List<KsqlEntity> makeAdminRequestWithResponse(
      TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  private static LagReportingMessage makeLagMessage(
      final KsqlHostInfoEntity hostInfo,
      final Map<QueryStateStoreId, StateStoreLags> lagMap
  ) {
    return new LagReportingMessage(hostInfo, new HostStoreLags(lagMap, 100));
  }

  private static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty(), properties);
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
    final String queryId
        = "_confluent-ksql-default_query_CTAS_" + output + "_0";
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
    if(!entity0.getActiveStores().isEmpty() && !entity0.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(Pair.of(host0, restApp0));
    }
    else if(!entity1.getActiveStores().isEmpty() && !entity1.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(Pair.of(host1, restApp1));
    } else {
      clusterFormation.setActive(Pair.of(host2, restApp2));
    }

    //find standby
    if(!entity0.getStandByStores().isEmpty() && !entity0.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Pair.of(host0, restApp0));
    }
    else if(!entity1.getStandByStores().isEmpty() && !entity1.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(Pair.of(host1, restApp1));
    } else {
      clusterFormation.setStandBy(Pair.of(host2, restApp2));
    }

    //find router
    if(entity0.getStandByStores().isEmpty() && entity0.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Pair.of(host0, restApp0));
    }
    else if(entity1.getStandByStores().isEmpty() && entity1.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(Pair.of(host1, restApp1));
    } else {
      clusterFormation.setRouter(Pair.of(host2, restApp2));
    }

    return clusterFormation;
  }

  static class ClusterFormation {
    Pair<KsqlHostInfoEntity, TestKsqlRestApp> active;
    Pair<KsqlHostInfoEntity, TestKsqlRestApp> standBy;
    Pair<KsqlHostInfoEntity, TestKsqlRestApp> router;

    ClusterFormation() {
    }

    public void setActive(final Pair<KsqlHostInfoEntity, TestKsqlRestApp> active) {
      this.active = active;
    }

    public void setStandBy(final Pair<KsqlHostInfoEntity, TestKsqlRestApp> standBy) {
      this.standBy = standBy;
    }

    public void setRouter(final Pair<KsqlHostInfoEntity, TestKsqlRestApp> router) {
      this.router = router;
    }

    public String toString() {
      return new StringBuilder()
          .append("Active = ").append(active.left)
          .append(", Standby = ").append(standBy.left)
          .append(", Router = ").append(router.left)
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
}

