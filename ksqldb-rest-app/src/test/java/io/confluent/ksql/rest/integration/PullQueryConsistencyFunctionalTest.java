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

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.extractQueryId;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequestWithResponse;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer0;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer1;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer2;
import io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.Shutoffs;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.test.util.TestBasicJaasConfig;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.UserDataProvider;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.ws.rs.core.MediaType;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * End to end test that uses consistency token with multiple nodes and queries.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class PullQueryConsistencyFunctionalTest {

  private static final String USER_TOPIC = "user_topic_";
  private static final String USERS_STREAM = "users";
  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final int HEADER = 1;
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
  private static final int BASE_TIME = 1_000_000;
  private final static String KEY1 = USER_PROVIDER.getStringKey(1);
  private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
  private String output;
  private String queryId;
  private String sql;
  private String topic;
  private ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR;
  private ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR_BEFORE;
  private ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR_AFTER;
  private ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR_AFTER_10;

  private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
    LogicalSchema.builder()
      .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
      .build(),
    SerdeFeatures.of(),
    SerdeFeatures.of()
  );

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_RESOURCE = "ksql-user";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";
  private static final Optional<BasicCredentials> USER_CREDS
    = Optional.of(BasicCredentials.of(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD));

  @ClassRule
  public static final TestBasicJaasConfig JAAS_CONFIG = TestBasicJaasConfig
    .builder(PROPS_JAAS_REALM)
    .addUser(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD, KSQL_RESOURCE)
    .build();

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
    .put(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG, KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
    .put(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
    .put(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_RESOURCE)
    .put(KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG, "/heartbeat,/lag")
    // In order to whitelist the above paths for auth, we need to install a noop authentication
    // plugin.  In practice, these are internal paths so we're not interested in testing auth
    // for them in these tests.
    .put(KsqlRestConfig.KSQL_AUTHENTICATION_PLUGIN_CLASS, NoAuthPlugin.class)
    .put(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
    .put(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED, true)
    .put(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS,
      PullQueryRoutingFunctionalTest.StaticStreamsTaskAssignor.class.getName())
    .put(StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
    .build();

  private static final Shutoffs APP_SHUTOFFS_0 = new Shutoffs();
  private static final Shutoffs APP_SHUTOFFS_1 = new Shutoffs();
  private static final Shutoffs APP_SHUTOFFS_2 = new Shutoffs();

  private static final int INT_PORT_0 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT_1 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT_2 = TestUtils.findFreeLocalPort();
  private static final KsqlHostInfoEntity HOST0 = new KsqlHostInfoEntity("localhost", INT_PORT_0);
  private static final KsqlHostInfoEntity HOST1 = new KsqlHostInfoEntity("localhost", INT_PORT_1);
  private static final KsqlHostInfoEntity HOST2 = new KsqlHostInfoEntity("localhost", INT_PORT_2);

  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
    .builder(TEST_HARNESS::kafkaBootstrapServers)
    .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
    .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
    .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
    .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
    .withFaultyKsqlClient(APP_SHUTOFFS_0::getKsqlOutgoing)
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
    .withFaultyKsqlClient(APP_SHUTOFFS_1::getKsqlOutgoing)
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
    .withFaultyKsqlClient(APP_SHUTOFFS_2::getKsqlOutgoing)
    .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
      + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer2.class.getName())
    .withProperties(COMMON_CONFIG)
    .build();

  public final TestApp TEST_APP_0 = new TestApp(HOST0, REST_APP_0, APP_SHUTOFFS_0);
  public final TestApp TEST_APP_1 = new TestApp(HOST1, REST_APP_1, APP_SHUTOFFS_1);
  public final TestApp TEST_APP_2 = new TestApp(HOST2, REST_APP_2, APP_SHUTOFFS_2);

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
    .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
    .around(TEST_HARNESS)
    .around(JAAS_CONFIG)
    .around(TMP);

  @Rule
  public final Timeout timeout = Timeout.builder()
    .withTimeout(2, TimeUnit.MINUTES)
    .withLookingForStuckThread(true)
    .build();

  @BeforeClass
  public static void setUpClass() {
    FaultyKafkaConsumer0.setPauseOffset(APP_SHUTOFFS_0::getKafkaPauseOffset);
    FaultyKafkaConsumer1.setPauseOffset(APP_SHUTOFFS_1::getKafkaPauseOffset);
    FaultyKafkaConsumer2.setPauseOffset(APP_SHUTOFFS_2::getKafkaPauseOffset);
  }

  @Before
  public void setUp() {
    //Create topic with 1 partition to control who is active and standby
    topic = USER_TOPIC + KsqlIdentifierTestUtil.uniqueIdentifierName();
    TEST_HARNESS.ensureTopics(1, topic);

    TEST_HARNESS.produceRows(
      topic,
      USER_PROVIDER,
      FormatFactory.KAFKA,
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
        + "   value_format='JSON');",
      USER_CREDS
    );
    //Create table
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();
    sql = "SELECT * FROM " + output + " WHERE USERID = '" + KEY1 + "';";
    List<KsqlEntity> res = makeAdminRequestWithResponse(
      REST_APP_0,
      "CREATE TABLE " + output + " AS"
        + " SELECT " + USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
        + " GROUP BY " + USER_PROVIDER.key() + ";",
      USER_CREDS
    );
    queryId = extractQueryId(res.get(0).toString());
    queryId = queryId.substring(0, queryId.length() - 1);
    CONSISTENCY_OFFSET_VECTOR =
        new ConsistencyOffsetVector(0, ImmutableMap.of(topic, ImmutableMap.of(0, 4L)));
    CONSISTENCY_OFFSET_VECTOR_BEFORE =
        new ConsistencyOffsetVector(0, ImmutableMap.of(topic, ImmutableMap.of(0, 3L)));
    CONSISTENCY_OFFSET_VECTOR_AFTER =
        new ConsistencyOffsetVector(0, ImmutableMap.of(topic, ImmutableMap.of(0, 6L)));
    CONSISTENCY_OFFSET_VECTOR_AFTER_10 =
        new ConsistencyOffsetVector(0, ImmutableMap.of(topic, ImmutableMap.of(0, 9L)));

    waitForTableRows();

    waitForStreamsMetadataToInitialize(
      REST_APP_0, ImmutableList.of(HOST0, HOST1, HOST2), USER_CREDS);
  }

  @After
  public void cleanUp() {
    REST_APP_0.closePersistentQueries(USER_CREDS);
    REST_APP_0.dropSourcesExcept(USER_CREDS);
    APP_SHUTOFFS_0.reset();
    APP_SHUTOFFS_1.reset();
    APP_SHUTOFFS_2.reset();
  }

  @Test
  public void shouldExecutePullQueryWithNoBound() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(), clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(3), USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, "");
    final Map<String, Object> requestProperties = builder.build();
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          REST_APP_0,
          sql,
          MediaType.APPLICATION_JSON,
          Collections.emptyMap(),
          requestProperties
      );
      return Arrays.asList(response.split(System.lineSeparator()));
    };
    final String serializedCV = CONSISTENCY_OFFSET_VECTOR.serialize();

    // When:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));

    // Then:
    assertThat(messages, hasSize(HEADER + 2));
    assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),
               endsWith("\",\"schema\":\"`USERID` STRING KEY, `COUNT` BIGINT\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_1\",1]}},"));
    assertThat(messages.get(2), is("{\"consistencyToken\":{\"consistencyToken\":"
                                       + "\"" + serializedCV + "\"}}]"));
    verifyConsistencyVector(messages.get(2), CONSISTENCY_OFFSET_VECTOR);
  }

  @Test
  public void shouldExecutePullQueryWithBound() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(), clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(3), USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);

    final String clientCV = CONSISTENCY_OFFSET_VECTOR_BEFORE.serialize();
    final String serverCV = CONSISTENCY_OFFSET_VECTOR.serialize();
    final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, clientCV);
    final Map<String, Object> requestProperties = builder.build();
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          clusterFormation.router.getApp(),
          sql,
          MediaType.APPLICATION_JSON,
          Collections.emptyMap(),
          requestProperties
      );
      return Arrays.asList(response.split(System.lineSeparator()));
    };

    // When:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));

    // Then:
    assertThat(messages, hasSize(HEADER + 2));
    assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),
               endsWith("\",\"schema\":\"`USERID` STRING KEY, `COUNT` BIGINT\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_1\",1]}},"));
    assertThat(messages.get(2), is("{\"consistencyToken\":{\"consistencyToken\":"
                                       + "\"" + serverCV + "\"}}]"));
    verifyConsistencyVector(messages.get(2), CONSISTENCY_OFFSET_VECTOR);
  }

  @Test
  public void shouldFailPullQueryWithBound() {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(), clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(3), USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    final String serializedCV = CONSISTENCY_OFFSET_VECTOR_AFTER.serialize();
    final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, serializedCV);
    final Map<String, Object> requestProperties = builder.build();

    // When:
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          clusterFormation.router.getApp(),
          sql,
          MediaType.APPLICATION_JSON,
          Collections.emptyMap(),
          requestProperties
      );
      return Arrays.asList(response.split(System.lineSeparator()));
    };

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertThat(messages, hasSize(HEADER + 1));
    assertThat(messages.get(1), containsString("Failed to get value from materialized table, "
                                                   + "reason: NOT_UP_TO_BOUND"));
  }

  @Test
  public void shouldExecuteThenFailPullQueryWithBound() throws Exception {
    // Given:
    ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
    waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(3),
        USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp, USER_CREDS);

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(clusterFormation.standBy.getHost(), Optional.of(5L), 5),
        USER_CREDS);

    // Cut off standby from Kafka to simulate lag
    clusterFormation.standBy.getShutoffs().setKafkaPauseOffset(0);
    Thread.sleep(2000);

    // Produce more data that will now only be available on active since standby is cut off
    TEST_HARNESS.produceRows(
        topic,
        USER_PROVIDER,
        FormatFactory.KAFKA,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );

    // Make sure that the lags get reported before issuing query
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.router.getHost(),
        HighAvailabilityTestUtil.lagsReported(clusterFormation.active.getHost(), Optional.of(10L), 10),
        USER_CREDS);

    final KsqlRestClient restClient = clusterFormation.router.getApp().buildKsqlClient(USER_CREDS);
    final ImmutableMap<String, Object> requestProperties =
        ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);

    // When:
    RestResponse<List<StreamedRow>> res = restClient.makeQueryRequest(
        sql, 1L, null, requestProperties);

    // Then:
    List<StreamedRow> rows = res.getResponse();
    assertThat(rows, hasSize(HEADER + 2));
    assertThat(rows.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows.get(1).getRow().get().getColumns(), is(ImmutableList.of(KEY1, 2)));
    assertThat(rows.get(2).getConsistencyToken(), is(not(Optional.empty())));
    ConsistencyOffsetVector cvResponse = ConsistencyOffsetVector.deserialize(
        rows.get(2).getConsistencyToken().get().getConsistencyToken());
    assertThat(cvResponse, is(CONSISTENCY_OFFSET_VECTOR_AFTER_10));

    // Given:
    // Partition active off
    clusterFormation.active.getShutoffs().shutOffAll();

    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.standBy.getHost(),
        HighAvailabilityTestUtil::remoteServerIsUp,
        USER_CREDS);
    waitForRemoteServerToChangeStatus(
        clusterFormation.router.getApp(),
        clusterFormation.active.getHost(),
        HighAvailabilityTestUtil::remoteServerIsDown,
        USER_CREDS);
    final ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
        .put(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, cvResponse.serialize());
    final Map<String, Object> requestProperties2 = builder.build();

    // When:
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          clusterFormation.router.getApp(),
          sql,
          MediaType.APPLICATION_JSON,
          Collections.emptyMap(),
          requestProperties2
      );
      return Arrays.asList(response.split(System.lineSeparator()));
    };

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertThat(messages, hasSize(HEADER + 1));
    assertThat(messages.get(1), containsString("Failed to get value from materialized table, "
                                                   + "reason: NOT_UP_TO_BOUND"));
  }

  private ClusterFormation findClusterFormation(
    TestApp testApp0, TestApp testApp1, TestApp testApp2) {
    ClusterFormation clusterFormation = new ClusterFormation();
    ClusterStatusResponse clusterStatusResponse
      = HighAvailabilityTestUtil.sendClusterStatusRequest(testApp0.getApp(), USER_CREDS);
    ActiveStandbyEntity entity0 = clusterStatusResponse.getClusterStatus().get(testApp0.getHost())
      .getActiveStandbyPerQuery().get(queryId);
    ActiveStandbyEntity entity1 = clusterStatusResponse.getClusterStatus().get(testApp1.getHost())
      .getActiveStandbyPerQuery().get(queryId);

    // find active
    if (!entity0.getActiveStores().isEmpty() && !entity0.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(testApp0);
    } else if (!entity1.getActiveStores().isEmpty() && !entity1.getActivePartitions().isEmpty()) {
      clusterFormation.setActive(testApp1);
    } else {
      clusterFormation.setActive(testApp2);
    }

    //find standby
    if (!entity0.getStandByStores().isEmpty() && !entity0.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(testApp0);
    } else if (!entity1.getStandByStores().isEmpty() && !entity1.getStandByPartitions().isEmpty()) {
      clusterFormation.setStandBy(testApp1);
    } else {
      clusterFormation.setStandBy(testApp2);
    }

    //find router
    if (entity0.getStandByStores().isEmpty() && entity0.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(testApp0);
    } else if (entity1.getStandByStores().isEmpty() && entity1.getActiveStores().isEmpty()) {
      clusterFormation.setRouter(testApp1);
    } else {
      clusterFormation.setRouter(testApp2);
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
      FormatFactory.KAFKA,
      FormatFactory.JSON,
      AGGREGATE_SCHEMA
    );
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }

  private static class TestApp {

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

  // AuthenticationPlugin which never returns a Principal
  public static class NoAuthPlugin implements AuthenticationPlugin {

    @Override
    public void configure(Map<String, ?> map) {
    }

    @Override
    public CompletableFuture<Principal> handleAuth(RoutingContext routingContext,
                                                   WorkerExecutor workerExecutor) {
      return CompletableFuture.completedFuture(null);
    }
  }

  private static String rawRestQueryRequest(
      final TestKsqlRestApp testApp,
      final String sql, final String mediaType, final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    return RestIntegrationTestUtil.rawRestQueryRequest(
            testApp, sql, mediaType, configOverrides, requestProperties, USER_CREDS)
        .body()
        .toString();
  }

  private static void verifyConsistencyVector(
      final String consistencyText, final ConsistencyOffsetVector consistencyOffsetVector) {
    String serializedCV = consistencyText.split(":\"")[1];
    serializedCV = serializedCV.substring(0, serializedCV.length()-4);
    final ConsistencyOffsetVector cvResponse = ConsistencyOffsetVector.deserialize(serializedCV);
    assertThat(cvResponse, is(consistencyOffsetVector));
  }
}
