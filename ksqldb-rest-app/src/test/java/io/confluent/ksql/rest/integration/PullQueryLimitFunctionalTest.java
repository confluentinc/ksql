package io.confluent.ksql.rest.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import static io.confluent.ksql.rest.entity.StreamedRowMatchers.matchersRowsAnyOrder;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.extractQueryId;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequestWithResponse;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makePullQueryRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
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
import io.confluent.ksql.util.KsqlConfig;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import io.confluent.ksql.util.UserDataProvider;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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


@Category({IntegrationTest.class})
public class PullQueryLimitFunctionalTest {

    private static final String USER_TOPIC = "user_topic_";
    private static final String USERS_STREAM = "users";
    private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
    private static final int HEADER = 1;
    private static final int LIMIT_REACHED_MESSAGE = 1;
    private static final int TOTAL_RECORDS = 5;
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
    private static final int BASE_TIME = 1_000_000;
    private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
    private String output;
    private String queryId;
    private String topic;

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
            .put(KsqlRestConfig.KSQL_AUTHENTICATION_PLUGIN_CLASS, PullQueryRoutingFunctionalTest.NoAuthPlugin.class)
            .build();

    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_0 = new HighAvailabilityTestUtil.Shutoffs();
    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_1 = new HighAvailabilityTestUtil.Shutoffs();
    private static final HighAvailabilityTestUtil.Shutoffs APP_SHUTOFFS_2 = new HighAvailabilityTestUtil.Shutoffs();

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
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer0.class.getName())
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
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer1.class.getName())
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
                    + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer.FaultyKafkaConsumer2.class.getName())
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
        FaultyKafkaConsumer.FaultyKafkaConsumer0.setPauseOffset(APP_SHUTOFFS_0::getKafkaPauseOffset);
        FaultyKafkaConsumer.FaultyKafkaConsumer1.setPauseOffset(APP_SHUTOFFS_1::getKafkaPauseOffset);
        FaultyKafkaConsumer.FaultyKafkaConsumer2.setPauseOffset(APP_SHUTOFFS_2::getKafkaPauseOffset);
    }

    @Before
    public void setUp() {
        //Create topic with 4 partition to control who is active and standby
        topic = USER_TOPIC + KsqlIdentifierTestUtil.uniqueIdentifierName();
        TEST_HARNESS.ensureTopics(4, topic);

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
        List<KsqlEntity> res = makeAdminRequestWithResponse(
                REST_APP_0,
                "CREATE TABLE " + output + " AS"
                        + " SELECT " + USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
                        + " GROUP BY " + USER_PROVIDER.key() + ";",
                USER_CREDS
        );
        queryId = extractQueryId(res.get(0).toString());
        queryId = queryId.substring(0, queryId.length() - 1);
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
    public void shouldReturnLimitRowsMultiHostSetupTable() {
        // Given:
        final int NUM_LIMIT_ROWS = 3;
        final int LIMIT_SUBSET_SIZE = HEADER + NUM_LIMIT_ROWS;
        ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
        waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
        waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
                clusterFormation.router.getHost(), HighAvailabilityTestUtil.lagsReported(3), USER_CREDS);


        final String sqlTableScan = "SELECT * FROM " + output + ";";
        final String sqlLimit = "SELECT * FROM " + output + " LIMIT " + NUM_LIMIT_ROWS + " ;";

        //issue table scan first
        final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlTableScan, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS));

        // issue pull query with limit
        final List<StreamedRow> rows_1 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(LIMIT_SUBSET_SIZE));

        // Partition off the active
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


        // issue table scan after partitioning active
        final List<StreamedRow> rows_2 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlTableScan, null, USER_CREDS);

        // check that we get all rows back
        assertThat(rows_2, hasSize(HEADER + TOTAL_RECORDS));
        assertThat(rows_0, is(matchersRowsAnyOrder(rows_2)));

        // issue pull query with limit after partitioning active
        final List<StreamedRow> rows_3 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_3, hasSize(LIMIT_SUBSET_SIZE));
    }

    @Test
    public void shouldReturnLimitRowsMultiHostSetupStream() {
        // Given:
        final int NUM_LIMIT_ROWS = 3;
        final int LIMIT_SUBSET_SIZE = HEADER + NUM_LIMIT_ROWS + LIMIT_REACHED_MESSAGE;
        ClusterFormation clusterFormation = findClusterFormation(TEST_APP_0, TEST_APP_1, TEST_APP_2);
        waitForClusterToBeDiscovered(clusterFormation.router.getApp(), 3, USER_CREDS);
        waitForRemoteServerToChangeStatus(clusterFormation.router.getApp(),
                clusterFormation.router.getHost(), HighAvailabilityTestUtil.lagsReported(3), USER_CREDS);


        final String sqlStreamPull = "SELECT * FROM " + USERS_STREAM + ";";
        final String sqlLimit = "SELECT * FROM " + USERS_STREAM + " LIMIT " + NUM_LIMIT_ROWS + " ;";

        //scan the entire stream first
        final List<StreamedRow> rows_0 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlStreamPull, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS));

        //issue pull query on stream with limit
        final List<StreamedRow> rows_1 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(LIMIT_SUBSET_SIZE));

        // Partition off the active
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


        // issue stream scan after partitioning active
        final List<StreamedRow> rows_2 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlStreamPull, null, USER_CREDS);

        // check that we get all rows back
        assertThat(rows_2, hasSize(HEADER + TOTAL_RECORDS));
        assertThat(rows_0, is(matchersRowsAnyOrder(rows_2)));

        // issue pull query with limit after partitioning active
        final List<StreamedRow> rows_3 = makePullQueryRequest(clusterFormation.router.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_3, hasSize(LIMIT_SUBSET_SIZE));

        // check that we got the same set of rows back with limit as pull query over streams reads from the beginning
        // We ignore the headers as they have different transient query id
        assertThat(rows_1.subList(HEADER, LIMIT_SUBSET_SIZE),
                is(rows_3.subList(HEADER, LIMIT_SUBSET_SIZE)));
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
        private final HighAvailabilityTestUtil.Shutoffs shutoffs;

        public TestApp(KsqlHostInfoEntity host, TestKsqlRestApp app, HighAvailabilityTestUtil.Shutoffs shutoffs) {
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

        public HighAvailabilityTestUtil.Shutoffs getShutoffs() {
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
}
