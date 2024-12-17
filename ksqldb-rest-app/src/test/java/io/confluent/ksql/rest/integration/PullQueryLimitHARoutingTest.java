/*
 * Copyright 2021 Confluent Inc.
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
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makePullQueryRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.UserDataProviderBig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;


@Ignore
@Category({IntegrationTest.class})
public class PullQueryLimitHARoutingTest {

    private static final String USER_TOPIC = "user_topic_";
    private static final String USERS_STREAM = "users";
    private static final UserDataProviderBig USER_PROVIDER = new UserDataProviderBig();
    private static final int TOTAL_RECORDS = USER_PROVIDER.getNumRecords();
    private static final int HEADER = 1;
    private static final int LIMIT_REACHED_MESSAGE = 1;
    private static final int COMPLETE_MESSAGE = 1;
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
    private static final int BASE_TIME = 1_000_000;
    private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
    private String output;
    private String queryId;
    private String topic;

    private static final Optional<BasicCredentials> USER_CREDS = Optional.empty();

    private static final PhysicalSchema AGGREGATE_SCHEMA = PhysicalSchema.from(
            LogicalSchema.builder()
                    .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
                    .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
                    .build(),
            SerdeFeatures.of(),
            SerdeFeatures.of()
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

    public final TestApp TEST_APP_0 = new TestApp(HOST0, REST_APP_0);
    public final TestApp TEST_APP_1 = new TestApp(HOST1, REST_APP_1);
    public final TestApp TEST_APP_2 = new TestApp(HOST2, REST_APP_2);

    public final List<TestApp> ALL_TEST_APPS = ImmutableList.of(TEST_APP_0, TEST_APP_1, TEST_APP_2);

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
            .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
            .around(TEST_HARNESS)
            .around(TMP);

    @Rule
    public final Timeout timeout = Timeout.builder()
            .withTimeout(4, TimeUnit.MINUTES)
            .withLookingForStuckThread(true)
            .build();

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
        final int numLimitRows = 300;
        final int limitSubsetSize = HEADER + numLimitRows;

        // This should effectively ensure that both actives and standbys are caught up.
        IntegrationTestUtil.waitForPersistentQueriesToProcessInputs(
            TEST_HARNESS.getKafkaCluster(),
            REST_APP_0.getEngine());
        for (TestApp testApp : ALL_TEST_APPS) {
            // Ensure that lags are reported
            waitForRemoteServerToChangeStatus(TEST_APP_0.getApp(),
                    testApp.getHost(),
                    HighAvailabilityTestUtil.zeroLagsReported(testApp.getHost()),
                    USER_CREDS);
        }

        final String sqlTableScan = "SELECT * FROM " + output + ";";
        final String sqlLimit = "SELECT * FROM " + output + " LIMIT " + numLimitRows + " ;";

        //issue table scan first
        final List<StreamedRow> rows_0 = makePullQueryRequestDistinct(TEST_APP_0.getApp(),
                sqlTableScan, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS));

        // issue pull query with limit
        final List<StreamedRow> rows_1 = makePullQueryRequestDistinct(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(limitSubsetSize));
    }

    @Test
    public void shouldReturnLimitRowsMultiHostSetupStream() {
        // Given:
        final int numLimitRows = 200;
        final int limitSubsetSize = HEADER + numLimitRows + LIMIT_REACHED_MESSAGE;

        // This should effectively ensure that both actives and standbys are caught up.
        IntegrationTestUtil.waitForPersistentQueriesToProcessInputs(
            TEST_HARNESS.getKafkaCluster(),
            REST_APP_0.getEngine());
        for (TestApp testApp : ALL_TEST_APPS) {
            // Ensure that lags are reported
            waitForRemoteServerToChangeStatus(REST_APP_0,
                testApp.getHost(),
                HighAvailabilityTestUtil.zeroLagsReported(testApp.getHost()),
                USER_CREDS);
        }

        final String sqlStreamPull = "SELECT * FROM " + USERS_STREAM + ";";
        final String sqlLimit = "SELECT * FROM " + USERS_STREAM + " LIMIT " + numLimitRows + " ;";

        //scan the entire stream first
        final List<StreamedRow> rows_0 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlStreamPull, null, USER_CREDS);

        //check that we got back all the rows
        assertThat(rows_0, hasSize(HEADER + TOTAL_RECORDS + COMPLETE_MESSAGE));

        //issue pull query on stream with limit
        final List<StreamedRow> rows_1 = makePullQueryRequest(TEST_APP_0.getApp(),
                sqlLimit, null, USER_CREDS);

        // check that we only got limit number of rows
        assertThat(rows_1, hasSize(limitSubsetSize));
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

        public TestApp(KsqlHostInfoEntity host, TestKsqlRestApp app) {
            this.host = host;
            this.app = app;
        }

        public KsqlHostInfoEntity getHost() {
            return host;
        }

        public TestKsqlRestApp getApp() {
            return app;
        }
    }

    private List<StreamedRow> makePullQueryRequestDistinct(
        final TestKsqlRestApp target,
        final String sql,
        final Map<String, ?> properties,
        final Optional<BasicCredentials> userCreds
    ) {
        return makePullQueryRequest(target, sql, properties, userCreds)
            .stream().distinct().collect(Collectors.toList());
    }
}
