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

package io.confluent.ksql.api.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

@Category({IntegrationTest.class})
public class PullBandwidthThrottleIntegrationTest {

    private static final PageViewDataProvider TEST_DATA_PROVIDER = new PageViewDataProvider();
    private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
    private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();

    private static final String AGG_TABLE = "AGG_TABLE";
    private static final Credentials NORMAL_USER = VALID_USER2;

    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
            .withKafkaCluster(
                    EmbeddedSingleNodeKafkaCluster.newBuilder()
                            .withoutPlainListeners()
                            .withSaslSslListeners()
            ).build();

    @Rule
    public final TestKsqlRestApp REST_APP = TestKsqlRestApp
            .builder(TEST_HARNESS::kafkaBootstrapServers)
            .withProperty("security.protocol", "SASL_SSL")
            .withProperty("sasl.mechanism", "PLAIN")
            .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
            .withProperty("ksql.query.pull.table.scan.enabled", true)
            .withProperties(ClientTrustStore.trustStoreProps())
            .withProperty(KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG, 1)
            .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
            .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
            .withProperty(KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED, true)
            .withProperty("auto.offset.reset", "earliest")
            .build();

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
            .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
            .around(TEST_HARNESS);
    private static final String RATE_LIMIT_MESSAGE = "Host is at bandwidth rate limit for pull queries.";

    @Rule
    public final Timeout timeout = Timeout.builder()
        .withTimeout(2, TimeUnit.MINUTES)
        .withLookingForStuckThread(true)
        .build();

    @BeforeClass
    public static void setUpClass() {
        TEST_HARNESS.ensureTopics(TEST_TOPIC);

        TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);
    }

    @Before
    public void before() {
        RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);

        makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
                + "SELECT PAGEID, LATEST_BY_OFFSET(USERID) AS USERID FROM " + TEST_STREAM + " GROUP BY PAGEID;"
        );
    }

    @After
    public void after() {
        REST_APP.closePersistentQueries();
        REST_APP.dropSourcesExcept();
    }

    private Vertx vertx;
    private WebClient client;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        client = createClient();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (vertx != null) {
            vertx.close();
        }
        REST_APP.getServiceContext().close();
    }

    @Test
    public void pullTableBandwidthThrottleTest() {
        String veryLong = createDataSize(100000);

        String sql = "SELECT CONCAT(\'"+ veryLong + "\') as placeholder from " + AGG_TABLE + ";";

        //the pull query should go through 2 times
        for (int i = 0; i < 2; i += 1) {
            assertThatEventually(() -> {
                QueryResponse queryResponse1 = executeQuery(sql);
                return queryResponse1.rows;
            }, hasSize(5));
        }

        //the third try should fail
        try {
            executeQuery(sql);
        } catch (KsqlException e) {
            assertEquals(RATE_LIMIT_MESSAGE, e.getMessage());
        }
    }

    @Test
    @Ignore
    public void pullStreamBandwidthThrottleTest() {
        String veryLong = createDataSize(100000);

        String sql = "SELECT CONCAT(\'"+ veryLong + "\') as placeholder from " + TEST_STREAM + ";";

        //the pull query should go through 2 times
        for (int i = 0; i < 2; i += 1) {
            assertThatEventually(() -> {
                QueryResponse queryResponse1 = executeQuery(sql);
                return queryResponse1.rows;
            }, hasSize(7));
        }

        //the third try should fail
        try {
            executeQuery(sql);
        } catch (KsqlException e) {
            assertEquals(RATE_LIMIT_MESSAGE, e.getMessage());
        }
    }

    private static String createDataSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i=0; i<msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    private QueryResponse executeQuery(final String sql) {
        return executeQueryWithVariables(sql, new JsonObject());
    }

    private QueryResponse executeQueryWithVariables(final String sql, final JsonObject variables) {
        JsonObject properties = new JsonObject();
        JsonObject requestBody = new JsonObject()
                .put("sql", sql).put("properties", properties).put("sessionVariables", variables);
        HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());
        return new QueryResponse(response.bodyAsString());
    }

    private WebClient createClient() {
        WebClientOptions options = new WebClientOptions().
                setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
                .setDefaultHost("localhost").setDefaultPort(REST_APP.getListeners().get(0).getPort());
        return WebClient.create(vertx, options);
    }

    private HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody) {
        return sendRequest(client, uri, requestBody);
    }

    private HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
                                             final Buffer requestBody) {
        VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
        client
                .post(uri)
                .sendBuffer(requestBody, requestFuture);
        try {
            return requestFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void makeKsqlRequest(final String sql) {
        RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
    }
}
