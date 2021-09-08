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

package io.confluent.ksql.api.integration;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_SCALABLE_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.QueryStreamSubscriber;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ScalablePushBandwidthThrottleIntegrationTest {
  private static final String RATE_LIMIT_MESSAGE = "Host is at bandwidth rate limit for queries.";
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

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
      .withProperty(KSQL_QUERY_PUSH_SCALABLE_ENABLED , true)
      .withProperty(KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG , 1)
      .withProperty("auto.offset.reset", "latest")
      .withProperty(KSQL_QUERY_PUSH_SCALABLE_REGISTRY_INSTALLED, true)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private Vertx vertx;
  private KsqlRestClient restClient;
  private StreamPublisher<StreamedRow> publisher;
  private QueryStreamSubscriber subscriber;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(TEST_TOPIC);

    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT PAGEID, LATEST_BY_OFFSET(USERID) AS USERID FROM " + TEST_STREAM + " GROUP BY PAGEID;"
    );
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    restClient = REST_APP.buildKsqlClient();
  }

  @After
  public void tearDown() {
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @SuppressFBWarnings({"DLS_DEAD_LOCAL_STORE"})
  @Test
  public void scalablePushBandwidthThrottleTestHTTP1() {
    assertAllPersistentQueriesRunning();
    String veryLong = createDataSize(100000);
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    String sql = "SELECT CONCAT(\'"+ veryLong + "\') as placeholder from " + AGG_TABLE + " EMIT CHANGES LIMIT 1;";

    // scalable push query should succeed 10 times
    for (int i = 0; i < 11; i += 1) {
      makeRequestAndSetupSubscriber(sql,
          ImmutableMap.of("auto.offset.reset", "latest"),
          header, complete );
      TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);
      System.out.println(i);
    }

    // scalable push query should fail on 11th try since it exceeds 1MB bandwidth limit
      try {
        makeQueryRequest(sql,
            ImmutableMap.of("auto.offset.reset", "latest"));
        throw new AssertionError("New scalable push query should have exceeded bandwidth limit ");
      } catch (KsqlException e) {
        assertThat(e.getMessage(), is(RATE_LIMIT_MESSAGE));
      }
  }



  private static String createDataSize(int msgSize) {
    StringBuilder sb = new StringBuilder(msgSize);
    for (int i=0; i<msgSize; i++) {
      sb.append('a');
    }
    return sb.toString();
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private void makeRequestAndSetupSubscriber(
      final String sql,
      final Map<String, ?> properties,
      final CompletableFuture<StreamedRow> header,
      final CompletableFuture<List<StreamedRow>> future
  ) {
    publisher = makeQueryRequest(sql, properties);
    subscriber = new QueryStreamSubscriber(publisher.getContext(), future, header);
    publisher.subscribe(subscriber);
  }

  StreamPublisher<StreamedRow> makeQueryRequest(
      final String sql,
      final Map<String, ?> properties
  ) {
    final RestResponse<StreamPublisher<StreamedRow>> res =
        restClient.makeQueryRequestStreamed(sql, null, properties);

    if (res.isErroneous()) {
      throw new KsqlException(res.getErrorMessage().getMessage());
    }
    return res.getResponse();
  }

  private void assertAllPersistentQueriesRunning() {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (metadata.getState() != State.RUNNING) {
          return false;
        }
      }
      return true;
    }, is(true));
  }
}
