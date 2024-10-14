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
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
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
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PageViewDataProvider.Batch;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

@Category({IntegrationTest.class})
public class ScalablePushBandwidthThrottleIntegrationTest {
  private static final String RATE_LIMIT_MESSAGE = "Host is at bandwidth rate limit for push queries.";
  private static final PageViewDataProvider TEST_DATA_PROVIDER = new PageViewDataProvider(
      "PAGEVIEW", Batch.BATCH4);
  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KSQL_QUERY_PUSH_V2_ENABLED , true)
      .withProperty(KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG , 1)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS, 0L)
      .withProperty("auto.offset.reset", "latest")
      .withProperty(KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final RuleChain CHAIN_TEST = RuleChain
      .outerRule(REST_APP);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(4, TimeUnit.MINUTES)
      .withLookingForStuckThread(true)
      .build();

  private Vertx vertx;
  private KsqlRestClient restClient;
  private StreamPublisher<StreamedRow> publisher;
  private QueryStreamSubscriber subscriber;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    restClient = REST_APP.buildKsqlClient();

    TEST_HARNESS.ensureTopics(TEST_TOPIC);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT PAGEID, LATEST_BY_OFFSET(USERID) AS USERID FROM " + TEST_STREAM + " GROUP BY PAGEID;"
    );
  }

  @After
  public void tearDown() {
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
    REST_APP.closePersistentQueries();
    REST_APP.dropSourcesExcept();
  }

  @SuppressFBWarnings({"DLS_DEAD_LOCAL_STORE"})
  @Test
  public void scalablePushBandwidthThrottleTestHTTP1() {
    assertAllPersistentQueriesRunning();
    String veryLong = createDataSize(1200000);
    String sql = "SELECT CONCAT(\'"+ veryLong + "\') as placeholder from " + AGG_TABLE + " EMIT CHANGES LIMIT 1;";

    // scalable push query should succeed 1 time
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    makeRequestAndSetupSubscriber(sql,
        ImmutableMap.of("auto.offset.reset", "latest"),
        header, complete );
    assertExpectedScalablePushQueries(1);
    // Produce exactly one row, or else other rows produced can complete other requests and it
    // confuses the test.
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);
    // header, row, limit reached message
    assertThatEventually(() ->  subscriber.getRows().size(), is (3));
    subscriber.close();
    publisher.close();

    // scalable push query should fail on 2nd try since it exceeds 1MB bandwidth limit
    try {
      makeQueryRequest(sql,
          ImmutableMap.of("auto.offset.reset", "latest"));
      throw new AssertionError("New scalable push query should have exceeded bandwidth limit ");
    } catch (KsqlException e) {
      assertThat(e.getMessage(), is(RATE_LIMIT_MESSAGE));
    }
  }

  @SuppressFBWarnings({"DLS_DEAD_LOCAL_STORE"})
  @Test
  public void scalablePushBandwidthThrottleTestHTTP2()
      throws ExecutionException, InterruptedException {
    assertAllPersistentQueriesRunning();
    String veryLong = createDataSize(1200000);
    String sql = "SELECT CONCAT(\'"+ veryLong + "\') as placeholder from " + AGG_TABLE + " EMIT CHANGES LIMIT 1;";

    // scalable push query should succeed 1 time
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    makeRequestAndSetupSubscriberAsync(sql,
        ImmutableMap.of("auto.offset.reset", "latest"),
        header, complete );
    assertExpectedScalablePushQueries(1);
    // Produce exactly one row, or else other rows produced can complete other requests and it
    // confuses the test.
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON, FormatFactory.JSON);
    // header, row
    assertThatEventually(() ->  subscriber.getRows().size(), is (2));
    subscriber.close();

    // http2 connections are reused, meaning if we close the request
    // and then start a new one immediately afterwards we're at risk
    // of garbling writes
    final CountDownLatch latch = new CountDownLatch(1);
    publisher.close().onComplete(x -> latch.countDown());
    latch.await();

    // scalable push query should fail on 11th try since it exceeds 1MB bandwidth limit
    try {
      makeQueryRequestAsync(sql,
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
  private void makeRequestAndSetupSubscriberAsync(
      final String sql,
      final Map<String, ?> properties,
      final CompletableFuture<StreamedRow> header,
      final CompletableFuture<List<StreamedRow>> future
  ) throws ExecutionException, InterruptedException {
    publisher = makeQueryRequestAsync(sql, properties);
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

  StreamPublisher<StreamedRow> makeQueryRequestAsync(
      final String sql,
      final Map<String, ?> properties
  ) throws ExecutionException, InterruptedException {
    final CompletableFuture<RestResponse<StreamPublisher<StreamedRow>>> res =
        restClient.makeQueryRequestStreamedAsync(sql, properties);

    if (res.get().isErroneous()) {
      throw new KsqlException(res.get().getErrorMessage().getMessage());
    }
    if (res.isCompletedExceptionally()
        && !res.get().getErrorMessage().getMessage().equalsIgnoreCase(RATE_LIMIT_MESSAGE)) {
      throw new KsqlException(res.get().getErrorMessage().getMessage());
    }

    return res.get().getResponse();
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

  private void assertExpectedScalablePushQueries(
      final int expectedScalablePushQueries
  ) {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (metadata.getScalablePushRegistry().get().latestNumRegistered()
            < expectedScalablePushQueries
            || !metadata.getScalablePushRegistry().get().latestHasAssignment()) {
          return false;
        }
      }
      return true;
    }, is(true));
  }
}
