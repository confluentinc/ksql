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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PageViewDataProvider.Batch;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class ScalablePushQueryFunctionalTest {
  private static final Logger LOG = LoggerFactory.getLogger(ScalablePushQueryFunctionalTest.class);

  private static final String PAGE_VIEW_CSAS = "PAGE_VIEW_CSAS";

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("PAGEID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("VIEWTIME"), SqlTypes.BIGINT)
      .build();

  private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)
      // Make rebalances happen quicker for the sake of the test
      .withProperty(KSQL_STREAMS_PREFIX + "max.poll.interval.ms", 5000)
      .withProperty(KSQL_STREAMS_PREFIX + "session.timeout.ms", 10000)
      .build();

  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)
      // Make rebalances happen quicker for the sake of the test
      .withProperty(KSQL_STREAMS_PREFIX + "max.poll.interval.ms", 5000)
      .withProperty(KSQL_STREAMS_PREFIX + "session.timeout.ms", 10000)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(TMP);

  // This will start and stop REST_APP_0 for each test, but we handle REST_APP_1 manually since each
  // test will use it differently.
  @Rule
  public final RuleChain CHAIN_TEST = RuleChain
      .outerRule(REST_APP_0);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(2, TimeUnit.MINUTES)
      .withLookingForStuckThread(true)
      .build();

  @Rule
  public final TestName testName = new TestName();

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }

  private String streamName;
  private PageViewDataProvider pageViewDataProvider;
  private PageViewDataProvider pageViewAdditionalDataProvider;
  private KsqlRestClient restClient;
  private StreamPublisher<StreamedRow> publisher;
  private QueryStreamSubscriber subscriber;

  @Before
  public void setUp() {
    // Just for the same of not unnecessarily starting and stopping REST_APP_1, we manage it
    // manually.
    if (!testName.getMethodName().endsWith("_skipServer1")) {
      REST_APP_1.start();
    }
    final String prefix = "PAGE_VIEWS_" + KsqlIdentifierTestUtil.uniqueIdentifierName();
    pageViewDataProvider = new PageViewDataProvider(prefix);
    pageViewAdditionalDataProvider = new PageViewDataProvider(prefix, Batch.BATCH2);
    TEST_HARNESS.ensureTopics(2, pageViewDataProvider.topicName());

    RestIntegrationTestUtil.createStream(REST_APP_0, pageViewDataProvider);
    streamName = PAGE_VIEW_CSAS + "_" + KsqlIdentifierTestUtil.uniqueIdentifierName();
    makeKsqlRequest("CREATE STREAM " + streamName + " AS "
        + "SELECT * FROM " + pageViewDataProvider.sourceName() + ";"
    );
    restClient = REST_APP_0.buildKsqlClient();
  }

  @After
  public void tearDown() {
    try {
      subscriber.close();
    } catch (Exception e) {
      LOG.error("Error closing subscriber", e);
    }
    try {
      publisher.close();
    } catch (Exception e) {
      LOG.error("Error closing publisher", e);
    }
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept();
    REST_APP_1.stop();
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP_0, sql);
  }

  @Test
  public void shouldGetContinuousStreamOfResults() throws ExecutionException, InterruptedException {
    assertAllPersistentQueriesRunning(true);
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    makeRequestAndSetupSubscriber(
        "SELECT USERID, PAGEID, VIEWTIME from " + streamName + " EMIT CHANGES;",
        ImmutableMap.of("auto.offset.reset", "latest"),
        header, complete);

    header.get();
    assertExpectedScalablePushQueries(1, true);

    TEST_HARNESS.produceRows(pageViewDataProvider.topicName(), pageViewDataProvider,
        FormatFactory.KAFKA, FormatFactory.JSON);

    assertThatEventually(() -> subscriber.getUniqueRows().size(),
        is(pageViewDataProvider.data().size() + 1));
    List<StreamedRow> orderedRows = subscriber.getUniqueRows().stream()
        .sorted(this::compareByTimestamp)
        .collect(Collectors.toList());

    assertFirstBatchOfRows(orderedRows);
  }

  @Test
  public void shouldRebalance_addNewHost_skipServer1()
      throws ExecutionException, InterruptedException {
    assertAllPersistentQueriesRunning(false);
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    makeRequestAndSetupSubscriber(
        "SELECT USERID, PAGEID, VIEWTIME from " + streamName + " EMIT CHANGES;",
        ImmutableMap.of("auto.offset.reset", "latest"),
        header, complete);

    header.get();
    assertExpectedScalablePushQueries(1, false);

    TEST_HARNESS.produceRows(pageViewDataProvider.topicName(), pageViewDataProvider,
        FormatFactory.KAFKA, FormatFactory.JSON);

    assertThatEventually(() -> subscriber.getUniqueRows().size(),
        is(pageViewDataProvider.data().size() + 1));

    REST_APP_1.start();
    assertAllPersistentQueriesRunning(true);
    assertExpectedScalablePushQueries(1, true);

    TEST_HARNESS.produceRows(pageViewDataProvider.topicName(), pageViewAdditionalDataProvider,
        FormatFactory.KAFKA, FormatFactory.JSON);

    assertThatEventually(() -> subscriber.getUniqueRows().size(),
        is(pageViewDataProvider.data().size() + pageViewAdditionalDataProvider.data().size() + 1));

    List<StreamedRow> orderedRows = subscriber.getUniqueRows().stream()
        .sorted(this::compareByTimestamp)
        .collect(Collectors.toList());

    assertFirstBatchOfRows(orderedRows);
    assertSecondBatchOfRows(orderedRows);
  }

  @Test
  public void shouldRebalance_removeHost() throws ExecutionException, InterruptedException {
    assertAllPersistentQueriesRunning(true);
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final CompletableFuture<List<StreamedRow>> complete = new CompletableFuture<>();
    makeRequestAndSetupSubscriber(
        "SELECT USERID, PAGEID, VIEWTIME from " + streamName + " EMIT CHANGES;",
        ImmutableMap.of("auto.offset.reset", "latest"),
        header, complete);

    header.get();
    assertExpectedScalablePushQueries(1, true);

    TEST_HARNESS.produceRows(pageViewDataProvider.topicName(), pageViewDataProvider,
        FormatFactory.KAFKA, FormatFactory.JSON);

    assertThatEventually(() -> subscriber.getUniqueRows().size(),
        is(pageViewDataProvider.data().size() + 1));

    REST_APP_1.stop();

    TEST_HARNESS.produceRows(pageViewDataProvider.topicName(), pageViewAdditionalDataProvider,
        FormatFactory.KAFKA, FormatFactory.JSON);

    assertThatEventually(() -> subscriber.getUniqueRows().size(),
        is(pageViewDataProvider.data().size() + pageViewAdditionalDataProvider.data().size() + 1));

    List<StreamedRow> orderedRows = subscriber.getUniqueRows().stream()
        .sorted(this::compareByTimestamp)
        .collect(Collectors.toList());

    assertFirstBatchOfRows(orderedRows);
    assertSecondBatchOfRows(orderedRows);
  }

  private void assertFirstBatchOfRows(final List<StreamedRow> orderedRows) {
    assertThat(orderedRows.get(0).getHeader().get().getSchema(),
        is(LOGICAL_SCHEMA));
    assertThat(orderedRows.get(1),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_1", "PAGE_1", 1)))));
    assertThat(orderedRows.get(2),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_2", "PAGE_2", 2)))));
    assertThat(orderedRows.get(3),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_4", "PAGE_3", 3)))));
    assertThat(orderedRows.get(4),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_3", "PAGE_4", 4)))));
    assertThat(orderedRows.get(5),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_0", "PAGE_5", 5)))));
    assertThat(orderedRows.get(6),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_2", "PAGE_5", 6)))));
    assertThat(orderedRows.get(7),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_3", "PAGE_5", 7)))));
  }

  private void assertSecondBatchOfRows(final List<StreamedRow> orderedRows) {
    assertThat(orderedRows.get(8),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_2", "PAGE_4", 8)))));
    assertThat(orderedRows.get(9),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_3", "PAGE_2", 9)))));
    assertThat(orderedRows.get(10),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_4", "PAGE_1", 10)))));
    assertThat(orderedRows.get(11),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_0", "PAGE_5", 11)))));
    assertThat(orderedRows.get(12),
        is(StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of("USER_1", "PAGE_4", 12)))));
  }

  private int compareByTimestamp(final StreamedRow sr1, final StreamedRow sr2) {
    if (sr1.getHeader().isPresent()) {
      return -1;
    } else if (sr2.getHeader().isPresent()) {
      return 1;
    } else {
      List<Object> cols1 = new ArrayList<>(sr1.getRow().get().getColumns());
      List<Object> cols2 = new ArrayList<>(sr2.getRow().get().getColumns());
      return Long.compare(((Number) cols1.get(2)).longValue(),
          ((Number) cols2.get(2)).longValue());
    }
  }

  private void assertAllPersistentQueriesRunning(boolean app1) {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP_0.getEngine().getPersistentQueries()) {
        if (metadata.getState() != State.RUNNING) {
          return false;
        }
      }
      if (app1) {
        for (final PersistentQueryMetadata metadata : REST_APP_1.getEngine()
            .getPersistentQueries()) {
          if (metadata.getState() != State.RUNNING) {
            return false;
          }
        }
      }
      return true;
    }, is(true));
  }

  private void assertExpectedScalablePushQueries(
      final int expectedScalablePushQueries,
      final boolean app1
  ) {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP_0.getEngine().getPersistentQueries()) {
        if (metadata.getScalablePushRegistry().get().latestNumRegistered()
            < expectedScalablePushQueries
            || !metadata.getScalablePushRegistry().get().latestHasAssignment()) {
          return false;
        }
      }
      if (app1) {
        for (final PersistentQueryMetadata metadata : REST_APP_1.getEngine()
            .getPersistentQueries()) {
          if (metadata.getScalablePushRegistry().get().latestNumRegistered()
              < expectedScalablePushQueries
              || !metadata.getScalablePushRegistry().get().latestHasAssignment()) {
            return false;
          }
        }
      }
      return true;
    }, is(true));
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
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }

    return res.getResponse();
  }
}
