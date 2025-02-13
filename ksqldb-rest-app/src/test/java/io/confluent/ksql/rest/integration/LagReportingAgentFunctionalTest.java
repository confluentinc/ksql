package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.sendClusterStatusRequest;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.PageViewDataProvider;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class LagReportingAgentFunctionalTest {
  private static final Logger LOG = LoggerFactory.getLogger(LagReportingAgentFunctionalTest.class);
  private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final int NUM_ROWS = PAGE_VIEWS_PROVIDER.data().size();

  private static final QueryStateStoreId STORE_0 = QueryStateStoreId.of(
      "CTAS_USER_VIEWS_3",
      "Aggregate-Aggregate-Materialize");
  private static final QueryStateStoreId STORE_1 = QueryStateStoreId.of(
      "CTAS_USER_LATEST_VIEWTIME_5",
      "Aggregate-Aggregate-Materialize");

  private static final int INT_PORT0 = TestUtils.findFreeLocalPort();
  private static final int INT_PORT1 = TestUtils.findFreeLocalPort();
  private static final KsqlHostInfoEntity HOST0 = new KsqlHostInfoEntity("localhost", INT_PORT0);
  private static final KsqlHostInfoEntity HOST1 = new KsqlHostInfoEntity("localhost", INT_PORT1);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT0)
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT0)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
      .withProperty(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      // Heartbeat
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      // Lag Reporting
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .withProperty("ksql.runtime.feature.shared.enabled", true)
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT1)
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT1)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
      .withProperty(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      // Heartbeat
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      // Lag Reporting
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .withProperty("ksql.runtime.feature.shared.enabled", true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.deleteInternalTopics("KSQL");
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE USER_VIEWS AS SELECT USERID, count(*) FROM " + PAGE_VIEW_STREAM
            + " GROUP BY USERID;"
    );
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE USER_LATEST_VIEWTIME AS SELECT USERID, max(VIEWTIME) FROM " + PAGE_VIEW_STREAM
            + " GROUP BY USERID;"
    );
  }

  @Test(timeout = 60000)
  public void shouldExchangeLags() {
    // Given:
    waitForClusterCondition(LagReportingAgentFunctionalTest::allServersDiscovered);

    // When:
    ClusterStatusResponse resp =
        waitForClusterCondition(LagReportingAgentFunctionalTest::allLagsReported);
    StateStoreLags stateStoreLags =
        resp.getClusterStatus().entrySet().iterator().next().getValue().getHostStoreLags()
            .getStateStoreLags(STORE_0).get();

    // Then:
    // Read the raw Kafka data from the topic to verify the reported lags
    final List<ConsumerRecord<byte[], byte[]>> records =
        TEST_HARNESS.verifyAvailableRecords("_confluent-ksql-default_query-CTAS_USER_VIEWS_3-"
            + "Aggregate-Aggregate-Materialize-changelog", NUM_ROWS);
    Map<Integer, Optional<ConsumerRecord<byte[], byte[]>>> partitionToMaxOffset =
        records.stream()
        .collect(Collectors.groupingBy(ConsumerRecord::partition, Collectors.maxBy(
            Comparator.comparingLong(ConsumerRecord::offset))));
    Assert.assertEquals(2, partitionToMaxOffset.size());
    Optional<LagInfoEntity> lagInfoEntity0 = stateStoreLags.getLagByPartition(0);
    Optional<LagInfoEntity> lagInfoEntity1 = stateStoreLags.getLagByPartition(1);
    long partition0Offset = lagInfoEntity0.get().getCurrentOffsetPosition();
    long partition1Offset = lagInfoEntity1.get().getCurrentOffsetPosition();
    Assert.assertEquals(partition0Offset, partitionToMaxOffset.get(0).get().offset() + 1);
    Assert.assertEquals(partition1Offset, partitionToMaxOffset.get(1).get().offset() + 1);
  }

  private ClusterStatusResponse waitForClusterCondition(
      Function<ClusterStatusResponse, Boolean> function) {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(REST_APP_0);
      if (function.apply(clusterStatusResponse)) {
        return clusterStatusResponse;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        Assert.fail();
      }
    }
  }

  private static boolean allServersDiscovered(ClusterStatusResponse clusterStatusResponse) {
    if (clusterStatusResponse.getClusterStatus().size() < 2) {
      return false;
    }
    return true;
  }

  private static boolean allLagsReported(ClusterStatusResponse response) {
    if (response.getClusterStatus().size() == 2) {
      HostStoreLags store0 = response.getClusterStatus().get(HOST0).getHostStoreLags();
      HostStoreLags store1 = response.getClusterStatus().get(HOST1).getHostStoreLags();
      if (arePartitionsCurrent(store0) && arePartitionsCurrent(store1)) {
        LOG.info("Found expected lags: {}", response.getClusterStatus().toString());
        return true;
      }
    }
    LOG.info("Didn't yet find expected lags: {}", response.getClusterStatus().toString());
    return false;
  }

  private static boolean arePartitionsCurrent(HostStoreLags stores) {
    if (stores.getStateStoreLags().size() < 2) {
      return false;
    }
    StateStoreLags stateStoreLags0 = stores.getStateStoreLags(STORE_0).get();
    StateStoreLags stateStoreLags1 = stores.getStateStoreLags(STORE_1).get();
    return stateStoreLags0.getSize() == 2 &&
        stateStoreLags1.getSize() == 2 &&
        isCurrent(stateStoreLags0, 0) && isCurrent(stateStoreLags0, 1) &&
        isCurrent(stateStoreLags1, 0) && isCurrent(stateStoreLags1, 1) &&
        (numMessages(stateStoreLags0, 0) + numMessages(stateStoreLags0, 1) == NUM_ROWS) &&
        (numMessages(stateStoreLags1, 0) + numMessages(stateStoreLags1, 1) == NUM_ROWS);
  }

  private static boolean isCurrent(final StateStoreLags stateStoreLags,
                                   final int partition) {
    final Optional<LagInfoEntity> lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.get().getCurrentOffsetPosition() > 0 && lagInfo.get().getOffsetLag() == 0;
  }

  private static long numMessages(final StateStoreLags stateStoreLags,
                                  final int partition) {
    final Optional<LagInfoEntity> lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.get().getCurrentOffsetPosition();
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}
