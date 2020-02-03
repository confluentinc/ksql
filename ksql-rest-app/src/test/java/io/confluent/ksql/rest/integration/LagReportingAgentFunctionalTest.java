package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
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
import org.junit.AfterClass;
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
  private static final TemporaryFolder TMP = new TemporaryFolder();

  static {
    try {
      TMP.create();
    } catch (final IOException e) {
      throw new AssertionError("Failed to init TMP", e);
    }
  }

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();
  private static final int NUM_ROWS = PAGE_VIEWS_PROVIDER.data().size();

  private static final QueryStateStoreId STORE_0 = QueryStateStoreId.of(
      "_confluent-ksql-default_query_CTAS_USER_VIEWS_3",
      "Aggregate-Aggregate-Materialize");
  private static final QueryStateStoreId STORE_1 = QueryStateStoreId.of(
      "_confluent-ksql-default_query_CTAS_USER_LATEST_VIEWTIME_5",
      "Aggregate-Aggregate-Materialize");

  private static final KsqlHostEntity HOST0 = new KsqlHostEntity("localhost", 8088);
  private static final KsqlHostEntity HOST1 = new KsqlHostEntity("localhost", 8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
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
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
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
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, Format.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE USER_VIEWS AS SELECT count(*) FROM " + PAGE_VIEW_STREAM
            + " GROUP BY USERID;"
    );
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE USER_LATEST_VIEWTIME AS SELECT max(VIEWTIME) FROM " + PAGE_VIEW_STREAM
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
        resp.getClusterStatus().entrySet().iterator().next().getValue().getHostStoreLags().getStateStoreLags(STORE_0);

    // Then:
    // Read the raw Kafka data from the topic to verify the reported lags
    final List<ConsumerRecord<byte[], byte[]>> records =
        TEST_HARNESS.verifyAvailableRecords("_confluent-ksql-default_query_CTAS_USER_VIEWS_3-"
            + "Aggregate-Aggregate-Materialize-changelog", NUM_ROWS);
    Map<Integer, Optional<ConsumerRecord<byte[], byte[]>>> partitionToMaxOffset =
        records.stream()
        .collect(Collectors.groupingBy(ConsumerRecord::partition, Collectors.maxBy(
            Comparator.comparingLong(ConsumerRecord::offset))));
    Assert.assertEquals(2, partitionToMaxOffset.size());
    long partition0Offset = stateStoreLags.getLagByPartition(0).getCurrentOffsetPosition();
    long partition1Offset = stateStoreLags.getLagByPartition(1).getCurrentOffsetPosition();
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

  private static ClusterStatusResponse sendClusterStatusRequest(final TestKsqlRestApp restApp) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {

      final RestResponse<ClusterStatusResponse> res = restClient.makeClusterStatusRequest();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }

      return res.getResponse();
    }
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
    return stores.getStateStoreLags().size() == 2 &&
        stores.getStateStoreLags(STORE_0).getSize() == 2 &&
        stores.getStateStoreLags(STORE_1).getSize() == 2 &&
        isCurrent(stores, STORE_0, 0) && isCurrent(stores, STORE_0, 1) &&
        isCurrent(stores, STORE_1, 0) && isCurrent(stores, STORE_1, 1) &&
        (numMessages(stores, STORE_0, 0) + numMessages(stores, STORE_0, 1) == NUM_ROWS) &&
        (numMessages(stores, STORE_1, 0) + numMessages(stores, STORE_1, 1) == NUM_ROWS);
  }

  private static boolean isCurrent(final HostStoreLags hostStoreLags,
                                   final QueryStateStoreId queryStateStoreId,
                                   final int partition) {
    final StateStoreLags stateStoreLags = hostStoreLags.getStateStoreLags(queryStateStoreId);
    final LagInfoEntity lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.getCurrentOffsetPosition() > 0 && lagInfo.getOffsetLag() == 0;
  }

  private static long numMessages(final HostStoreLags hostStoreLags,
                                  final QueryStateStoreId queryStateStoreId,
                                  final int partition) {
    final StateStoreLags stateStoreLags = hostStoreLags.getStateStoreLags(queryStateStoreId);
    final LagInfoEntity lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.getCurrentOffsetPosition();
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}
