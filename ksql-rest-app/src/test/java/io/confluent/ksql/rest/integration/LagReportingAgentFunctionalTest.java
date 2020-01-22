package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterLagsResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.util.PageViewDataProvider;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import kafka.zookeeper.ZooKeeperClientException;
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

  private static final HostInfoEntity HOST0 = new HostInfoEntity("localhost",8088);
  private static final HostInfoEntity HOST1 = new HostInfoEntity("localhost",8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 3000)
      // Heartbeat
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      // Lag Reporting
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_DATA_EXPIRATION_MS_CONFIG, 60000)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 3000)
      // Heartbeat
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      // Lag Reporting
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_DATA_EXPIRATION_MS_CONFIG, 60000)
      .withProperty(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .withProperties(ClientTrustStore.trustStoreProps())
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
  }

  @AfterClass
  public static void tearDownClass() {
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept();
  }

  @Test(timeout = 60000)
  public void shouldExchangeLags() {
    // Given:
    waitForClusterToBeDiscovered();

    // When:
    ClusterLagsResponse resp = waitForLags(LagReportingAgentFunctionalTest::allLagsReported);
    Map<Integer, Map<String, LagInfoEntity>> partitions =
        resp.getLags().entrySet().iterator().next().getValue();

    // Then:
    Assert.assertEquals(1, partitions.get(0).size());
    Assert.assertTrue(partitions.get(0).values().iterator().next().getCurrentOffsetPosition() > 0);
    Assert.assertEquals(1, partitions.get(1).size());
    Assert.assertTrue(partitions.get(1).values().iterator().next().getCurrentOffsetPosition() > 0);
  }

  private void waitForClusterToBeDiscovered() {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(REST_APP_0);
      if (allServersDiscovered(clusterStatusResponse.getClusterStatus())) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  private boolean allServersDiscovered(Map<String, HostStatusEntity> clusterStatus) {
    if (clusterStatus.size() < 2) {
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

  private static ClusterLagsResponse getLags(final TestKsqlRestApp restApp)
  {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {

      final RestResponse<ClusterLagsResponse> res = restClient.makeClusterLagsRequest();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }

      return res.getResponse();
    }
  }

  private static boolean allLagsReported(Map<String, Map<Integer, Map<String, LagInfoEntity>>> lags) {
    if (lags.size() == 1 && lags.entrySet().iterator().next().getValue().size() == 2) {
      LOG.info("Found expected lags {}", lags.toString());
      return true;
    }
    return false;
  }

  private ClusterLagsResponse waitForLags(
      Function<Map<String, Map<Integer, Map<String, LagInfoEntity>>>, Boolean> function)
  {
    while (true) {
      final ClusterLagsResponse clusterLagsResponse = getLags(REST_APP_0);
      if (function.apply(clusterLagsResponse.getLags())) {
        return clusterLagsResponse;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        Assert.fail();
      }
    }
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}
