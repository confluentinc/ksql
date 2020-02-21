package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.integration.RestIntegrationTestUtil.extractQueryId;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.SkippableTestKsqlRestAppWrapper;
import io.confluent.ksql.rest.server.SkippableTestKsqlRestAppWrapper.SkipHost;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
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
  private static final int NUM_ROWS = PAGE_VIEWS_PROVIDER.data().size();

  private static final String QUERY_PREFIX = "_confluent-ksql-default_query_";
  private static final String STORE_NAME = "Aggregate-Aggregate-Materialize";

  private static final KsqlHostInfoEntity HOST0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity HOST1 = new KsqlHostInfoEntity("localhost", 8089);

  private static final Map<String, Object> COMMON_CONFIG = ImmutableMap.<String, Object>builder()
      .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
      .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 200)
      .put(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS, true)
      // Heartbeat
      .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 1000)
      .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      // Lag Reporting
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG, true)
      .put(KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG, 3000)
      .build();

  private static final String APP_1_NAME = "APP1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withEnabledKsqlClient()
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperties(COMMON_CONFIG)
      .build();

  @Rule
  public SkippableTestKsqlRestAppWrapper REST_APP_1 = new SkippableTestKsqlRestAppWrapper(
      TestKsqlRestApp
          .builder(TEST_HARNESS::kafkaBootstrapServers)
          .withEnabledKsqlClient()
          .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
          .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
          .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
          .withProperties(COMMON_CONFIG)
          .build(),
      APP_1_NAME);

  private QueryStateStoreId query0;
  private QueryStateStoreId query1;

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);
  }

  @Before
  public void setUp() {
    String pageViewStream = KsqlIdentifierTestUtil.uniqueIdentifierName();
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM " + pageViewStream
            + " (" + PAGE_VIEWS_PROVIDER.ksqlSchemaString() + ") "
            + "WITH (kafka_topic='" + PAGE_VIEWS_PROVIDER.topicName() + "', value_format='json');"
    );

    String userViewsTable = KsqlIdentifierTestUtil.uniqueIdentifierName();
    List<KsqlEntity> res = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE " + userViewsTable + " AS SELECT COUNT(*) FROM " + pageViewStream
            + " GROUP BY USERID;"
    );
    String userViewsQueryId = QUERY_PREFIX + extractQueryId(res.get(0).toString());
    query0 = QueryStateStoreId.of(userViewsQueryId, STORE_NAME);

    String userLatestViewTimeTable = KsqlIdentifierTestUtil.uniqueIdentifierName();
    res = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE TABLE " + userLatestViewTimeTable + " AS SELECT max(VIEWTIME) FROM "
            + pageViewStream
            + " GROUP BY USERID;"
    );
    String userLatestViewTime = QUERY_PREFIX + extractQueryId(res.get(0).toString());
    query1 = QueryStateStoreId.of(userLatestViewTime, STORE_NAME);
  }

  @Test(timeout = 60000)
  public void shouldExchangeLags() {
    // Given:
    waitForClusterCondition(allServersDiscovered(2));

    // When:
    ClusterStatusResponse resp =
        waitForClusterCondition(allLagsReported(2));
    StateStoreLags stateStoreLags =
        resp.getClusterStatus().entrySet().iterator().next().getValue().getHostStoreLags()
            .getStateStoreLags(query0).get();

    // Then:
    // Read the raw Kafka data from the topic to verify the reported lags
    final List<ConsumerRecord<byte[], byte[]>> records =
        TEST_HARNESS.verifyAvailableRecords(query0.getQueryApplicationId() + "-"
            + query0.getStateStoreName() + "-changelog", NUM_ROWS);
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

  @Test(timeout = 60000)
  @SkipHost(names = {APP_1_NAME})
  public void shouldHaveLags_singleHost() {
    // Given:
    waitForClusterCondition(allServersDiscovered(1));

    // When:
    waitForClusterCondition(allLagsReported(1));
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

  private static Function<ClusterStatusResponse, Boolean> allServersDiscovered(int expectedSize) {
    return (clusterStatusResponse) -> {
      if (clusterStatusResponse.getClusterStatus().size() != expectedSize) {
        return false;
      }
      return true;
    };
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

  private Function<ClusterStatusResponse, Boolean> allLagsReported(int expectedSize) {
    return (response) -> {
      Map<KsqlHostInfoEntity, HostStatusEntity> aliveStatuses =
          response.getClusterStatus().entrySet().stream()
              .filter(e -> e.getValue().getHostAlive())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      if (aliveStatuses.size() == 2 && expectedSize == 2) {
        HostStoreLags store0 = aliveStatuses.get(HOST0).getHostStoreLags();
        HostStoreLags store1 = aliveStatuses.get(HOST1).getHostStoreLags();
        if (arePartitionsCurrent(store0) && arePartitionsCurrent(store1)) {
          LOG.info("Found expected lags: {}", response.getClusterStatus().toString());
          return true;
        }
      } else if (aliveStatuses.size() == 1 && expectedSize == 1) {
        HostStoreLags store0 = aliveStatuses.get(HOST0).getHostStoreLags();
        if (arePartitionsCurrent(store0)) {
          LOG.info("Found expected lags: {}", response.getClusterStatus().toString());
          return true;
        }
      }
      LOG.info("Didn't yet find expected lags: {}", response.getClusterStatus().toString());
      return false;
    };
  }

  private boolean arePartitionsCurrent(HostStoreLags stores) {
    if (stores.getStateStoreLags().size() < 2) {
      return false;
    }
    StateStoreLags stateStoreLags0 = stores.getStateStoreLags(query0).get();
    StateStoreLags stateStoreLags1 = stores.getStateStoreLags(query1).get();
    return stateStoreLags0.getSize() == 2 &&
        stateStoreLags1.getSize() == 2 &&
        isCurrent(stateStoreLags0, 0) && isCurrent(stateStoreLags0, 1) &&
        isCurrent(stateStoreLags1, 0) && isCurrent(stateStoreLags1, 1) &&
        (numMessages(stateStoreLags0, 0) + numMessages(stateStoreLags0, 1) == NUM_ROWS) &&
        (numMessages(stateStoreLags1, 0) + numMessages(stateStoreLags1, 1) == NUM_ROWS);
  }

  private boolean isCurrent(final StateStoreLags stateStoreLags,
                            final int partition) {
    final Optional<LagInfoEntity> lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.get().getCurrentOffsetPosition() > 0 && lagInfo.get().getOffsetLag() == 0;
  }

  private long numMessages(final StateStoreLags stateStoreLags,
                           final int partition) {
    final Optional<LagInfoEntity> lagInfo = stateStoreLags.getLagByPartition(partition);
    return lagInfo.get().getCurrentOffsetPosition();
  }

  private String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}
