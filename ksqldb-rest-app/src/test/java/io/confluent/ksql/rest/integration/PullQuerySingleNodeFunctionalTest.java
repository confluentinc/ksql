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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.getOffsets;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makeAdminRequestWithResponse;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.makePullQueryRequest;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForStreamsMetadataToInitialize;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.FaultyKafkaConsumer.FaultyKafkaConsumer0;
import io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.Shutoffs;
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
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.UserDataProvider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IntegrationTest.class})
public class PullQuerySingleNodeFunctionalTest {

  private static final Logger LOG = LoggerFactory.getLogger(PullQuerySingleNodeFunctionalTest.class);

  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query with ID (\\S+)");
  private static final String USER_TOPIC = "user_topic_";
  private static final String USERS_STREAM = "users";
  private static final UserDataProvider USER_PROVIDER = new UserDataProvider();
  private static final int HEADER = 1;
  private static final int BASE_TIME = 1_000_000;
  private final static String KEY = USER_PROVIDER.getStringKey(0);
  private final static String KEY_3 = USER_PROVIDER.getStringKey(3);
  private static final Map<String, ?> LAG_FILTER_3 =
      ImmutableMap.of(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG, "3");

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

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
  private static final int INT_PORT_0 = TestUtils.findFreeLocalPort();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", INT_PORT_0);
  private static final Shutoffs APP_SHUTOFFS_0 = new Shutoffs();

  @Rule
  public final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir())
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:" + INT_PORT_0)
      .withFaultyKsqlClient(APP_SHUTOFFS_0::getKsqlOutgoing)
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX
          + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, FaultyKafkaConsumer0.class.getName())
      .withProperty(KSQL_STREAMS_PREFIX + CONSUMER_PREFIX + "max.poll.records", 1)
      .withProperties(COMMON_CONFIG)
      .build();

  private final AtomicLong timestampSupplier = new AtomicLong(BASE_TIME);
  private String output;
  private String queryId;
  private String sql;
  private String sqlKey3;
  private String topic;

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS).around(TMP);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(1, TimeUnit.MINUTES)
      .withLookingForStuckThread(true)
      .build();


  @BeforeClass
  public static void setUpClass() {
    FaultyKafkaConsumer0.setPauseOffset(APP_SHUTOFFS_0::getKafkaPauseOffset);
  }

  @After
  public void cleanUp() {
    REST_APP_0.closePersistentQueries();
    REST_APP_0.dropSourcesExcept();
    APP_SHUTOFFS_0.reset();
  }

  @Before
  public void setUp() {
    //Create topic with 1 partition to control who is active and standby
    topic = USER_TOPIC + KsqlIdentifierTestUtil.uniqueIdentifierName();
    TEST_HARNESS.ensureTopics(1, topic);

    final Multimap<GenericKey, RecordMetadata> producedRows = TEST_HARNESS.produceRows(
        topic,
        USER_PROVIDER,
        FormatFactory.KAFKA,
        FormatFactory.JSON,
        timestampSupplier::getAndIncrement
    );

    LOG.info("Produced rows: " + producedRows.size());

    //Create stream
    makeAdminRequest(
        REST_APP_0,
        "CREATE STREAM " + USERS_STREAM
            + " (" + USER_PROVIDER.ksqlSchemaString(false) + ")"
            + " WITH ("
            + "   kafka_topic='" + topic + "', "
            + "   value_format='JSON');"
    );
    //Create table
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();
    sql = "SELECT * FROM " + output + " WHERE USERID = '" + KEY + "';";
    List<KsqlEntity> res = makeAdminRequestWithResponse(
        REST_APP_0,
        "CREATE TABLE " + output + " AS"
            + " SELECT " + USER_PROVIDER.key() + ", COUNT(1) AS COUNT FROM " + USERS_STREAM
            + " GROUP BY " + USER_PROVIDER.key() + ";"
    );
    queryId = extractQueryId(res.get(0).toString());
    queryId = queryId.substring(0, queryId.length() - 1);
    waitForTableRows(TEST_HARNESS);

    sqlKey3 = "SELECT * FROM " + output + " WHERE USERID = '" + KEY_3
        + "';";
    waitForStreamsMetadataToInitialize(
        REST_APP_0, ImmutableList.of(host0));
  }

  @Ignore
  @Test
  public void restoreAfterClearState() {
    waitForStreamsMetadataToInitialize(REST_APP_0, ImmutableList.of(host0));
    waitForRemoteServerToChangeStatus(REST_APP_0, host0, HighAvailabilityTestUtil
        .lagsReported(host0, Optional.empty(), 5));

    // When:
    final List<StreamedRow> rows_0 = makePullQueryRequest(
        REST_APP_0, sql, LAG_FILTER_3);

    // Then:
    assertThat(rows_0, hasSize(HEADER + 1));
    KsqlHostInfoEntity host = rows_0.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(host0.getHost()));
    assertThat(host.getPort(), is(host0.getPort()));
    assertThat(rows_0.get(1).getRow(), is(not(Optional.empty())));
    assertThat(rows_0.get(1).getRow().get().getColumns(), is(ImmutableList.of(KEY, 1)));

    // Stop the server and blow away the state
    LOG.info("Shutting down the server " + host0.toString());
    REST_APP_0.stop();
    String stateDir = (String)REST_APP_0.getBaseConfig()
        .get(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG);
    clearDir(stateDir);

    // Pause incoming kafka consumption
    APP_SHUTOFFS_0.setKafkaPauseOffset(2);

    LOG.info("Restarting the server " + host0.toString());
    REST_APP_0.start();

    waitForStreamsMetadataToInitialize(REST_APP_0, ImmutableList.of(host0));
    waitForRemoteServerToChangeStatus(REST_APP_0, host0, HighAvailabilityTestUtil
        .lagsReported(host0, Optional.of(2L), 5));

    ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil
        .sendClusterStatusRequest(REST_APP_0);
    Pair<Long, Long> pair = getOffsets(host0, clusterStatusResponse.getClusterStatus());
    assertThat(pair.left, is(2L));
    assertThat(pair.right, is(5L));

    final List<StreamedRow> sameRows = makePullQueryRequest(
        REST_APP_0, sql, LAG_FILTER_3);

    host = sameRows.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(host0.getHost()));
    assertThat(host.getPort(), is(host0.getPort()));
    assertThat(sameRows.get(1).getRow(), is(not(Optional.empty())));
    // Still haven't gotten the update yet
    assertThat(sameRows.get(1).getRow().get().getColumns(), is(ImmutableList.of(KEY, 1)));

    // Row not found!
    final List<StreamedRow> headerOnly = makePullQueryRequest(
        REST_APP_0, sqlKey3, LAG_FILTER_3);
    assertThat(headerOnly.size(), is(1));

    // Unpause incoming kafka consumption. We then expect active to catch back up.
    APP_SHUTOFFS_0.setKafkaPauseOffset(-1);

    waitForRemoteServerToChangeStatus(REST_APP_0, host0, HighAvailabilityTestUtil
        .lagsReported(host0, Optional.of(5L), 5));

    clusterStatusResponse = HighAvailabilityTestUtil
        .sendClusterStatusRequest(REST_APP_0);
    pair = getOffsets(host0, clusterStatusResponse.getClusterStatus());
    assertThat(pair.left, is(5L));
    assertThat(pair.right, is(5L));

    final List<StreamedRow> updatedRows = makePullQueryRequest(
        REST_APP_0, sqlKey3, LAG_FILTER_3);

    // Now it is found!
    host = updatedRows.get(1).getSourceHost().get();
    assertThat(host.getHost(), is(host0.getHost()));
    assertThat(host.getPort(), is(host0.getPort()));
    assertThat(updatedRows.get(1).getRow(), is(not(Optional.empty())));
    assertThat(updatedRows.get(1).getRow().get().getColumns(), is(ImmutableList.of(KEY_3, 1)));
  }


  private static String extractQueryId(final String outputString) {
    final java.util.regex.Matcher matcher = QUERY_ID_PATTERN.matcher(outputString);
    assertThat("Could not find query id in: " + outputString, matcher.find());
    return matcher.group(1);
  }

  private static String getNewStateDir() {
    try {
      return TMP.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }

  private void waitForTableRows(final IntegrationTestHarness testHarness) {
    testHarness.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_PROVIDER.data().size(),
        FormatFactory.KAFKA,
        FormatFactory.JSON,
        AGGREGATE_SCHEMA
    );
  }

  private static void clearDir(String stateDir) {
    LOG.info("Deleting tmp dir " + stateDir);
    try {
      List<Path> children = Files.list(Paths.get(stateDir)).collect(Collectors.toList());
      for (Path path : children) {
        Files.walk(path)
            .map(Path::toFile)
            .sorted(Comparator.reverseOrder())
            .forEach(File::delete);
      }

    } catch (IOException e) {
      throw new AssertionError("Can't delete", e);
    }
  }
}
