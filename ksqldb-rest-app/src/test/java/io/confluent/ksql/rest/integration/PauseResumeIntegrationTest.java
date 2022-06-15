/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PageViewDataProvider.Batch;
import kafka.zookeeper.ZooKeeperClientException;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static org.apache.kafka.clients.CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Category({IntegrationTest.class})
public class PauseResumeIntegrationTest {

  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private PageViewDataProvider PAGE_VIEWS_PROVIDER;
  private PageViewDataProvider PAGE_VIEWS_PROVIDER2;
  private String PAGE_VIEW_TOPIC;
  private String PAGE_VIEW_STREAM;
  private String SINK_TOPIC;
  private String SINK_STREAM;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static TestKsqlRestApp REST_APP;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = KsqlTestFolder.temporaryFolder();

  @BeforeClass
  public static void classSetUp() throws IOException {
    File BACKUP_LOCATION = TMP_FOLDER.newFolder();

    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KSQL_METASTORE_BACKUP_LOCATION, BACKUP_LOCATION.getPath())
        .withProperty(SESSION_TIMEOUT_MS_CONFIG, 10000) // Reduces the time for a rebalance during ksqlDB restart.
        .build();
  }

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Before
  public void setUpTopicsAndStreams() {
    REST_APP.start();
    int CURRENT_ID = COUNTER.getAndIncrement();

    String pageViewPrefix = "PAGEVIEW" + CURRENT_ID;
    PAGE_VIEWS_PROVIDER = new PageViewDataProvider(pageViewPrefix);
    PAGE_VIEWS_PROVIDER2 = new PageViewDataProvider(
        pageViewPrefix, Batch.BATCH2);
    PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
    PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
    SINK_TOPIC = "sink_topic" + CURRENT_ID;
    SINK_STREAM = "sink_stream" + CURRENT_ID;

    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);
  }

  @After
  public void cleanUp() {
    REST_APP.closePersistentQueries();
    REST_APP.dropSourcesExcept();
    REST_APP.stop();
  }

  @Test
  public void shouldPauseAndResumeQuery() {
    // Given:
    createQuery("");
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    Supplier<Integer> supplier = () -> RestIntegrationTestUtil.makeQueryRequest(REST_APP,
        "select * from " + SINK_STREAM + ";",Optional.empty()).size();

    // 7 records are produced + a header & footer.
    assertThatEventually(supplier, equalTo(9));

    // When:
    // JNH: factor out?
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE " + queryId + ";");

    assertThat(getPausedCount(), equalTo(1));

    // Produce more records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);

    // Observe no new records -- This kinda *doesn't work*.  Hard to prove a negative sometimes...
    assertThatEventually(supplier, equalTo(9));

    // Then:
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME "
        + queryId + ";");

    // 5 more records have been produced
    assertThatEventually(supplier, equalTo(14));
    assertThat(getRunningCount(), equalTo(1));
  }

  @Test
  public void shouldPauseAndResumeMultipleQueries() {
    // Given:
    createQuery("1");
    createQuery("2");

    Supplier<Integer> supplier = getSupplier(1);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    // 7 records are produced + a header & footer.
    assertThatEventually(supplier, equalTo(9));

    // When:
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE " + queryId + ";");
    assertThat(getPausedCount(), equalTo(1));

    // Produce more records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);
    
    assertThatEventually(supplier, equalTo(9));

    // Then:
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME " + queryId + ";");

    // 5 more records have been produced
    assertThatEventually(supplier, equalTo(14));

    List <KsqlEntity> showQueries3 = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;");
    assertThat(((Queries) showQueries3.get(0)).getQueries().get(0).getStatusCount().getStatuses()
        .get(KsqlQueryStatus.RUNNING), equalTo(1));
  }

  @Test
  public void pausedQueriesShouldBePausedOnRestart()  {
    // Given:
    createQuery("1");
    createQuery("2");

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON,
        System::currentTimeMillis);

    Supplier<Integer> supplier = getSupplier(1);
    Supplier<Integer> supplier2 = getSupplier(2);

    // 7 records are produced + a header & footer.
    assertThatEventually(supplier, equalTo(9));
    assertThatEventually(supplier2, equalTo(9));

    // When:
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE "
        + queryId + ";");
    assertThatEventually(supplier, equalTo(9));

    // Restart server
    REST_APP.stop();
    REST_APP.start();

    // Verify PAUSED state -- eventually
    assertThatEventually(this::getPausedCount, equalTo(1));

    // Verify number of processed records
    assertThatEventually(supplier, equalTo(9));

    // Then:
    String queryId2 = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME " + queryId2 + ";");
    assertThatEventually(this::getPausedCount, equalTo(0));
    assertThatEventually(this::getRunningCount, equalTo(1));

    // Adding more after restart.
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);

    assertThatEventually(supplier, equalTo(14));

    List <KsqlEntity> showQueries3 = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;");
    assertThat(((Queries) showQueries3.get(0)).getQueries().get(0).getStatusCount().getStatuses()
        .get(KsqlQueryStatus.RUNNING), equalTo(1));
  }

  // Parameterize this entire class to run with multiple REST instances?
  // @Test
  public void pauseResumeShouldWorkWithMultipleServers() {

  }

  private void createQuery(final String suffix) {
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM " + SINK_STREAM + suffix
            + " WITH (kafka_topic='" + SINK_TOPIC + suffix + "',format='json')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC + suffix);
  }

  @NotNull
  private Supplier<Integer> getSupplier(int streamNumber) {
    return () -> RestIntegrationTestUtil.makeQueryRequest(REST_APP,
        "select * from " + SINK_STREAM + streamNumber + ";",Optional.empty()).size();
  }

  private int getPausedCount() {
    return getCount(KsqlQueryStatus.PAUSED);
  }

  private int getRunningCount() {
    return getCount(KsqlQueryStatus.RUNNING);
  }

  private int getCount(KsqlQueryStatus status) {
    try {

    List <KsqlEntity> showQueries2 = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;");
    System.out.println("Show queries " + showQueries2.stream().map( q ->
            ((Queries)q).getQueries().get(0).getStatusCount().toString())
        .collect(Collectors.joining("\n")));

    // This failing shows that the command topic isn't being read completely?
    return ((Queries) showQueries2.get(0)).getQueries().get(0).getStatusCount().getStatuses()
        .get(status);
    } catch (Exception e) {
      return 0;
    }
  }
}
