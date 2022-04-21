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

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.AssertEventually;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PageViewDataProvider.Batch;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.impl.FutureConvertersImpl.P;

@Category({IntegrationTest.class})
public class PauseResumeIntegrationTest {

  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private Integer CURRENT_ID;
  private PageViewDataProvider PAGE_VIEWS_PROVIDER;
  private PageViewDataProvider PAGE_VIEWS_PROVIDER2;
  private String PAGE_VIEW_TOPIC;
  private String PAGE_VIEW_STREAM;
  private String SINK_TOPIC;
  private String SINK_STREAM;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  //  @ClassRule
  //  public static TemporaryFolder TMP_FOLDER;
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = KsqlTestFolder.temporaryFolder();

  private static File BACKUP_LOCATION;

//  static {
//    try {
//      TMP_FOLDER = KsqlTestFolder.temporaryFolder();
//      BACKUP_LOCATION = KsqlTestFolder.temporaryFolder().newFolder();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }

  private static TestKsqlRestApp REST_APP;

  @BeforeClass
  public static void classSetUp() throws Exception {
    BACKUP_LOCATION = TMP_FOLDER.newFolder();

    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        //.withStaticServiceContext(TEST_HARNESS::getServiceContext)
        .withProperty(KSQL_METASTORE_BACKUP_LOCATION, BACKUP_LOCATION.getPath())
        //.withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
        .build();
  }

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);
      //.around(REST_APP);

  @Before
  public void setUpTopicsAndStreams() {
    REST_APP.start();
    CURRENT_ID = COUNTER.getAndIncrement();

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
  public void shouldPauseAndResumeQuery() throws Exception {
    // Given:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',format='json')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC);
    // Observe query state RUNNING
    // Produce some records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    // JNH: Observe some output.
    Supplier<Integer> supplier = () -> RestIntegrationTestUtil.makeQueryRequest(REST_APP,
        "select * from " + SINK_STREAM + ";",Optional.empty()).size();

    // 7 records are produced + a header & footer.
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // When:
    // JNH: Pausing a query
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    List <KsqlEntity> pauseResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE "
        + queryId + ";");

    // JNH: Observe query state PAUSED
    System.out.println("Pause response " + pauseResponse);
    assertThat(getPausedCount(), equalTo(1));

    // Produce more records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);

    // Give time to process
    Thread.sleep(2000);

    // JNH: Observe no further output somehow?
    // Observe no new records -- This kinda *doesn't work*.  Hard to prove a negative sometimes...
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // Then:
    // JNH: After resuming

    List <KsqlEntity> resumeResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME "
        + queryId + ";");

    // JNH: Observe more output!
    // 5 more records have been produced
    AssertEventually.assertThatEventually(supplier, equalTo(14));

    // JNH: Observe query state RUNNING
    assertThat(getRunningCount(), equalTo(1));
  }

  // JNH - Update this to have multiple queries and show that there are no interactions
  // @Test
  public void shouldPauseAndResumeMultipleQueries() throws Exception {
    // Given:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',format='json')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC);

    // Starting count...
    Supplier<Integer> supplier = () -> RestIntegrationTestUtil.makeQueryRequest(REST_APP,
        "select * from " + SINK_STREAM + ";",Optional.empty()).size();
    //Integer initialCount = supplier.get();
    //System.out.println("Initial count " + initialCount);

    // Observe query state RUNNING
    // Produce some records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    // JNH: Observe some output.
    // 7 records are produced + a header & footer.
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // When:
    // JNH: Pausing a query
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    List <KsqlEntity> pauseResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE "
        + queryId + ";");

    // JNH: Observe query state PAUSED
    System.out.println("Pause response " + pauseResponse);
    assertThat(getPausedCount(), equalTo(1));

    // Give time to process
    Thread.sleep(2000);

    // Produce more records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);

    // JNH: Observe no further output somehow?
    // Observe no new records -- This kinda *doesn't work*.  Hard to prove a negative sometimes...
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // Then:
    // JNH: After resuming
    List <KsqlEntity> resumeResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME "
        + queryId + ";");

    // JNH: Observe more output!
    // 5 more records have been produced
    AssertEventually.assertThatEventually(supplier, equalTo(14));

    // JNH: Observe query state RUNNING
    List <KsqlEntity> showQueries3 = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;");
    assertThat(((Queries) showQueries3.get(0)).getQueries().get(0).getStatusCount().getStatuses()
        .get(KsqlQueryStatus.RUNNING), equalTo(1));

  }

  // Further, paused queries should not process data on server restart
  // JNH: Presently broken.  Cannot resume after restart :(
  // @Test
  public void pausedQueriesShouldBePausedOnRestart() throws Exception  {
    // Given:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM " + SINK_STREAM
            + " WITH (kafka_topic='" + SINK_TOPIC + "',format='json')"
            + " AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );

    TEST_HARNESS.getKafkaCluster().waitForTopicsToBePresent(SINK_TOPIC);
    // Observe query state RUNNING
    // Produce some records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, KAFKA, JSON, System::currentTimeMillis);

    // JNH: Observe some output.
    Supplier<Integer> supplier = () -> {
      int size = RestIntegrationTestUtil.makeQueryRequest(REST_APP,
          "select * from " + SINK_STREAM + ";",Optional.empty()).size();
      System.out.println("JNH: Got query size: " + size);
      return size;
    };

    // 7 records are produced + a header & footer.
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // When:
    // JNH: Pausing a query
    String queryId = ((Queries) RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;").get(0)).getQueries().get(0).getId().toString();
    List <KsqlEntity> pauseResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "PAUSE "
        + queryId + ";");

    // JNH: Observe query state PAUSED
    System.out.println("Pause response " + pauseResponse);
    assertThat(getPausedCount(), equalTo(1));

    // Produce more records
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);

    // Give time to process
    Thread.sleep(2000);

    // JNH: Observe no further output somehow?
    // Observe no new records -- This kinda *doesn't work*.  Hard to prove a negative sometimes...
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    // Restatrt server
    System.out.println("JNH: Stopping server");
    //REST_APP.stop();
    REST_APP.stop();
    Thread.sleep(10000);
    System.out.println("JNH: Starting server");
    REST_APP.start();

    // Verify PAUSED state -- eventually
    AssertEventually.assertThatEventually(() -> getPausedCount(), equalTo(1));

    // Verify number of processed records
    AssertEventually.assertThatEventually(supplier, equalTo(9));

    System.out.println("JNH: Calling RESUME");

    // Then:
    // JNH: After resuming

    List <KsqlEntity> resumeResponse = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "RESUME "
        + queryId + ";");

    // Adding more after restart.
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER2, KAFKA, JSON,
        System::currentTimeMillis);
    // JNH: Observe more output!
    // 5 more records have been produced
    AssertEventually.assertThatEventually(supplier, equalTo(14));

    // JNH: Observe query state RUNNING
    List <KsqlEntity> showQueries3 = RestIntegrationTestUtil.makeKsqlRequest(REST_APP, "SHOW "
        + "QUERIES;");
    assertThat(((Queries) showQueries3.get(0)).getQueries().get(0).getStatusCount().getStatuses()
        .get(KsqlQueryStatus.RUNNING), equalTo(1));
  }

  // @Test
  public void shouldBeAbleToCreatePausedQuery() throws Exception {

  }

  // JNH Maybe just copy this file and create with multiple REST instances?
  // @Test
  public void pauseResumeShouldWorkWithMultipleServers() throws Exception {

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

  private void shouldReturn50303WhenTerminating() {
    // Given: TERMINATE CLUSTER has been issued

    // When:
    final KsqlErrorMessage error = RestIntegrationTestUtil.makeKsqlRequestWithError(REST_APP, "SHOW STREAMS;");

    // Then:
    assertThat(error.getErrorCode(), is(Errors.ERROR_CODE_SERVER_SHUTTING_DOWN));
  }

  private static void terminateCluster(final List<String> deleteTopicList) {

    HttpResponse<Buffer> resp = RestIntegrationTestUtil
        .rawRestRequest(REST_APP, HttpVersion.HTTP_1_1, HttpMethod.POST, "/ksql/terminate",
                        new ClusterTerminateRequest(deleteTopicList), Optional.empty());

    assertThat(resp.statusCode(), is(OK.code()));
  }

}
