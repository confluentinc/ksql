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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class TerminateTransientQueryFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @Rule
  public final Timeout timeout = Timeout.seconds(60);

  private ExecutorService service;
  private Runnable backgroundTask;
  private boolean requestCompleted = false;

  @Before
  public void setUp() {
    service = Executors.newFixedThreadPool(1);
    backgroundTask = () -> {
      RestIntegrationTestUtil.makeQueryRequest(
          REST_APP_0,
          "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES;",
          Optional.empty());
      requestCompleted = true;
    };
  }

  @After
  public void tearDown() {
    service.shutdownNow();
  }

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Test
  public void shouldTerminatePushQueryOnSameNode() throws InterruptedException {
    // Given:
    givenPushQuery();
    final String transientQueryId = getTransientQueryIds().get(0);

    // When:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "terminate " + transientQueryId + ";"
    );

    // Then:
    assertThat(
        "Should terminate push query on same node using query id",
        !checkForTransientQuery()
    );
    assertThatEventually(() -> requestCompleted, is(true));
  }

  @Test
  public void shouldTerminatePushQueryOnAnotherNode() throws InterruptedException {
    // Given:
    givenPushQuery();
    final String transientQueryId = getTransientQueryIds().get(0);

    // When:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_1,
        "terminate " + transientQueryId + ";"
    );

    // Then:
    assertThat(
        "Should terminate push query on another node using query id",
        !checkForTransientQuery()
    );
    assertThatEventually(() -> requestCompleted, is(true));
  }

  public List<RunningQuery> showQueries (){
    return ((Queries) RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "show queries;"
    ).get(0)).getQueries();
  }

  public boolean checkForTransientQuery (){
    List<RunningQuery> queries = showQueries();
    return queries.stream()
        .anyMatch(q -> q.getId().toString().contains("transient"));
  }

  public List<String> getTransientQueryIds () {
    return showQueries().stream()
        .filter(q -> q.getId().toString().contains("transient"))
        .map(q -> q.getId().toString())
        .collect(Collectors.toList());
  }

  public void givenPushQuery() throws InterruptedException {
    service.execute(backgroundTask);

    boolean repeat = true;
    while (repeat){
      repeat = !checkForTransientQuery();
      Thread.sleep(1000L);
    }
  }
}