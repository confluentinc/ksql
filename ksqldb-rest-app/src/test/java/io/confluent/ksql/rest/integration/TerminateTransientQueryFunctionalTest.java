/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class TerminateTransientQueryFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8089);

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

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
  }

  @Test
  public void shouldTerminatePushQueryOnSingleNode() {
    // Given:
    ExecutorService service = Executors.newFixedThreadPool(2);
    Runnable backgroundTask = new Runnable() {
      @Override
      public void run() {
        RestIntegrationTestUtil.makeQueryRequest(
            REST_APP_0,
            "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES;",
            Optional.of(BasicCredentials.of("user", "pwd"))
        );
      }
    };
    service.execute(backgroundTask);

    int numQueries = 0;
    while(numQueries != 1) {
      final List<KsqlEntity> results = showQueries();
      final Queries result = (Queries) results.get(0);
      numQueries = result.getQueries().size();
    }

    final List<KsqlEntity> results = showQueries();
    final Queries result = (Queries) results.get(0);
    final QueryId queryId = result.getQueries().get(0).getId();

    // When:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "terminate " + queryId.toString() + ";"
    );

    final List<KsqlEntity> finalResults = showQueries();
    final Queries finalResult = (Queries) finalResults.get(0);
    numQueries = finalResult.getQueries().size();

    // Then:
    assertThat(
        "Should terminate push query using query id",
        numQueries,
        is(0)
    );
  }

  @Test
  public void shouldTerminatePushQueryOnAnotherNode() throws InterruptedException {
    // Given:
    ExecutorService service = Executors.newFixedThreadPool(2);
    Runnable backgroundTask = new Runnable() {
      @Override
      public void run() {
        System.out.println("FOOOO");
        RestIntegrationTestUtil.makeQueryRequest(
            REST_APP_1,
            "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES;",
            Optional.of(BasicCredentials.of("user", "pwd"))
        );

      }
    };
    service.submit(backgroundTask);

    int numQueries = 0;
    while(numQueries != 1) {
      final List<KsqlEntity> results = showQueries();
      final Queries result = (Queries) results.get(0);
      numQueries = result.getQueries().size();
      Thread.sleep(100);
    }

    final List<KsqlEntity> results = showQueries();
    final Queries result = (Queries) results.get(0);
    final QueryId queryId = result.getQueries().get(0).getId();

    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "terminate " + queryId.toString() + ";"
    );

    final List<KsqlEntity> finalResults = showQueries();
    final Queries finalResult = (Queries) finalResults.get(0);
    numQueries = finalResult.getQueries().size();

    assertThat(
        "Should terminate push query on another node using query id",
        numQueries,
        is(0)
    );
  }

  public  List<KsqlEntity> showQueries (){
    return RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_1,
        "show queries;"
    );
  }
}