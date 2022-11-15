/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ShowQueriesMultiNodeFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
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
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
  }

  @Test
  public void shouldShowAllQueries() {
    // When:
    final Supplier<String> app0Response = () -> getShowQueriesResult(REST_APP_0);
    final Supplier<String> app1Response = () -> getShowQueriesResult(REST_APP_1);

    // Then:
    assertThatEventually("App0", app0Response, is("RUNNING:2"));
    assertThatEventually("App1", app1Response, is("RUNNING:2"));
  }

  private static String getShowQueriesResult(final TestKsqlRestApp restApp) {
    final List<KsqlEntity> results = RestIntegrationTestUtil.makeKsqlRequest(
        restApp,
        "Show Queries;"
    );

    if (results.size() != 1) {
      return "Expected 1 response, got " + results.size();
    }

    final KsqlEntity result = results.get(0);

    if (!(result instanceof Queries)) {
      return "Expected Queries, got " + result;
    }

    final List<RunningQuery> runningQueries = ((Queries) result)
        .getQueries();

    if (runningQueries.size() != 1) {
      return "Expected 1 running query, got " + runningQueries.size();
    }

    return runningQueries.get(0).getStatusCount().toString();
  }

  @Test
  public void shouldShowAllQueriesExtended() {
    // When:
    final Supplier<Set<KsqlHostInfoEntity>> app0Response = () -> getShowQueriesExtendedResult(REST_APP_0);
    final Supplier<Set<KsqlHostInfoEntity>> app1Response = () -> getShowQueriesExtendedResult(REST_APP_1);

    // Then:
    assertThatEventually("App0", app0Response, containsInAnyOrder(host0, host1));
    assertThatEventually("App1", app1Response, containsInAnyOrder(host0, host1));
  }

  private static Set<KsqlHostInfoEntity> getShowQueriesExtendedResult(final TestKsqlRestApp restApp) {
    final List<KsqlEntity> results = RestIntegrationTestUtil.makeKsqlRequest(
        restApp,
        "Show Queries Extended;"
    );

    if (results.size() != 1) {
      return Collections.emptySet();
    }

    final KsqlEntity result = results.get(0);

    if (!(result instanceof QueryDescriptionList)) {
      return Collections.emptySet();
    }

    final List<QueryDescription> queryDescriptions = ((QueryDescriptionList) result)
        .getQueryDescriptions();

    if (queryDescriptions.size() != 1) {
      return Collections.emptySet();
    }
    
    return queryDescriptions.get(0).getKsqlHostQueryStatus().keySet();
  }
}