/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

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
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ListAllQueriesFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
          .builder(TEST_HARNESS::kafkaBootstrapServers)
          .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
          .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
          .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
          .withProperty(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
          .withProperties(ClientTrustStore.trustStoreProps())
          .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
          .builder(TEST_HARNESS::kafkaBootstrapServers)
          .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
          .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
          .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
          .withProperty(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
          .withProperties(ClientTrustStore.trustStoreProps())
          .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP_0)
      .around(REST_APP_1);

  @BeforeClass
  public static void setUpClass() throws InterruptedException {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
    waitForClusterToBeDiscovered(REST_APP_0, 2);
  }

  @Test
  public void shouldShowAllQueries() {
    // Given:
    final List<KsqlEntity> entities0 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show Queries;"
    );
    final List<RunningQuery> runningQueries0 = ((Queries) entities0.get(0)).getQueries();
    assertThat(runningQueries0.size(), equalTo(1));
    assertThat(runningQueries0.get(0).getKsqlHostInfo().orElse(null), equalTo(host0));


    final List<KsqlEntity> entities1 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_1,
        "Show Queries;"
    );
    final List<RunningQuery> runningQueries1 = ((Queries) entities1.get(0)).getQueries();
    assertThat(runningQueries1.size(), equalTo(1));
    assertThat(runningQueries1.get(0).getKsqlHostInfo().orElse(null), equalTo(host1));

    // Then:
    final List<KsqlEntity> allEntities = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show All Queries;"
    );

    final List<RunningQuery> allRunningQueries = ((Queries) allEntities.get(0)).getQueries();
    assertThat(allRunningQueries.size(), equalTo(2));
    assertThat(allRunningQueries.stream()
        .map(RunningQuery::getKsqlHostInfo)
        .map(host -> host.orElse(null))
        .collect(Collectors.toSet()), containsInAnyOrder(host0, host1));
  }

  @Test
  public void shouldShowAllQueriesExtended() {
    // Given:
    final List<KsqlEntity> entities0 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show Queries Extended;"
    );
    final List<QueryDescription> queryDescriptions0 = ((QueryDescriptionList) entities0.get(0)).getQueryDescriptions();
    assertThat(queryDescriptions0.size(), equalTo(1));
    assertThat(queryDescriptions0.get(0).getKsqlHostInfo().orElse(null), equalTo(host0));
    
    final List<KsqlEntity> entities1 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_1,
        "Show Queries Extended;"
    );
    final List<QueryDescription> queryDescriptions1 = ((QueryDescriptionList) entities1.get(0)).getQueryDescriptions();
    assertThat(queryDescriptions1.size(), equalTo(1));
    assertThat(queryDescriptions1.get(0).getKsqlHostInfo().orElse(null), equalTo(host1));

    // Then:
    final List<KsqlEntity> clusterEntities = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show All Queries Extended;");

    final List<QueryDescription> clusterQueryDescriptions = ((QueryDescriptionList) clusterEntities.get(0)).getQueryDescriptions();
    assertThat(clusterQueryDescriptions.size(), equalTo(2));
    assertThat(clusterQueryDescriptions.stream()
        .map(QueryDescription::getKsqlHostInfo)
        .map(host -> host.orElse(null))
        .collect(Collectors.toSet()), containsInAnyOrder(host0, host1));
  }
}