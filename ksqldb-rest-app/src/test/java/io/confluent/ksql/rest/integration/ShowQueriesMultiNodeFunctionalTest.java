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

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
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
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.util.PageViewDataProvider;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();
  private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("localhost", 8088);
  private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("localhost", 8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
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
    final List<KsqlEntity> allEntities0 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show Queries;"
    );

    final List<RunningQuery> allRunningQueries0 = ((Queries) allEntities0.get(0)).getQueries();
    assertThat(allRunningQueries0.size(), equalTo(1));
    assertThat(allRunningQueries0.get(0).getState().toString(), is("RUNNING:2"));

    final List<KsqlEntity> allEntities1 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_1,
        "Show Queries;"
    );

    final List<RunningQuery> allRunningQueries1 = ((Queries) allEntities1.get(0)).getQueries();
    assertThat(allRunningQueries1.size(), equalTo(1));
    assertThat(allRunningQueries0.get(0).getState().toString(), is("RUNNING:2"));
  }

  @Test
  public void shouldShowAllQueriesExtended() {
    final List<KsqlEntity> clusterEntities0 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show Queries Extended;");

    final List<QueryDescription> clusterQueryDescriptions0 = ((QueryDescriptionList) clusterEntities0.get(0)).getQueryDescriptions();
    assertThat(clusterQueryDescriptions0.size(), equalTo(1));
    assertThat(clusterQueryDescriptions0.get(0).getKsqlHostQueryState().keySet(), containsInAnyOrder(host0, host1));

    final List<KsqlEntity> clusterEntities1 = RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "Show Queries Extended;");

    final List<QueryDescription> clusterQueryDescriptions1 = ((QueryDescriptionList) clusterEntities1.get(0)).getQueryDescriptions();
    assertThat(clusterQueryDescriptions1.size(), equalTo(1));
    assertThat(clusterQueryDescriptions0.get(0).getKsqlHostQueryState().keySet(), containsInAnyOrder(host0, host1));
  }
}