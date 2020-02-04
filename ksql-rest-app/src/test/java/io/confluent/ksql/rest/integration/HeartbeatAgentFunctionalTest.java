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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlHostEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class HeartbeatAgentFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();

  private static final KsqlHostEntity host0 = new KsqlHostEntity("localhost",8088);
  private static final KsqlHostEntity host1 = new KsqlHostEntity("localhost",8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .withProperty(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
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
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, Format.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
  }

  @Before
  public void setup() {
    REST_APP_0.start();
    REST_APP_1.start();
  }

  @After
  public void tearDown() {
    REST_APP_0.stop();
    REST_APP_1.stop();
  }

  @Test(timeout = 60000)
  public void shouldMarkServersAsUp() {
    // Given:
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 2);
    HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsDown);

    // When:
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(REST_APP_0, host1, 3000);
    final ClusterStatusResponse clusterStatusResponseUp = HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp.getClusterStatus().get(host0).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp.getClusterStatus().get(host1).getHostAlive(), is(true));
  }

  @Test(timeout = 60000)
  public void shouldMarkRemoteServerAsDown() {
    // Given:
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 2);

    // When:
    ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsDown);

    // Then:
    assertThat(clusterStatusResponse.getClusterStatus().get(host0).getHostAlive(), is(true));
    assertThat(clusterStatusResponse.getClusterStatus().get(host1).getHostAlive(), is(false));
  }

  @Test(timeout = 60000)
  public void shouldMarkRemoteServerAsUpThenDownThenUp() {
    // Given:
    HighAvailabilityTestUtil.waitForClusterToBeDiscovered(REST_APP_0, 2);
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(REST_APP_0, host1, 3000);

    // When:
    final ClusterStatusResponse clusterStatusResponseUp1 =  HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp1.getClusterStatus().get(host0).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp1.getClusterStatus().get(host1).getHostAlive(), is(true));

    // When:
    ClusterStatusResponse clusterStatusResponseDown =  HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsDown);

    // Then:
    assertThat(clusterStatusResponseDown.getClusterStatus().get(host0).getHostAlive(), is(true));
    assertThat(clusterStatusResponseDown.getClusterStatus().get(host1).getHostAlive(), is(false));

    // When :
    HighAvailabilityTestUtil.sendHeartbeartsForWindowLength(REST_APP_0, host1, 3000);
    ClusterStatusResponse clusterStatusResponseUp2 = HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus(
        REST_APP_0, host1, HighAvailabilityTestUtil::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp2.getClusterStatus().get(host0).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp2.getClusterStatus().get(host1).getHostAlive(), is(true));
  }
}
