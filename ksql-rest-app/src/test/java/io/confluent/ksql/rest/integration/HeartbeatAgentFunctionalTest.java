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
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.state.HostInfo;
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

  private static final HostInfo host0 = new HostInfo("localhost",8088);
  private static final HostInfo host1 = new HostInfo("localhost",8089);
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();
  private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8089")
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 600000)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 200)
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 2000)
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
    waitForClusterToBeDiscovered();
    waitForRemoteServerToChangeStatus(this::remoteServerIsDown);

    // When:
    sendHeartbeartsEveryIntervalForWindowLength(100, 3000);
    final ClusterStatusResponse clusterStatusResponseUp = waitForRemoteServerToChangeStatus(
        this::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp.getClusterStatus().get(host0.toString()).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp.getClusterStatus().get(host1.toString()).getHostAlive(), is(true));
  }

  @Test(timeout = 60000)
  public void shouldMarkRemoteServerAsDown() {
    // Given:
    waitForClusterToBeDiscovered();

    // When:
    ClusterStatusResponse clusterStatusResponse = waitForRemoteServerToChangeStatus(
        this::remoteServerIsDown);

    // Then:
    assertThat(clusterStatusResponse.getClusterStatus().get(host0.toString()).getHostAlive(), is(true));
    assertThat(clusterStatusResponse.getClusterStatus().get(host1.toString()).getHostAlive(), is(false));
  }

  @Test(timeout = 60000)
  public void shouldMarkRemoteServerAsUpThenDownThenUp() {
    // Given:
    waitForClusterToBeDiscovered();
    sendHeartbeartsEveryIntervalForWindowLength(100, 2000);

    // When:
    final ClusterStatusResponse clusterStatusResponseUp1 = waitForRemoteServerToChangeStatus(
        this::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp1.getClusterStatus().get(host0.toString()).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp1.getClusterStatus().get(host1.toString()).getHostAlive(), is(true));

    // When:
    ClusterStatusResponse clusterStatusResponseDown = waitForRemoteServerToChangeStatus(
        this::remoteServerIsDown);

    // Then:
    assertThat(clusterStatusResponseDown.getClusterStatus().get(host0.toString()).getHostAlive(), is(true));
    assertThat(clusterStatusResponseDown.getClusterStatus().get(host1.toString()).getHostAlive(), is(false));

    // When :
    sendHeartbeartsEveryIntervalForWindowLength(100, 2000);
    ClusterStatusResponse clusterStatusResponseUp2 = waitForRemoteServerToChangeStatus(
        this::remoteServerIsUp);

    // Then:
    assertThat(clusterStatusResponseUp2.getClusterStatus().get(host0.toString()).getHostAlive(), is(true));
    assertThat(clusterStatusResponseUp2.getClusterStatus().get(host1.toString()).getHostAlive(), is(true));
  }

  private void waitForClusterToBeDiscovered() {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(REST_APP_0);
      if(allServersDiscovered(clusterStatusResponse.getClusterStatus())) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  private boolean allServersDiscovered(Map<String, HostStatusEntity> clusterStatus) {
    if(clusterStatus.size() < 2) {
      return false;
    }
    return true;
  }

  private void sendHeartbeartsEveryIntervalForWindowLength(long interval, long window) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < window) {
      sendHeartbeatRequest(REST_APP_0, host1, System.currentTimeMillis());
      try {
        Thread.sleep(interval);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  private ClusterStatusResponse waitForRemoteServerToChangeStatus(
      Function<Map<String, HostStatusEntity>, Boolean> function)
  {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(REST_APP_0);
      if(function.apply(clusterStatusResponse.getClusterStatus())) {
        return clusterStatusResponse;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  private boolean remoteServerIsDown(Map<String, HostStatusEntity> clusterStatus) {
    if (!clusterStatus.containsKey(host1.toString())) {
      return true;
    }
    for( Entry<String, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().contains("8089") && !entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  private boolean remoteServerIsUp(Map<String, HostStatusEntity> clusterStatus) {
    for( Entry<String, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().contains("8089") && entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  private static void sendHeartbeatRequest(
      final TestKsqlRestApp restApp,
      final HostInfo host,
      final long timestamp) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {
      restClient.makeAsyncHeartbeatRequest(new HostInfoEntity(host.host(), host.port()), timestamp);
    }
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
}
