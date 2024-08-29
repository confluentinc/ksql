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

import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForClusterToBeDiscovered;
import static io.confluent.ksql.rest.integration.HighAvailabilityTestUtil.waitForRemoteServerToChangeStatus;
import static io.confluent.ksql.rest.server.utils.TestUtils.findFreeLocalPort;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.JAAS_KAFKA_PROPS_NAME;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.security.BasicCredentials;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.test.util.secure.MultiNodeKeyStore;
import io.confluent.ksql.test.util.secure.MultiNodeTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.vertx.core.net.SocketAddress;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@Category({IntegrationTest.class})
@RunWith(Enclosed.class)
public class SystemAuthenticationFunctionalTest {
  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();
  @ClassRule
  public static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();
  private static PageViewDataProvider PAGE_VIEWS_PROVIDER;
  private static String PAGE_VIEW_TOPIC;
  private static final BiFunction<Integer, String, SocketAddress> LOCALHOST_FACTORY =
      (port, host) -> SocketAddress.inetSocketAddress(port, "localhost");
  private static String PAGE_VIEW_STREAM;

  private static Map<String, Object> JASS_AUTH_CONFIG;
  private static Map<String, Object> COMMON_CONFIG;

  @BeforeClass
  public static void setupClass() {
    PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
    PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
    PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

    JASS_AUTH_CONFIG = ImmutableMap.<String, Object>builder()
        .put("authentication.method", "BASIC")
        .put("authentication.roles", "**")
        // Reuse the Kafka JAAS config for KSQL authentication which has the same valid users
        .put("authentication.realm", JAAS_KAFKA_PROPS_NAME)
        .put(
            KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS,
            MockKsqlSecurityExtension.class.getName()
        )
        .build();

    COMMON_CONFIG = ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
        .put(KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG, 1000)
        .put(KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG, 3000)
        .put(KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG, 3000)
        .put(KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG, getNewStateDir(TMP))
        .put(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1)
        .put(KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG, 1000)
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "system-auth-functional-test")
        .putAll(SERVER_KEY_STORE.keyStoreProps())
        .build();
  }

  private static Map<String, String> internalKeyStoreProps(boolean node1) {
    Map<String, String> keyStoreProps = MultiNodeKeyStore.keyStoreProps();
    Map<String, String> trustStoreProps = MultiNodeTrustStore.trustStoreNode1Node2Props();

    return ImmutableMap.<String, String>builder()
        .put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            keyStoreProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
        .put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            keyStoreProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
        .put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            keyStoreProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG))
        .put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            trustStoreProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
        .put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            trustStoreProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).build();
  }

  private static final BasicCredentials USER1 = BasicCredentials.of(
      VALID_USER1.username,
      VALID_USER1.password
  );

  private static void commonClassSetup(final IntegrationTestHarness TEST_HARNESS,
      final TestKsqlRestApp REST_APP_0) {
    TEST_HARNESS.ensureTopics(2, PAGE_VIEW_TOPIC);
    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER, Optional.of(USER1));
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP_0,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";",
        Optional.of(USER1)
    );
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class MutualAuth {
    private static int INT_PORT_0 = findFreeLocalPort();
    private static int INT_PORT_1 = findFreeLocalPort();
    private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("node-1.example.com",
        INT_PORT_0);
    private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("node-2.example.com",
        INT_PORT_1);
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(LOCALHOST_FACTORY)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:0")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://node-1.example.com:" + INT_PORT_0)
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:" + INT_PORT_0)
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED)
        .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG, "node-1.example.com")
        .withProperties(COMMON_CONFIG)
        .withProperties(JASS_AUTH_CONFIG)
        .withProperties(internalKeyStoreProps(true))
        .build();

    private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(LOCALHOST_FACTORY)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:0")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://node-2.example.com:" + INT_PORT_1)
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:" + INT_PORT_1)
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED)
        .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG, "node-2.example.com")
        .withProperties(COMMON_CONFIG)
        .withProperties(JASS_AUTH_CONFIG)
        .withProperties(internalKeyStoreProps(false))
        .build();

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
        .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
        .around(TEST_HARNESS)
        .around(REST_APP_0)
        .around(REST_APP_1);

    @Mock
    private KsqlAuthorizationProvider authorizationProvider;

    @BeforeClass
    public static void setUpClass() {
      KsqlAuthorizationProvider staticAuthorizationProvider =
          Mockito.mock(KsqlAuthorizationProvider.class);
      MockKsqlSecurityExtension.setAuthorizationProvider(staticAuthorizationProvider);
      allowAccess(staticAuthorizationProvider, USER1, "POST", "/ksql");
      commonClassSetup(TEST_HARNESS, REST_APP_0);
    }

    @Before
    public void setUp() {
      MockKsqlSecurityExtension.setAuthorizationProvider(authorizationProvider);
    }


    @Test(timeout = 60000)
    public void shouldHeartbeatSuccessfully() throws InterruptedException {
      // Given:
      allowAccess(authorizationProvider, USER1, "GET", "/clusterStatus");
      waitForClusterToBeDiscovered(REST_APP_0, 2, Optional.of(USER1));

      // This ensures that we can't hit the initial optimistic alive status
      Thread.sleep(6000);

      // When:
      // wait for a single response that shows both are alive, this is necessary
      // because if we wait for them separately the test can be flaky as one might
      // go down while we wait for the other
      final ClusterStatusResponse response = waitForRemoteServerToChangeStatus(
          REST_APP_0,
          clusterStatus -> clusterStatus.get(host0).getHostAlive()
              && clusterStatus.get(host1).getHostAlive(),
          Optional.of(USER1)
      );

      // Then:
      assertThat( response.getClusterStatus().get(host0).getHostAlive(), is(true));
      assertThat( response.getClusterStatus().get(host1).getHostAlive(), is(true));
      verify(authorizationProvider, never())
          .checkEndpointAccess(argThat(new PrincipalMatcher(USER1)), any(),
              not(eq("/clusterStatus")));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class HttpsNoMutualAuth {
    private static int INT_PORT_0 = findFreeLocalPort();
    private static int INT_PORT_1 = findFreeLocalPort();
    private static final KsqlHostInfoEntity host0 = new KsqlHostInfoEntity("node-1.example.com",
        INT_PORT_0);
    private static final KsqlHostInfoEntity host1 = new KsqlHostInfoEntity("node-2.example.com",
        INT_PORT_1);
    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(LOCALHOST_FACTORY)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:0")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://node-1.example.com:" + INT_PORT_0)
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:" + INT_PORT_0)
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE)
        .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG, "node-1.example.com")
        .withProperty(KsqlRestConfig.KSQL_SERVER_SNI_CHECK_ENABLE, true)
        .withProperties(COMMON_CONFIG)
        .withProperties(internalKeyStoreProps(true))
        .build();

    private static final TestKsqlRestApp REST_APP_1 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withEnabledKsqlClient(LOCALHOST_FACTORY)
        .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://0.0.0.0:0")
        .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG,
            "https://node-2.example.com:" + INT_PORT_1)
        .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://0.0.0.0:" + INT_PORT_1)
        .withProperty(KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
            KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE)
        .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG, "node-2.example.com")
        .withProperty(KsqlRestConfig.KSQL_SERVER_SNI_CHECK_ENABLE, true)
        .withProperties(COMMON_CONFIG)
        .withProperties(internalKeyStoreProps(false))
        .build();

    @ClassRule
    public static final RuleChain CHAIN = RuleChain
        .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
        .around(TEST_HARNESS)
        .around(REST_APP_0)
        .around(REST_APP_1);

    @Mock
    private KsqlAuthorizationProvider authorizationProvider;

    @BeforeClass
    public static void setUpClass() {
      commonClassSetup(TEST_HARNESS, REST_APP_0);
    }

    @Test(timeout = 60000)
    public void shouldHeartbeatSuccessfully() throws InterruptedException {
      // Given:
      waitForClusterToBeDiscovered(REST_APP_0, 2);

      // This ensures that we can't hit the initial optimistic alive status
      Thread.sleep(6000);

      // When:
      // wait for a single response that shows both are alive, this is necessary
      // because if we wait for them separately the test can be flaky as one might
      // go down while we wait for the other
      final ClusterStatusResponse response = waitForRemoteServerToChangeStatus(
          REST_APP_0,
          clusterStatus -> clusterStatus.get(host0).getHostAlive()
              && clusterStatus.get(host1).getHostAlive(),
          Optional.empty()
      );

      // Then:
      assertThat( response.getClusterStatus().get(host0).getHostAlive(), is(true));
      assertThat( response.getClusterStatus().get(host1).getHostAlive(), is(true));
    }
  }

  private static void allowAccess(final KsqlAuthorizationProvider ap,
      final BasicCredentials user, final String method, final String path) {
    doNothing().when(ap)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private static class PrincipalMatcher implements ArgumentMatcher<Principal> {

    private final BasicCredentials user;

    PrincipalMatcher(final BasicCredentials user) {
      this.user = user;
    }

    @Override
    public boolean matches(final Principal principal) {
      return this.user.username().equals(principal.getName());
    }
  }

  public static String getNewStateDir(TemporaryFolder rootTempDir) {
    try {
      return rootTempDir.newFolder().getAbsolutePath();
    } catch (final IOException e) {
      throw new AssertionError("Failed to create new state dir", e);
    }
  }
}
