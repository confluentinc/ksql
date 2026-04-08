/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.JAAS_KAFKA_PROPS_NAME;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.security.BasicCredentials;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.errors.AuthorizationException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class AuthorizationFunctionalTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final BasicCredentials USER1 = BasicCredentials.of(
      VALID_USER1.username,
      VALID_USER1.password
  );

  private static final String TOPIC_1 = "topic_1";
  private static final int INT_PORT = TestUtils.findFreeLocalPort();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("authentication.method", "BASIC")
      .withProperty("authentication.roles", "**")
      // Reuse the Kafka JAAS config for KSQL authentication which has the same valid users
      .withProperty("authentication.realm", JAAS_KAFKA_PROPS_NAME)
      .withProperty(
          KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS,
          MockKsqlSecurityExtension.class.getName()
      )
      .withProperty(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG, true)
      .withProperty(KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "http://localhost:" + INT_PORT)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  KsqlHostInfoEntity HOST_ENTITY = new KsqlHostInfoEntity("host", 9000);

  @Mock
  private KsqlAuthorizationProvider authorizationProvider;

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(TOPIC_1);
  }

  @Before
  public void setUp() {
    MockKsqlSecurityExtension.setAuthorizationProvider(authorizationProvider);
  }

  @Test
  public void shouldDenyAccess() {
    // Given:
    denyAccess(USER1, "POST", "/ksql");

    // When
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> makeKsqlRequest(USER1, "SHOW TOPICS;")
    );

  // Then:
    assertThat(e.getMessage(), containsString(format("Access denied to User:%s", USER1.username())));
  }

  @Test
  public void shouldAllowAccess() {
    // Given:
    allowAccess(USER1, "POST", "/ksql");

    // When:
    final List<KsqlEntity> results = makeKsqlRequest(USER1, "SHOW TOPICS;");

    // Then:
    final List<KafkaTopicInfo> topics = ((KafkaTopicsList)results.get(0)).getTopics();
    assertThat(topics.size(), is(1));
    assertThat(topics.get(0).getName(), is(TOPIC_1));
  }

  @Test
  public void shouldDenyAccess_internal() {
    // Given:
    denyAccess(USER1, "POST", "/heartbeat");

    // When
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> makeHeartbeat(USER1)
    );

    // Then:
    assertThat(e.getMessage(), containsString(format("Access denied to User:%s", USER1.username())));
  }

  @Test
  public void shouldAllowAccess_internal() {
    // Given:
    allowAccess(USER1, "POST", "/heartbeat");

    // When:
    final HeartbeatResponse response = makeHeartbeat(USER1);

    // Then:
    assertThat(response.getIsOk(), is(true));
  }

  @Test
  public void shouldDenyAccess_internalPathNormaListener() {
    // When
    final AssertionError e = assertThrows(
        AssertionError.class,
        () -> makeHeartbeatNormalListener(USER1)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Can't call internal endpoint on public listener"));
  }

  private void allowAccess(final BasicCredentials user, final String method, final String path) {
    doNothing().when(authorizationProvider)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private void denyAccess(final BasicCredentials user, final String method, final String path) {
    doThrow(new AuthorizationException(String.format("Access denied to User:%s", user.username())))
        .when(authorizationProvider)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private List<KsqlEntity> makeKsqlRequest(final BasicCredentials credentials, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql, Optional.of(credentials));
  }

  private HeartbeatResponse makeHeartbeat(final BasicCredentials credentials) {
    return HighAvailabilityTestUtil.sendHeartbeatRequest(REST_APP,
        HOST_ENTITY, 1000, Optional.of(credentials));
  }

  private HeartbeatResponse makeHeartbeatNormalListener(final BasicCredentials credentials) {
    return HighAvailabilityTestUtil.sendHeartbeatRequestNormalListener(REST_APP,
        HOST_ENTITY, 1000, Optional.of(credentials));
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
}
