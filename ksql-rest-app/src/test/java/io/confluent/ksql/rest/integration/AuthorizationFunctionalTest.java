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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.errors.AuthorizationException;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class AuthorizationFunctionalTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final Credentials USER1 = VALID_USER1;

  private static final String TOPIC_1 = "topic_1";

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
      .withServiceContextBinder((config, extension) -> new AbstractBinder() {
        @Override
        protected void configure() {
          bindFactory(new Factory<ServiceContext>() {
            @Override
            public ServiceContext provide() {
              return TEST_HARNESS.getServiceContext();
            }

            @Override
            public void dispose(final ServiceContext serviceContext) {
              // do nothing because TEST_HARNESS#getServiceContext always
              // returns the same instance
            }
          })
              .to(ServiceContext.class)
              .in(RequestScoped.class);
        }
      })
      .build();

  @Mock
  private KsqlAuthorizationProvider authorizationProvider;

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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

    // Then:
    expectedException.expect(AssertionError.class);
    expectedException.expectMessage(
        String.format("Access denied to User:%s", USER1.username)
    );

    // When:
    makeKsqlRequest(USER1, "SHOW TOPICS;");
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

  private void allowAccess(final Credentials user, final String method, final String path) {
    doNothing().when(authorizationProvider)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private void denyAccess(final Credentials user, final String method, final String path) {
    doThrow(new AuthorizationException(String.format("Access denied to User:%s", user.username)))
        .when(authorizationProvider)
        .checkEndpointAccess(argThat(new PrincipalMatcher(user)), eq(method), eq(path));
  }

  private List<KsqlEntity> makeKsqlRequest(final Credentials credentials, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql, Optional.of(credentials));
  }

  private static class PrincipalMatcher implements ArgumentMatcher<Principal> {
    private final Credentials user;

    PrincipalMatcher(final Credentials user) {
      this.user = user;
    }

    @Override
    public boolean matches(final Principal principal) {
      return this.user.username.equals(principal.getName());
    }
  }
}
