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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.streams.KafkaClientSupplier;
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
import org.mockito.Mock;

import javax.ws.rs.core.Configurable;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.JAAS_KAFKA_PROPS_NAME;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Category({IntegrationTest.class})
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
      .withServiceContextBinder(config -> new AbstractBinder() {
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

  private static Set<String> allowedUsers = new HashSet<>();

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
    allowedUsers.clear();
    MockKsqlSecurityExtension.setAuthorizationProvider(authorizationProvider);
  }

  @Test
  public void shouldLoadSecurityExtension() {
    // Then:
    assertThat(MockKsqlSecurityExtension.initialized, is(true));
    assertThat(MockKsqlSecurityExtension.closed, is(false));
  }

  @Test
  public void shouldDenyAccess() {
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
    givenAuthorizedUser(USER1);

    // When:
    final List<KsqlEntity> results = makeKsqlRequest(USER1, "SHOW TOPICS;");

    // Then:
    final List<KafkaTopicInfo> topics = ((KafkaTopicsList)results.get(0)).getTopics();
    assertThat(topics.size(), is(1));
    assertThat(topics.get(0).getName(), is(TOPIC_1));
  }

  private void givenAuthorizedUser(final Credentials user) {
    allowedUsers.add(user.username);
  }

  private List<KsqlEntity> makeKsqlRequest(final Credentials credentials, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql, Optional.of(credentials));
  }

  /*
   * Mock the Security extension and authorization provider for all tests
   */
  public static class MockKsqlSecurityExtension implements KsqlSecurityExtension {
    public static boolean initialized = false;
    public static boolean closed = false;
    public static KsqlAuthorizationProvider provider;

    public static void setAuthorizationProvider(final KsqlAuthorizationProvider provider) {
      MockKsqlSecurityExtension.provider = provider;
    }

    @Override
    public void initialize(KsqlConfig ksqlConfig) {
      MockKsqlSecurityExtension.initialized = true;
    }

    @Override
    public Optional<KsqlAuthorizationProvider> getAuthorizationProvider() {
      return Optional.of((user, method, path) -> {
            if (!allowedUsers.contains(user.getName())) {
              throw new KsqlException(String.format("Access denied to User:%s", user));
            }
          }
      );
    }

    @Override
    public void register(Configurable<?> configurable) {

    }

    @Override
    public KafkaClientSupplier getKafkaClientSupplier(Principal principal) throws KsqlException {
      return null;
    }

    @Override
    public Supplier<SchemaRegistryClient> getSchemaRegistryClientSupplier(Principal principal) throws KsqlException {
      return null;
    }

    @Override
    public void close() {
      MockKsqlSecurityExtension.closed = true;
    }
  }

}
