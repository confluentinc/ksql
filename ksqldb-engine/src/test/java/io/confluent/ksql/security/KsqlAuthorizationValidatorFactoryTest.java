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

package io.confluent.ksql.security;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAuthorizationValidatorFactoryTest {
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;
  @Mock
  private KsqlAuthorizationProvider authorizationProvider;

  private Node node;

  @Before
  public void setUp() {
    node = new Node(1, "host", 9092);

    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_AUTO);
  }

  @Test
  public void shouldReturnProvidedAuthorizationValidatorWhenAuthorizationProviderIsNonEmpty() {
    // Given:
    givenKafkaAuthorizer("", Collections.emptySet());

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.of(authorizationProvider)
    );

    // Then
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlProvidedAccessValidator.class)));
  }

  @Test
  public void shouldReturnBackendAuthorizationValidatorWhenKafkaAuthorizerIsSet() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", Collections.emptySet());

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlBackendAccessValidator.class)));
  }
  
  @Test
  public void shouldChooseProvidedAuthorizationValidatorOverKafkaBackendValidator() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", Collections.emptySet());

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.of(authorizationProvider)
    );

    // Then
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlProvidedAccessValidator.class)));
  }

  @Test
  public void shouldReturnEmptyAuthorizationValidatorWhenNoAuthorizationProviderIsFound() {
    // Given:
    givenKafkaAuthorizer("", Collections.emptySet());

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then
    assertThat(validator, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyAuthorizationValidatorKafkaAuthorizerIsSetButNotEnabled() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", Collections.emptySet());
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then:
    assertThat(validator, is(Optional.empty()));
    verifyNoMoreInteractions(adminClient);
  }

  @Test
  public void shouldReturnAuthorizationValidatorIfEnabled() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_ON);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS))
        .thenReturn(0L);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then:
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlBackendAccessValidator.class)));
    verifyNoMoreInteractions(adminClient);
  }

  @Test
  public void shouldNotReturnProvidedAuthorizationValidatorIfAccessValidatorFlagIsOff() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.of(authorizationProvider)
    );

    // Then:
    assertThat(validator, is(Optional.empty()));
  }

  @Test
  public void shouldReturnProvidedAuthorizationValidatorWhenCacheIsEnabled() {
    // Given:
    givenKafkaAuthorizer("", Collections.emptySet());
    when(ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS)).thenReturn(1L);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.of(authorizationProvider)
    );

    // Then:
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlCacheAccessValidator.class)));
    verifyNoMoreInteractions(adminClient);
  }

  @Test
  public void shouldReturnAuthorizationValidatorWhenCacheIsEnabled() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_ON);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS))
        .thenReturn(1L);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then:
    assertThat("validator should be present", validator.isPresent());
    assertThat(validator.get(), is(instanceOf(KsqlAuthorizationValidatorImpl.class)));
    assertThat(((KsqlAuthorizationValidatorImpl)validator.get()).getAccessValidator(),
        is(instanceOf(KsqlCacheAccessValidator.class)));
    verifyNoMoreInteractions(adminClient);
  }

  @Test
  public void shouldReturnEmptyValidatorIfAuthorizedOperationsReturnNull() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", null);

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then
    assertThat(validator, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyValidatorIfKafkaBrokerVersionTooLowButAuthorizerClassConfigIsSet() {
    // Given:
    givenSingleNode();
    givenAuthorizerClass("a-class");
    when(adminClient.describeCluster(any())).thenThrow(new UnsupportedVersionException("too old"));

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then
    assertThat(validator, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyValidatorIfKafkaBrokerVersionTooLowAndExceptionWrapped()
      throws InterruptedException, ExecutionException {
    // Given:
    givenSingleNode();
    givenAuthorizerClass("a-class");
    final KafkaFuture<Set<AclOperation>> authorized = mockAuthorizedOperationsFuture();
    final DescribeClusterResult result = mock(DescribeClusterResult.class);
    when(adminClient.describeCluster(any())).thenReturn(result);
    when(result.authorizedOperations()).thenReturn(authorized);
    when(authorized.get())
        .thenThrow(new ExecutionException(new UnsupportedVersionException("too old")));

    // When:
    final Optional<KsqlAuthorizationValidator> validator = KsqlAuthorizationValidatorFactory.create(
        ksqlConfig,
        serviceContext,
        Optional.empty()
    );

    // Then
    assertThat(validator, is(Optional.empty()));
  }

  private void givenKafkaAuthorizer(
      final String className,
      final Set<AclOperation> authOperations
  ) {
    final DescribeClusterResult describeClusterResult = describeClusterResult(authOperations);
    when(adminClient.describeCluster()).thenReturn(describeClusterResult);
    when(adminClient.describeCluster(any()))
        .thenReturn(describeClusterResult);
    final DescribeConfigsResult describeConfigsResult = describeBrokerResult(
        Collections.singletonList(
            new ConfigEntry(KAFKA_AUTHORIZER_CLASS_NAME, className)
        )
    );
    when(adminClient.describeConfigs(describeBrokerRequest()))
        .thenReturn(describeConfigsResult);
  }

  private DescribeClusterResult describeClusterResult(final Set<AclOperation> authOperations) {
    final Collection<Node> nodes = Collections.singletonList(node);
    final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
    when(describeClusterResult.authorizedOperations())
        .thenReturn(KafkaFuture.completedFuture(authOperations));
    return describeClusterResult;
  }

  private Collection<ConfigResource> describeBrokerRequest() {
    return Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
  }

  private DescribeConfigsResult describeBrokerResult(final List<ConfigEntry> brokerConfigs) {
    final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    final Map<ConfigResource, Config> config = ImmutableMap.of(
        new ConfigResource(ConfigResource.Type.BROKER, node.idString()), new Config(brokerConfigs));
    when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(config));
    return describeConfigsResult;
  }

  private void givenSingleNode() {
    final Collection<Node> nodes = Collections.singletonList(node);
    final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
    when(adminClient.describeCluster()).thenReturn(describeClusterResult);
  }

  private void givenAuthorizerClass(final String name) {
    final DescribeConfigsResult describeConfigsResult = describeBrokerResult(
        Collections.singletonList(
            new ConfigEntry(KAFKA_AUTHORIZER_CLASS_NAME, name)
        )
    );
    when(adminClient.describeConfigs(describeBrokerRequest()))
        .thenReturn(describeConfigsResult);
  }

  @SuppressWarnings("unchecked")
  private KafkaFuture<Set<AclOperation>> mockAuthorizedOperationsFuture() {
    return mock(KafkaFuture.class);
  }
}
