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

package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TopicAccessValidatorFactoryTest {
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;

  private Node node;

  @Rule
  final public MockitoRule mockitoJUnit = MockitoJUnit.rule();

  @Before
  public void setUp() {
    node = new Node(1, "host", 9092);

    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_AUTO);
  }

  @Test
  public void shouldReturnAuthorizationValidator() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", Collections.emptySet());

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(
        ksqlConfig,
        serviceContext
    );

    // Then
    assertThat(validator, is(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  @Test
  public void shouldReturnDummyValidator() {
    // Given:
    givenKafkaAuthorizer("", Collections.emptySet());

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(
        ksqlConfig,
        serviceContext
    );

    // Then
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  @Test
  public void shouldReturnDummyValidatorIfNotEnabled() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(
        ksqlConfig,
        serviceContext
    );

    // Then:
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
    verifyZeroInteractions(adminClient);
  }

  @Test
  public void shouldReturnAuthorizationValidatorIfEnabled() {
    // Given:
    when(ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR))
        .thenReturn(KsqlConfig.KSQL_ACCESS_VALIDATOR_ON);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(
        ksqlConfig,
        serviceContext
    );

    // Then:
    assertThat(validator, instanceOf(AuthorizationTopicAccessValidator.class));
    verifyZeroInteractions(adminClient);
  }

  @Test
  public void shouldReturnDummyValidatorIfAuthorizedOperationsReturnNull() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", null);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(
        ksqlConfig,
        serviceContext
    );

    // Then
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
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
}
