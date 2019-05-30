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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.services.ServiceContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class TopicAccessValidatorFactoryTest {
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;

  private Node node;

  @Before
  public void setUp() {
    node = new Node(1, "host", 9092);

    expect(serviceContext.getAdminClient()).andReturn(adminClient);
    replay(serviceContext);
  }

  @Test
  public void shouldReturnAuthorizationValidator() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", Collections.emptySet());

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(serviceContext, null);

    // Then
    assertThat(validator, is(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  @Test
  public void shouldReturnDummyValidator() {
    // Given:
    givenKafkaAuthorizer("", Collections.emptySet());

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(serviceContext, null);

    // Then
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  @Test
  public void shouldReturnDummyValidatorIfAuthorizedOperationsReturnNull() {
    // Given:
    givenKafkaAuthorizer("an-authorizer-class", null);

    // When:
    final TopicAccessValidator validator = TopicAccessValidatorFactory.create(serviceContext, null);

    // Then
    assertThat(validator, not(instanceOf(AuthorizationTopicAccessValidator.class)));
  }

  private void givenKafkaAuthorizer(
      final String className,
      final Set<AclOperation> authOperations
  ) {
    expect(adminClient.describeCluster()).andReturn(describeClusterResult(authOperations));
    expect(adminClient.describeCluster(anyObject()))
        .andReturn(describeClusterResult(authOperations));
    expect(adminClient.describeConfigs(describeBrokerRequest()))
        .andReturn(describeBrokerResult(Collections.singletonList(
            new ConfigEntry(KAFKA_AUTHORIZER_CLASS_NAME, className)
        )));

    replay(adminClient);
  }

  private DescribeClusterResult describeClusterResult(final Set<AclOperation> authOperations) {
    final Collection<Node> nodes = Collections.singletonList(node);
    final DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(nodes));
    expect(describeClusterResult.authorizedOperations())
        .andReturn(KafkaFuture.completedFuture(authOperations));
    replay(describeClusterResult);
    return describeClusterResult;
  }

  private Collection<ConfigResource> describeBrokerRequest() {
    return Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
  }

  private DescribeConfigsResult describeBrokerResult(final List<ConfigEntry> brokerConfigs) {
    final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    final Map<ConfigResource, Config> config = ImmutableMap.of(
        new ConfigResource(ConfigResource.Type.BROKER, node.idString()), new Config(brokerConfigs));
    expect(describeConfigsResult.all()).andReturn(KafkaFuture.completedFuture(config)).anyTimes();
    replay(describeConfigsResult);
    return describeConfigsResult;
  }
}
