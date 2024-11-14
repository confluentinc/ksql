/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.services;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class SandboxedKafkaTopicClientTest {

  private SandboxedKafkaTopicClientTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase<KafkaTopicClient>> getMethodsToTest() {
      return TestMethods.builder(KafkaTopicClient.class)
          .ignore("createTopic", String.class, int.class, short.class)
          .ignore("createTopic", String.class, int.class, short.class, Map.class)
          .ignore("isTopicExists", String.class)
          .ignore("describeTopic", String.class)
          .ignore("getTopicConfig", String.class)
          .ignore("describeTopics", Collection.class)
          .ignore("deleteTopics", Collection.class)
          .ignore("listTopicsStartOffsets", Collection.class)
          .ignore("listTopicsEndOffsets", Collection.class)
          .build();
    }

    private final TestCase<KafkaTopicClient> testCase;
    private KafkaTopicClient sandboxedKafkaTopicClient;

    public UnsupportedMethods(final TestCase<KafkaTopicClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedKafkaTopicClient = SandboxedKafkaTopicClient.createProxy(
          mock(KafkaTopicClient.class),
          () -> mock(Admin.class)
      );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedKafkaTopicClient);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private KafkaTopicClient delegate;
    @Mock
    private Admin mockedAdmin;

    private KafkaTopicClient sandboxedClient;
    private final Map<String, ?> configs = ImmutableMap.of(
        "some config", 1,
        TopicConfig.RETENTION_MS_CONFIG, 8640000000L);

    @Before
    public void setUp() {
      sandboxedClient = SandboxedKafkaTopicClient.createProxy(
          delegate,
          () -> mockedAdmin
      );
    }

    @Test
    public void shouldTrackCreatedTopicWithNoConfig() {
      // Given:
      sandboxedClient.createTopic("some topic", 1, (short) 3);

      // Then:
      assertThat(sandboxedClient.isTopicExists("some topic"), is(true));
    }

    @Test
    public void shouldTrackCreatedTopicsWithConfig() {
      // Given:
      sandboxedClient.createTopic("some topic", 1, (short) 3, configs);

      // Then:
      assertThat(sandboxedClient.isTopicExists("some topic"), is(true));
      assertThat(sandboxedClient.getTopicConfig("some topic").entrySet(),
          equalTo(toStringConfigs(configs).entrySet()));
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test
    public void shouldNotCallDelegateOnIsTopicExistsIfTopicCreatedInScope() {
      // given:
      sandboxedClient.createTopic("some topic", 1, (short) 3, configs);
      Mockito.clearInvocations(delegate);

      // When:
      sandboxedClient.isTopicExists("some topic");

      // Then:
      verify(delegate, never()).isTopicExists("some topic");
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test
    public void shouldDelegateOnIsTopicExistsIfTopicNotCreatedInScope() {
      // When:
      sandboxedClient.isTopicExists("some topic");

      // Then:
      verify(delegate).isTopicExists("some topic");
    }

    @Test
    public void shouldTrackCreatedTopicDetails() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 3, configs);

      // When:
      final TopicDescription result = sandboxedClient
          .describeTopic("some topic");

      // Then:
      assertThat(result, is(new TopicDescription(
          "some topic",
          false,
          topicPartitions(2, 3),
          Sets.newHashSet(AclOperation.READ, AclOperation.WRITE))));
    }

    @Test
    public void shouldTrackCreatedTopicsDetails() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 3, configs);

      // When:
      final Map<String, TopicDescription> result = sandboxedClient
          .describeTopics(ImmutableList.of("some topic"));

      // Then:
      assertThat(result.keySet(), contains("some topic"));
      assertThat(result.get("some topic"), is(new TopicDescription(
          "some topic",
          false,
          topicPartitions(2, 3),
          Sets.newHashSet(AclOperation.READ, AclOperation.WRITE))));
    }

    @Test
    public void shouldCreateTopicWithBrokerDefaultReplicationFactor() {
      // Given:
      final short defaultReplicationFactor = 5;
      final short userBrokerDefaultReplicationFactor = -1;
      mockAdmin(defaultReplicationFactor);

      // When:
      sandboxedClient.createTopic("some topic", 2, userBrokerDefaultReplicationFactor, configs);

      // Then:
      final TopicDescription result = sandboxedClient
          .describeTopic("some topic");

      assertThat(result, is(new TopicDescription(
          "some topic",
          false,
          topicPartitions(2, defaultReplicationFactor),
          Sets.newHashSet(AclOperation.READ, AclOperation.WRITE))
      ));
    }

    private void mockAdmin(final short defaultReplicationFactor) {
      final Node broker = mock(Node.class);
      when(broker.idString()).thenReturn("someId");
      final KafkaFutureImpl<Collection<Node>> nodes = new KafkaFutureImpl<>();
      nodes.complete(Collections.singleton(broker));

      final DescribeClusterResult mockCluster = mock(DescribeClusterResult.class);
      when(mockCluster.nodes()).thenReturn(nodes);
      when(mockedAdmin.describeCluster()).thenReturn(mockCluster);

      final KafkaFutureImpl<Map<ConfigResource, Config>> config = new KafkaFutureImpl<>();
      config.complete(Collections.singletonMap(
          new ConfigResource(Type.BROKER, "someId"),
          new Config(Collections.singleton(
              new ConfigEntry("default.replication.factor", String.valueOf(defaultReplicationFactor))
          )))
      );
      final DescribeConfigsResult mockConfigs = mock (DescribeConfigsResult.class);
      when(mockConfigs.all()).thenReturn(config);

      when(mockedAdmin.describeConfigs(any())).thenReturn(mockConfigs);
    }

    @Test
    public void shouldThrowOnCreateIfValidateCreateTopicFails() {
      // Given:
      doThrow(TopicAuthorizationException.class).when(delegate)
          .validateCreateTopic("some topic", 2, (short) 3, configs);

      // Where:
      assertThrows(
          TopicAuthorizationException.class,
          () -> sandboxedClient.createTopic("some topic", 2, (short) 3, configs)
      );
    }

    @Test
    public void shouldNotCreateTopicIfValidateCreateTopicFails() {
      // Given:
      doThrow(TopicAuthorizationException.class).when(delegate)
          .validateCreateTopic("some topic", 2, (short) 3, configs);

      // When:
      try {
        sandboxedClient.createTopic("some topic", 2, (short) 3, configs);
      } catch (final TopicAuthorizationException e) {
        // skip
      }

      // Then:
      verify(delegate, times(0))
          .createTopic("some topic", 2, (short) 3, configs);
    }

    @Test
    public void shouldThrowOnCreateIfTopicPreviouslyCreatedInScopeWithDifferentPartitionCount() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 3, configs);

      // When:
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 4, (short) 3, configs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldThrowOnCreateIfTopicPreviouslyCreatedInScopeWithDifferentReplicaCount() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 1, configs);

      // When:
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 2, (short) 2, configs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldThrowOnCreateIfTopicPreviouslyCreatedInScopeWithDifferentRetentionMs() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 3, configs);

      // When:
      final Map<String, ?> newConfigs = ImmutableMap.of(
          TopicConfig.RETENTION_MS_CONFIG, 5000L);
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 2, (short) 3, newConfigs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldThrowOnCreateIfTopicAlreadyExistsWithDifferentPartitionCount() {
      // Given:
      givenTopicExists("some topic", 2, 3);

      // When:
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 3, (short) 3, configs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldThrowOnCreateIfTopicAlreadyExistsWithDifferentReplicaCount() {
      // Given:
      givenTopicExists("some topic", 2, 1);

      // When:
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 2, (short) 2, configs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldThrowOnCreateIfTopicAlreadyExistsWithDifferentRetentionMs() {
      // Given:
      givenTopicExists("some topic", 2, 1);

      // When:
      final Map<String, ?> newConfigs = ImmutableMap.of(
          TopicConfig.RETENTION_MS_CONFIG, 5000L);
      final KafkaTopicExistsException e = assertThrows(
          KafkaTopicExistsException.class,
          () -> sandboxedClient.createTopic("some topic", 2, (short) 1, newConfigs)
      );

      // Then:
      assertThat(e.getMessage(), containsString("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica/retention configuration than required"));
    }

    @Test
    public void shouldSupportDeleteTopics() {
      // Given:
      sandboxedClient.createTopic("some topic", 1, (short)1);

      // When:
      sandboxedClient.deleteTopics(ImmutableList.of("some topic"));

      // Then:
      verify(delegate, atMostOnce()).deleteTopics(any());

      // Should be able to recreate the topic with different params:
      sandboxedClient.createTopic("some topic", 3, (short)3);
    }

    @Test
    public void shouldNoOpDeletingTopicThatWasNotCreatedInScope() {
      // When:
      sandboxedClient.deleteTopics(ImmutableList.of("some topic"));

      // Then:
      verify(delegate, atMostOnce()).deleteTopics(any());
    }

    @Test
    public void shouldSupportListTopicsStartOffsets() {
      // Given:
      final ImmutableList<String> topicNames = ImmutableList.of("some topic");
      when(delegate.listTopicsStartOffsets(topicNames)).thenReturn(
          ImmutableMap.of(
              new TopicPartition("some topic", 0), 9L,
              new TopicPartition("some topic", 1), 10L
          ));
      // When:
      final Map<TopicPartition, Long> offsets = sandboxedClient.listTopicsStartOffsets(topicNames);

      // Then:
      assertEquals(2, offsets.keySet().size());
      assertEquals(Long.valueOf(9L), offsets.get(new TopicPartition("some topic", 0)));
      assertEquals(Long.valueOf(10L), offsets.get(new TopicPartition("some topic", 1)));
    }

    @Test
    public void shouldSupportListTopicsEndOffsets() {
      // Given:
      final ImmutableList<String> topicNames = ImmutableList.of("some topic");
      when(delegate.listTopicsEndOffsets(topicNames)).thenReturn(
          ImmutableMap.of(
              new TopicPartition("some topic", 0), 99L,
              new TopicPartition("some topic", 1), 100L
          ));
      // When:
      final Map<TopicPartition, Long> offsets = sandboxedClient.listTopicsEndOffsets(topicNames);

      // Then:
      assertEquals(2, offsets.keySet().size());
      assertEquals(Long.valueOf(99L), offsets.get(new TopicPartition("some topic", 0)));
      assertEquals(Long.valueOf(100L), offsets.get(new TopicPartition("some topic", 1)));
    }

    @SuppressWarnings("SameParameterValue")
    private void givenTopicExists(
        final String topic,
        final int numPartitions,
        final int numReplicas
    ) {
      when(delegate.isTopicExists(topic)).thenReturn(true);
      when(delegate.describeTopics(Collections.singleton(topic)))
          .thenReturn(Collections.singletonMap(
              topic,
              new TopicDescription(topic, false, topicPartitions(numPartitions, numReplicas))));
      when(delegate.getTopicConfig(topic)).thenReturn((Map<String, String>) configs);
    }

    private static List<TopicPartitionInfo> topicPartitions(
        final int numPartitions,
        final int numReplicas
    ) {
      final List<Node> replicas = unmodifiableList(IntStream.range(0, numReplicas)
          .mapToObj(idx -> (Node) null)
          .collect(Collectors.toList()));

      final Builder<TopicPartitionInfo> builder = ImmutableList.builder();
      IntStream.range(0, numPartitions)
          .mapToObj(idx -> new TopicPartitionInfo(idx + 1, null, replicas, emptyList()))
          .forEach(builder::add);

      return builder.build();
    }

    private static Map<String, String> toStringConfigs(final Map<String, ?> configs) {
      return configs.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }
  }
}