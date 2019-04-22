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
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.test.util.TestMethods;
import io.confluent.ksql.test.util.TestMethods.TestCase;
import io.confluent.ksql.util.KafkaTopicClient;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
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
          .ignore("describeTopics", Collection.class)
          .ignore("deleteTopics", Collection.class)
          .build();
    }

    private final TestCase<KafkaTopicClient> testCase;
    private KafkaTopicClient sandboxedKafkaTopicClient;

    public UnsupportedMethods(final TestCase<KafkaTopicClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      sandboxedKafkaTopicClient = SandboxedKafkaTopicClient.createProxy(mock(KafkaTopicClient.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(sandboxedKafkaTopicClient);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Mock
    private KafkaTopicClient delegate;
    private KafkaTopicClient sandboxedClient;
    private final Map<String, ?> configs = ImmutableMap.of("some config", 1);

    @Before
    public void setUp() {
      sandboxedClient = SandboxedKafkaTopicClient.createProxy(delegate);
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
    }

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
          topicPartitions(2, 3))));
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
          topicPartitions(2, 3))));
    }

    @Test
    public void shouldThrowOnCreateIfTopicPreviouslyCreatedInScopeWithDifferentPartitionCount() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 3, configs);

      // Expect:
      expectedException.expect(KafkaTopicExistsException.class);
      expectedException.expectMessage("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica configuration than required");

      // When:
      sandboxedClient.createTopic("some topic", 4, (short) 3, configs);
    }

    @Test
    public void shouldThrowOnCreateIfTopicPreviouslyCreatedInScopeWithDifferentReplicaCount() {
      // Given:
      sandboxedClient.createTopic("some topic", 2, (short) 1, configs);

      // Expect:
      expectedException.expect(KafkaTopicExistsException.class);
      expectedException.expectMessage("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica configuration than required");

      // When:
      sandboxedClient.createTopic("some topic", 2, (short) 2, configs);
    }

    @Test
    public void shouldThrowOnCreateIfTopicAlreadyExistsWithDifferentPartitionCount() {
      // Given:
      givenTopicExists("some topic", 2, 3);

      // Expect:
      expectedException.expect(KafkaTopicExistsException.class);
      expectedException.expectMessage("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica configuration than required");

      // When:
      sandboxedClient.createTopic("some topic", 3, (short) 3, configs);
    }

    @Test
    public void shouldThrowOnCreateIfTopicAlreadyExistsWithDifferentReplicaCount() {
      // Given:
      givenTopicExists("some topic", 2, 1);

      // Expect:
      expectedException.expect(KafkaTopicExistsException.class);
      expectedException.expectMessage("A Kafka topic with the name 'some topic' already "
          + "exists, with different partition/replica configuration than required");

      // When:
      sandboxedClient.createTopic("some topic", 2, (short) 2, configs);
    }

    @Test
    public void shouldSupportDeleteTopics() {
      // Given:
      sandboxedClient.createTopic("some topic", 1, (short)1);

      // When:
      sandboxedClient.deleteTopics(ImmutableList.of("some topic"));

      // Then:
      verify(delegate, never()).deleteTopics(any());

      // Should be able to recreate the topic with different params:
      sandboxedClient.createTopic("some topic", 3, (short)3);
    }

    @Test
    public void shouldNoOpDeletingTopicThatWasNotCreatedInScope() {
      // When:
      sandboxedClient.deleteTopics(ImmutableList.of("some topic"));

      // Then:
      verifyZeroInteractions(delegate);
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
  }
}