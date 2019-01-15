/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class TryKafkaTopicClientTest {

  private TryKafkaTopicClientTest() {
  }

  @RunWith(Parameterized.class)
  public static class UnsupportedMethods {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> getMethodsToTest() {
      return TestMethods.builder(TryKafkaTopicClient.class)
          .ignore("createTopic", String.class, int.class, short.class, Map.class)
          .ignore("isTopicExists", String.class)
          .ignore("describeTopics", Collection.class)
          .build();
    }

    private final TestCase<TryKafkaTopicClient> testCase;
    private TryKafkaTopicClient tryKafkaTopicClient;

    public UnsupportedMethods(final TestCase<TryKafkaTopicClient> testCase) {
      this.testCase = Objects.requireNonNull(testCase, "testCase");
    }

    @Before
    public void setUp() {
      tryKafkaTopicClient = new TryKafkaTopicClient(mock(KafkaTopicClient.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnUnsupportedOperation() throws Throwable {
      testCase.invokeMethod(tryKafkaTopicClient);
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class SupportedMethods {

    @Mock
    private KafkaTopicClient delegate;
    private TryKafkaTopicClient tryKafkaTopicClient;
    private final Map<String, ?> configs = ImmutableMap.of("some config", 1);

    @Before
    public void setUp() {
      tryKafkaTopicClient = new TryKafkaTopicClient(delegate);
    }

    @Test
    public void shouldTrackCreatedTopics() {
      // Given:
      tryKafkaTopicClient.createTopic("some topic", 1, (short) 3, configs);

      // Then:
      assertThat(tryKafkaTopicClient.isTopicExists("some topic"), is(true));
    }

    @Test
    public void shouldNotCallDelegateOnIsTopicExistsIfTopicCreatedInScope() {
      // given:
      tryKafkaTopicClient.createTopic("some topic", 1, (short) 3, configs);

      // When:
      tryKafkaTopicClient.isTopicExists("some topic");

      // Then:
      verify(delegate, never()).isTopicExists("some topic");
    }

    @Test
    public void shouldDelegateOnIsTopicExistsIfTopicNotCreatedInScope() {
      // When:
      tryKafkaTopicClient.isTopicExists("some topic");

      // Then:
      verify(delegate).isTopicExists("some topic");
    }

    @Test
    public void shouldTrackCreatedTopicDetails() {
      // Given:
      tryKafkaTopicClient.createTopic("some topic", 2, (short) 3, configs);

      // When:
      final Map<String, TopicDescription> result = tryKafkaTopicClient
          .describeTopics(ImmutableSet.of("some topic"));

      // Then:
      final List<Node> threeReplicas = IntStream.range(0, 3)
          .mapToObj(idx -> (Node) null)
          .collect(Collectors.toList());

      assertThat(result.get("some topic"), is(new TopicDescription(
          "some topic",
          false,
          ImmutableList.of(
              new TopicPartitionInfo(1, null, threeReplicas, Collections.emptyList()),
              new TopicPartitionInfo(2, null, threeReplicas, Collections.emptyList())
          ))));
    }
  }
}