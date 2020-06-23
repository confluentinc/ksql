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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicInfoExtended;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KafkaTopicsListExtended;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListTopicsExecutorTest {

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Mock
  private AdminClient adminClient;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private SessionProperties sessionProperties;
  @Mock
  private Node node;

  @Before
  public void setUp() {
    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(serviceContext.getTopicClient()).thenReturn(topicClient);

    when(topicClient.listTopicNames()).thenReturn(ImmutableSet.of(
        "topic1",
        "topic2",
        "_confluent_any_topic"
    ));

    when(topicClient.describeTopics(any())).thenAnswer(inv -> {
      final Set<String> topics = inv.getArgument(0);

      return topics.stream()
          .collect(Collectors.toMap(
              Function.identity(),
              topic -> new TopicDescription(topic, false, makePartitionInfo())
              ));
    });
  }

  @Test
  public void shouldFilterOutInternalTopics() {
    // Given:
    final ConfiguredStatement<?> stmt = engine.configure("LIST TOPICS;");

    // When:
    executeListTopics(stmt, KafkaTopicsList.class);

    // Then:
    verify(topicClient).describeTopics(ImmutableSet.of("topic1", "topic2"));
  }

  @Test
  public void shouldNotFilterOutInternalTopics() {// Given:
    // Given:
    final ConfiguredStatement<?> stmt = engine.configure("LIST ALL TOPICS;");

    // When:
    executeListTopics(stmt, KafkaTopicsList.class);

    // Then:
    verify(topicClient).describeTopics(ImmutableSet.of("topic1", "topic2", "_confluent_any_topic"));
  }

  @Test
  public void shouldReturnBasicTopicInfo() {
    // Given:
    final ConfiguredStatement<?> stmt = engine.configure("LIST TOPICS;");

    // When:
    final KafkaTopicsList topicsList = executeListTopics(stmt, KafkaTopicsList.class);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", ImmutableList.of(3, 3)),
        new KafkaTopicInfo("topic2", ImmutableList.of(3, 3))
    ));
  }

  @Test
  public void shouldListKafkaTopicsThatDifferByCase() {
    // Given:
    when(topicClient.listTopicNames()).thenReturn(ImmutableSet.of(
        "topic1",
        "toPIc1"
    ));

    final ConfiguredStatement<?> stmt = engine.configure("LIST TOPICS;");

    // When:
    final KafkaTopicsList topicsList = executeListTopics(stmt, KafkaTopicsList.class);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfo("topic1", ImmutableList.of(3, 3)),
        new KafkaTopicInfo("toPIc1", ImmutableList.of(3, 3))
    ));
  }

  @Test
  public void shouldListKafkaTopicsExtended() {
    // Given:
    givenMockConsumers();

    givenPartitionMsgCounts(ImmutableMap.of(
       new TopicPartition("topic1", 0), 24L,
       new TopicPartition("topic1", 1), 1L,
       new TopicPartition("topic2", 0), 56L,
       new TopicPartition("topic2", 1), 3L
    ));

    final ConfiguredStatement<?> stmt = engine.configure("LIST TOPICS EXTENDED;");

    // When:
    final KafkaTopicsListExtended topicsList =
        executeListTopics(stmt, KafkaTopicsListExtended.class);

    // Then:
    assertThat(topicsList.getTopics(), containsInAnyOrder(
        new KafkaTopicInfoExtended("topic1", ImmutableList.of(3, 3), 25L, 0, 0),
        new KafkaTopicInfoExtended("topic2", ImmutableList.of(3, 3), 59L, 0, 0)
    ));
  }

  private void givenMockConsumers() {
    final ListConsumerGroupsResult result = mock(ListConsumerGroupsResult.class);
    final KafkaFutureImpl<Collection<ConsumerGroupListing>> groups = new KafkaFutureImpl<>();

    when(result.all()).thenReturn(groups);
    when(adminClient.listConsumerGroups()).thenReturn(result);
    groups.complete(ImmutableList.of());
  }

  private void givenPartitionMsgCounts(final Map<TopicPartition, Long> counts) {
    when(topicClient.maxMsgCounts(counts.keySet())).thenReturn(counts);
  }

  @SuppressWarnings("unchecked")
  private <T> T executeListTopics(
      final ConfiguredStatement<?> listTopics,
      final Class<T> returnType
  ) {
    final KsqlEntity result = CustomExecutors.LIST_TOPICS.execute(
        listTopics,
        sessionProperties,
        executionContext,
        serviceContext
    ).orElseThrow(IllegalStateException::new);

    assertThat(result, instanceOf(returnType));
    return (T) result;
  }

  private List<TopicPartitionInfo> makePartitionInfo() {
    return IntStream.range(0, 2)
        .mapToObj(idx -> new TopicPartitionInfo(idx, node, ImmutableList.of(node, node, node), ImmutableList.of(node, node)))
        .collect(Collectors.toList());
  }
}
