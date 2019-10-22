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

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandTopicTest {

  private static final String COMMAND_TOPIC_NAME = "foo";
  @Mock
  private Consumer<CommandId, Command> commandConsumer;

  private CommandTopic commandTopic;

  @Mock
  private Future<RecordMetadata> future;

  @Mock
  private CommandId commandId1;
  @Mock
  private Command command1;
  @Mock
  private CommandId commandId2;
  @Mock
  private Command command2;
  @Mock
  private CommandId commandId3;
  @Mock
  private Command command3;

  @Mock
  private ConsumerRecords<CommandId, Command> consumerRecords;
  @Captor
  private ArgumentCaptor<Collection<TopicPartition>> topicPartitionsCaptor;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final static TopicPartition TOPIC_PARTITION = new TopicPartition(COMMAND_TOPIC_NAME, 0);

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    commandTopic = new CommandTopic(COMMAND_TOPIC_NAME, commandConsumer);
  }

  @Test
  public void shouldAssignCorrectPartitionToConsumer() {
    // When:
    commandTopic.start();

    // Then:
    verify(commandConsumer)
        .assign(eq(Collections.singleton(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
  }

  @Test
  public void shouldGetNewCommandsIteratorCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

    // When:
    final Iterable<ConsumerRecord<CommandId, Command>> newCommands = commandTopic
        .getNewCommands(Duration.ofHours(1));

    // Then:
    assertThat(newCommands, sameInstance(consumerRecords));
  }

  @Test
  public void shouldGetRestoreCommandsCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 1, commandId2, command2)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 2, commandId3, command3)))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic
        .getRestoreCommands(Duration.ofMillis(1));

    // Then:
    verify(commandConsumer).seekToBeginning(topicPartitionsCaptor.capture());
    assertThat(topicPartitionsCaptor.getValue(),
        equalTo(Collections.singletonList(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
        new QueuedCommand(commandId2, command2, Optional.empty(), 1L),
        new QueuedCommand(commandId3, command3, Optional.empty(), 2L))));
  }

  @Test
  public void shouldHaveOffsetsInQueuedCommands() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
            .thenReturn(someConsumerRecords(
                    new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
                    new ConsumerRecord<>("topic", 0, 1, commandId2, command2)))
            .thenReturn(someConsumerRecords(
                    new ConsumerRecord<>("topic", 0, 2, commandId3, command3)))
            .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic
            .getRestoreCommands(Duration.ofMillis(1));

    // Then:
    verify(commandConsumer).seekToBeginning(topicPartitionsCaptor.capture());
    assertThat(topicPartitionsCaptor.getValue(),
            equalTo(Collections.singletonList(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
            new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
            new QueuedCommand(commandId2, command2, Optional.empty(),1L),
            new QueuedCommand(commandId3, command3, Optional.empty(), 2L))));
  }

  @Test
  public void shouldGetRestoreCommandsCorrectlyWithDuplicateKeys() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 1, commandId2, command2)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 2, commandId2, command3),
            new ConsumerRecord<>("topic", 0, 3, commandId3, command3)))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic
        .getRestoreCommands(Duration.ofMillis(1));

    // Then:
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
        new QueuedCommand(commandId2, command2, Optional.empty(), 1L),
        new QueuedCommand(commandId2, command3, Optional.empty(), 2L),
        new QueuedCommand(commandId3, command3, Optional.empty(), 3L))));
  }


  @Test
  public void shouldFilterNullCommandsWhileRestoringCommands() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 1, commandId2, command2),
            new ConsumerRecord<>("topic", 0, 2, commandId2, null)
        ))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic
        .getRestoreCommands(Duration.ofMillis(1));

    // Then:
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
        new QueuedCommand(commandId2, command2, Optional.empty(), 1L))));
  }

  @Test
  public void shouldWakeUp() {
    // When:
    commandTopic.wakeup();

    //Then:
    verify(commandConsumer).wakeup();
  }

  @Test
  public void shouldCloseAllResources() {
    // When:
    commandTopic.close();

    //Then:
    verify(commandConsumer).close();
  }

  @Test
  public void shouldHaveAllCreateCommandsInOrder() {
    // Given:
    final ConsumerRecords<CommandId, Command> records = someConsumerRecords(
        new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
        new ConsumerRecord<>("topic", 0, 1, commandId2, command2),
        new ConsumerRecord<>("topic", 0, 2, commandId3, command3));

    when(commandTopic.getNewCommands(any()))
        .thenReturn(records)
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> commands = commandTopic.getRestoreCommands(Duration.ofMillis(10));

    // Then:
    assertThat(commands, equalTo(Arrays.asList(
        new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
        new QueuedCommand(commandId2, command2, Optional.empty(), 1L),
        new QueuedCommand(commandId3, command3, Optional.empty(), 2L)
    )));
  }

  @Test
  public void shouldGetEndOffsetCorrectly() {
    // Given:
    when(commandConsumer.endOffsets(any()))
        .thenReturn(Collections.singletonMap(TOPIC_PARTITION, 123L));

    // When:
    final long endOff = commandTopic.getEndOffset();

    // Then:
    assertThat(endOff, equalTo(123L));
    verify(commandConsumer).endOffsets(Collections.singletonList(TOPIC_PARTITION));
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  private static ConsumerRecords<CommandId, Command> someConsumerRecords(
      final ConsumerRecord<CommandId, Command>... consumerRecords
  ) {
    return new ConsumerRecords<>(
        ImmutableMap.of(TOPIC_PARTITION, ImmutableList.copyOf(consumerRecords)));
  }
}
