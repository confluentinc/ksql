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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.confluent.ksql.rest.server.computation.QueuedCommand;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandTopicTest {

  private static final String COMMAND_TOPIC_NAME = "foo";
  @Mock
  private Consumer<byte[], byte[]> commandConsumer;

  private CommandTopic commandTopic;

  @Mock
  private CommandTopicBackup commandTopicBackup;

  private final byte[] commandId1 = "commandId1".getBytes(Charset.defaultCharset());
  private final byte[] command1 = "command1".getBytes(Charset.defaultCharset());
  private final byte[] commandId2 = "commandId2".getBytes(Charset.defaultCharset());
  private final byte[] command2 = "command2".getBytes(Charset.defaultCharset());
  private final byte[] commandId3 = "commandId3".getBytes(Charset.defaultCharset());
  private final byte[] command3 = "command3".getBytes(Charset.defaultCharset());
  
  private ConsumerRecord<byte[], byte[]> record1;
  private ConsumerRecord<byte[], byte[]> record2;
  private ConsumerRecord<byte[], byte[]> record3;
  

  @Mock
  private ConsumerRecords<byte[], byte[]> consumerRecords;
  @Captor
  private ArgumentCaptor<Collection<TopicPartition>> topicPartitionsCaptor;

  private final static TopicPartition TOPIC_PARTITION = new TopicPartition(COMMAND_TOPIC_NAME, 0);

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    record1 = new ConsumerRecord<>("topic", 0, 0, commandId1, command1);
    record2 = new ConsumerRecord<>("topic", 0, 1, commandId2, command2);
    record3 = new ConsumerRecord<>("topic", 0, 2, commandId3, command3);
    commandTopic = new CommandTopic(COMMAND_TOPIC_NAME, commandConsumer, commandTopicBackup);
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
    final Iterable<ConsumerRecord<byte[], byte[]>> newCommands = commandTopic
        .getNewCommands(Duration.ofHours(1));

    // Then:
    assertThat(newCommands, sameInstance(consumerRecords));
  }

  @Test
  public void shouldGetRestoreCommandsCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            record1,
            record2))
        .thenReturn(someConsumerRecords(
            record3))
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
            record1,
            record2,
            new ConsumerRecord<>("topic", 0, 2, commandId2, null)
        ))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> recordList = commandTopic
        .getRestoreCommands(Duration.ofMillis(1));

    // Then:
    assertThat(recordList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty(), 0L),
        new QueuedCommand(commandId2, command2, Optional.empty(),1L))));
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
    final ConsumerRecords<byte[], byte[]> records = someConsumerRecords(record1, record2, record3);

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
  public void shouldInitializeCommandTopicBackup() {
    // When
    commandTopic.start();

    // Then
    verify(commandTopicBackup, times(1)).initialize();
  }

  @Test
  public void shouldCloseCommandTopicBackup() {
    // When
    commandTopic.close();

    // Then
    verify(commandTopicBackup, times(1)).close();
  }

  @Test
  public void shouldBackupRestoreCommands() {
    // Given
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(record1, record2))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
    commandTopic.start();

    // When
    commandTopic.getRestoreCommands(Duration.ofHours(1));

    // Then
    final InOrder inOrder = Mockito.inOrder(commandTopicBackup);
    inOrder.verify(commandTopicBackup, times(1)).writeRecord(record1);
    inOrder.verify(commandTopicBackup, times(1)).writeRecord(record2);
  }

  @Test
  public void shouldBackupNewCommands() {
    // Given
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(record1, record2))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
    commandTopic.start();

    // When
    commandTopic.getNewCommands(Duration.ofHours(1));

    // Then
    final InOrder inOrder = Mockito.inOrder(commandTopicBackup);
    inOrder.verify(commandTopicBackup, times(1)).writeRecord(record1);
    inOrder.verify(commandTopicBackup, times(1)).writeRecord(record2);
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
  private static ConsumerRecords<byte[], byte[]> someConsumerRecords(
      final ConsumerRecord<byte[], byte[]>... consumerRecords
  ) {
    return new ConsumerRecords<>(
        ImmutableMap.of(TOPIC_PARTITION, ImmutableList.copyOf(consumerRecords)));
  }
}
