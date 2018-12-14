/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandTopicTest {

  private static final String COMMAND_TOPIC_NAME = "foo";
  @Mock
  private Consumer<CommandId, Command> commandConsumer;
  @Mock
  private Producer<CommandId, Command> commandProducer;

  private CommandTopic commandTopic;

  @Mock
  private Future<RecordMetadata> future ;

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
  private ArgumentCaptor<Collection<TopicPartition>> collectionArgumentCaptor;
  @Mock
  private Node node;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final static TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);


  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    commandTopic = new CommandTopic(COMMAND_TOPIC_NAME, commandConsumer, commandProducer);
    when(commandProducer.send(any(ProducerRecord.class))).thenReturn(future);
  }

  @Test
  public void shouldAssignCorrectAssignPartitionToConsumer() {
    verify(commandConsumer).assign(eq(Collections.singleton(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
  }

  @Test
  public void shouldSendCommandCorrectly() throws Exception {
    // When
    commandTopic.send(commandId1, command1);

    // Then
    verify(commandProducer).send(new ProducerRecord<>(COMMAND_TOPIC_NAME, 0, commandId1, command1));
    verify(future).get();
  }

  @Test
  public void shouldThrowExceptionIfSendIsNotSuccessfull() throws Exception {
    // Given:
    when(future.get()).thenThrow(new ExecutionException(new RuntimeException("Send was unsuccessful!")));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Send was unsuccessful!");

    // When
    commandTopic.send(commandId1, command1);
  }

  @Test
  public void shouldThrowRuntimeExceptionIfSendCausesRunTimeException() throws Exception {
    // Given:
    when(future.get()).thenThrow(new ExecutionException(new Exception("Send was unsuccessful because of non RunTime exception!")));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("java.lang.Exception: Send was unsuccessful because of non RunTime exception!");

    // When
    commandTopic.send(commandId1, command1);
  }

  @Test
  public void shouldThrowRuntimeExceptionIfSendThrowsInterruptedException() throws Exception {
    // Given:
    when(future.get()).thenThrow(new InterruptedException("InterruptedException"));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("InterruptedException");

    // When
    commandTopic.send(commandId1, command1);
  }

  @Test
  public void shouldGetNewCommandsIteratorCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

    // When:
    final Iterable<ConsumerRecord<CommandId, Command>> newCommands = commandTopic.getNewCommands(Duration.ofHours(1));

    // Then:
    assertThat(newCommands, sameInstance(consumerRecords));
  }

  @Test
  public void shouldGetRestoreCommandsCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 0, commandId2, command2)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId3, command3)))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic.getRestoreCommands(Duration.ofMillis(1));


    // Then:
    verify(commandConsumer).seekToBeginning(collectionArgumentCaptor.capture());
    assertThat(collectionArgumentCaptor.getValue(), equalTo(Collections.singletonList(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty()),
        new QueuedCommand(commandId2, command2, Optional.empty()),
        new QueuedCommand(commandId3, command3, Optional.empty()))));
  }


  @Test
  public void shouldGetRestoreCommandsCorrectlyWithDuplicateKeys() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 0, commandId2, command2)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId2, command3),
            new ConsumerRecord<>("topic", 0, 0, commandId3, command3)))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic.getRestoreCommands(Duration.ofMillis(1));


    // Then:
    verify(commandConsumer).seekToBeginning(collectionArgumentCaptor.capture());
    assertThat(collectionArgumentCaptor.getValue(), equalTo(Collections.singletonList(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty()),
        new QueuedCommand(commandId2, command2, Optional.empty()),
        new QueuedCommand(commandId2, command3, Optional.empty()),
        new QueuedCommand(commandId3, command3, Optional.empty()))));
  }


  @Test
  public void shouldFilterNullCommandsWhileRestoringCommands() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 0, commandId2, command2)))
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic.getRestoreCommands(Duration.ofMillis(1));


    // Then:
    verify(commandConsumer).seekToBeginning(collectionArgumentCaptor.capture());
    assertThat(collectionArgumentCaptor.getValue(), equalTo(Collections.singletonList(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty()),
        new QueuedCommand(commandId2, command2, Optional.empty()))));
  }

  @Test
  public void shouldCloseAllResources() {
    // When:
    commandTopic.close();

    //Then:
    final InOrder ordered = inOrder(commandConsumer);
    ordered.verify(commandConsumer).wakeup();
    ordered.verify(commandConsumer).close();
    verify(commandProducer).close();
  }


  @SuppressWarnings("unchecked")
  private static ConsumerRecords<CommandId, Command> someConsumerRecords(final ConsumerRecord... consumerRecords) {
    return new ConsumerRecords(
        ImmutableMap.of(TOPIC_PARTITION, ImmutableList.copyOf(consumerRecords)));
  }

}
