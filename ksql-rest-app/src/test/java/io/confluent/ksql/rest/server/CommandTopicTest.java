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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.QueuedCommand;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.server.computation.RestoreCommands;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import java.time.Duration;
import java.util.Arrays;
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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
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
  @Captor
  private ArgumentCaptor<ProducerRecord> recordCaptor;

  @Mock
  private static CommandId commandId1;
  @Mock
  private static Command command1;
  @Mock
  private static CommandId commandId2;
  @Mock
  private static Command command2;
  @Mock
  private static CommandId commandId3;
  @Mock
  private static Command command3;

  @Mock
  private QueuedCommandStatus queuedCommandStatus1;
  @Mock
  private ConsumerRecord consumerRecord;
  @Mock
  private ConsumerRecords consumerRecords;
  @Captor
  private ArgumentCaptor<Collection<TopicPartition>> collectionArgumentCaptor;
  @Mock
  private Node node;

  private final PartitionInfo partitionInfo = new PartitionInfo("topic", 1, node,
      new Node[]{node}, new Node[]{node});

  private final static TopicPartition topicPartition = new TopicPartition("topic", 0);


  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    commandTopic = new CommandTopic(COMMAND_TOPIC_NAME, commandConsumer, commandProducer);
    when(commandProducer.send(any(ProducerRecord.class))).thenReturn(future);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSendCommandCorrectly() throws Exception {
    // When
    commandTopic.send(commandId1, command1);

    // Then
    verify(commandProducer).send(new ProducerRecord<>(COMMAND_TOPIC_NAME, commandId1, command1));
    verify(future).get();
  }

  @Test (expected = RuntimeException.class)
  @SuppressWarnings("unchecked")
  public void shouldThrowExceptionIfSendIsNotSuccessfull() throws Exception {
    // Given:
    when(future.get()).thenThrow(mock(ExecutionException.class));

    // When
    commandTopic.send(commandId1, command1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldGetNewCommandsIteratorCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

    // When:
    final Iterable<ConsumerRecord<CommandId, Command>> newCommands = commandTopic.getNewCommands(Duration.ofHours(1));

    // Then:
    assertThat(newCommands, sameInstance(consumerRecords));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldGetRestoreCommandsCorrectly() {
    // Given:
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords())
        .thenReturn(new ConsumerRecords(Collections.emptyMap()));

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic.getRestoreCommands(Duration.ofMillis(1));


    // Then:
    verify(commandConsumer).seekToBeginning(collectionArgumentCaptor.capture());
    assertThat(queuedCommandList, equalTo(ImmutableList.of(
        new QueuedCommand(commandId1, command1, Optional.empty()),
        new QueuedCommand(commandId2, command2, Optional.empty()),
        new QueuedCommand(commandId3, command3, Optional.empty()))));
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

  @Test
  public void shouldHaveAllCreateCommandsInOrder() {
    // Given:
    when(commandConsumer.partitionsFor(any(String.class))).thenReturn(ImmutableList.of(partitionInfo));
    when(commandConsumer.poll(any(Duration.class)))
        .thenReturn(someConsumerRecords());

    
//    final CommandId createId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
//    final CommandId dropId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.DROP);
//    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
//    final Command originalCommand = new Command(
//        "some statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
//    final Command dropCommand = new Command(
//        "drop", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
//    final Command latestCommand = new Command(
//        "a new statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

//    when(commandConsumer.poll(any(Duration.class))).thenReturn();

    // When:
    final List<QueuedCommand> queuedCommandList = commandTopic.getRestoreCommands(Duration.ofMillis(1));




  }


  private static ConsumerRecords<CommandId, Command> someConsumerRecords() {
    return new ConsumerRecords<>(
        ImmutableMap.of(topicPartition, ImmutableList.of(
            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
            new ConsumerRecord<>("topic", 0, 0, commandId2, command2),
            new ConsumerRecord<>("topic", 0, 0, commandId3, command3))
        ));
  }

}
