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
import static org.hamcrest.core.IsEqual.equalTo;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CommandTopicTest {

  private Consumer<CommandId, Command> commandConsumer;
  private Producer<CommandId, Command> commandProducer;

  private CommandTopic commandTopic;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    commandConsumer = Mockito.mock(Consumer.class);
    commandProducer = Mockito.mock(Producer.class);
    commandTopic = new CommandTopic("foo", commandConsumer, commandProducer);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSendCommandCorrectly() throws Exception {
    // When
    final Future<RecordMetadata> future = Mockito.mock(Future.class);
    Mockito.when(commandProducer.send(Mockito.any(ProducerRecord.class))).thenReturn(future);
    commandTopic.send(Mockito.mock(CommandId.class), Mockito.mock(Command.class));

    // Then
    Mockito.verify(commandProducer).send(Mockito.any(ProducerRecord.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldGetNewCommandsCorrectly() {
    // Given:
    final CommandId commandId1 = Mockito.mock(CommandId.class);
    final Command command1 = Mockito.mock(Command.class);
    final QueuedCommandStatus queuedCommandStatus1 = Mockito.mock(QueuedCommandStatus.class);
    final Map<CommandId, QueuedCommandStatus> commandStatusMap = new HashMap();
    commandStatusMap.put(commandId1, queuedCommandStatus1);
    final TopicPartition topicPartition = new TopicPartition("foo", 1);
    final ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.key()).thenReturn(commandId1);
    Mockito.when(consumerRecord.value()).thenReturn(command1);
    Mockito.when(commandConsumer.poll(Mockito.any(Duration.class))).thenReturn(new ConsumerRecords(Collections.singletonMap(topicPartition, Collections.singletonList(consumerRecord))));

    // When:
    final List<QueuedCommand> newCommands = commandTopic.getNewCommands(commandStatusMap);

    // Then:
    MatcherAssert.assertThat(newCommands.size(), CoreMatchers.equalTo(1));
    MatcherAssert.assertThat(newCommands.get(0).getCommandId(), CoreMatchers.is(commandId1));
    MatcherAssert.assertThat(newCommands.get(0).getStatus().get(), CoreMatchers.is(queuedCommandStatus1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldGetRestoreCommandsCorrectly() {
    // Given:
    final CommandId createId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final CommandId dropId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.DROP);
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final Command originalCommand = new Command(
        "some statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final Command dropCommand = new Command(
        "drop", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final Command latestCommand = new Command(
        "a new statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
            new ConsumerRecord<>("topic", 0, 0, createId, originalCommand),
            new ConsumerRecord<>("topic", 0, 0, dropId, dropCommand),
            new ConsumerRecord<>("topic", 0, 0, createId, latestCommand))
    ));
    Mockito.when(commandConsumer.poll(Mockito.any(Duration.class)))
        .thenReturn(records)
        .thenReturn(new ConsumerRecords(Collections.emptyMap()));

    // When:
    final RestoreCommands restoreCommands = commandTopic.getRestoreCommands(Duration.ofMillis(1));


    // Then:
    assertThat(restoreCommands.getToRestore().size(), equalTo(3));
    assertThat(restoreCommands.getToRestore().keySet(), equalTo(ImmutableSet.of(
        new Pair<>(0, createId),
        new Pair<>(1, dropId),
        new Pair<>(2, createId))));
  }

}
