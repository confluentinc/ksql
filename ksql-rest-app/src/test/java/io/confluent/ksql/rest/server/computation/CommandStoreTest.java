/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;

import io.confluent.ksql.util.Pair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class CommandStoreTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private CommandIdAssigner commandIdAssigner;
  @Mock
  private CommandTopic commandTopic;
  @Mock
  private Statement statement;

  @Mock
  private CommandId commandId1;
  @Mock
  private Command command1;
  @Mock
  private CommandId commandId2;

  private CommandStore commandStore;

  @Before
  public void setup() {
    commandStore = new CommandStore(commandIdAssigner, commandTopic);
  }

  @Test
  public void shouldHaveAllCreateCommandsInOrder() {
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

    final List<QueuedCommand> queuedCommandList = buildRecords(
      createId, originalCommand,
      dropId, dropCommand,
      createId, latestCommand
    );
    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(queuedCommandList);

    // When:
    final List<QueuedCommand> commands = commandStore.getRestoreCommands();

    // Then:
    assertThat(commands, equalTo(Arrays.asList(new QueuedCommand(createId, originalCommand),
        new QueuedCommand(dropId, dropCommand),
        new QueuedCommand(createId, latestCommand))));
  }

  @Test (expected = IllegalStateException.class)
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    doThrow(new RuntimeException("oops!")).doNothing().when(commandTopic).send(any(), any());
    try {
      commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());
      fail();
    } catch (RuntimeException r) {
      // Do nothing.
    }

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic, times(2)).send(any(CommandId.class), any(Command.class));

  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());


    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));

  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1)));
    doAnswer(
        (Answer) invocation -> {
          List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();
          assertThat(queuedCommandList.size(), equalTo(1));
          assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
          return null;

        }
    ).when(commandTopic).send(any(CommandId.class), any(Command.class));

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));
    verify(commandTopic).getNewCommands(any(Duration.class));
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(
            new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1),
            new ConsumerRecord<CommandId, Command>("", 1, 1, commandId2, null)
            ));

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands();

    // Then:
    assertThat(commands, equalTo(ImmutableList.of(new QueuedCommand(commandId1, command1))));
    verify(commandTopic).getNewCommands(any(Duration.class));
  }


  @Test
  public void shouldDistributeCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1)));
    doAnswer(
        (Answer) invocation -> {
          List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();
          assertThat(queuedCommandList.size(), equalTo(1));
          assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
          return null;

        }
    ).when(commandTopic).send(any(CommandId.class), any(Command.class));

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));
    verify(commandTopic).getNewCommands(any(Duration.class));
  }

  @Test
  public void shouldCloseCorrectly() {
    // When:
    commandStore.close();

    // Then:
    verify(commandTopic).close();
  }

  private void setupCommandTopicToReturnCommand(final CommandId commandId, final Command command) {
    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(buildRecords(commandId, command));
  }

  private CommandStore createCommandStoreThatAssignsSameId(final CommandId commandId) {
    final CommandIdAssigner commandIdAssigner = mock(CommandIdAssigner.class);
    when(commandIdAssigner.getCommandId(any())).thenReturn(new CommandId(commandId.getType(), commandId.getEntity(), commandId.getAction()));
    return createCommandStore(commandIdAssigner);
  }

  private CommandStore createCommandStore() {
    return createCommandStore(new CommandIdAssigner(new MetaStoreImpl(new InternalFunctionRegistry())));
  }

  private CommandStore createCommandStore(final CommandIdAssigner commandIdAssigner) {
    return new CommandStore(commandIdAssigner, commandTopic);
  }

  private static List<Pair<CommandId, Command>> getPriorCommands(final CommandStore commandStore) {
    return commandStore.getRestoreCommands().stream()
        .map(
            queuedCommand -> new Pair<>(
                queuedCommand.getCommandId(), queuedCommand.getCommand()))
        .collect(Collectors.toList());
  }

  private static List<QueuedCommand> buildRecords(final Object ...args) {
    assertThat(args.length % 2, equalTo(0));
    final List<QueuedCommand> queuedCommandList = new ArrayList<>();
    for (int i = 0; i < args.length; i += 2) {
      assertThat(args[i], instanceOf(CommandId.class));
      assertThat(args[i + 1], anyOf(is(nullValue()), instanceOf(Command.class)));
      queuedCommandList.add(
          new QueuedCommand((CommandId) args[i], (Command) args[i + 1]));
    }
    return queuedCommandList;
  }

}
