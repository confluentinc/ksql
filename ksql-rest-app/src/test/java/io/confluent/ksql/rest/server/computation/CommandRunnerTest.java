/**
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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

public class CommandRunnerTest {
  final StatementExecutor statementExecutor = mock(StatementExecutor.class);
  final CommandStore commandStore = mock(CommandStore.class);

  private List<QueuedCommand> getQueuedCommands() {
    final List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
    return commandList.stream()
        .map(
            c -> new QueuedCommand(c.getLeft(), Optional.ofNullable(c.getRight()), Optional.empty()))
        .collect(Collectors.toList());
  }

  private RestoreCommands getRestoreCommands(final List<Pair<CommandId, Command>> commandList) {
    final RestoreCommands restoreCommands = new RestoreCommands();
    commandList.forEach(
        idCommandPair -> restoreCommands.addCommand(idCommandPair.left, idCommandPair.right)
    );
    return restoreCommands;
  }

  private RestoreCommands getRestoreCommands() {
    return getRestoreCommands(new TestUtils().getAllPriorCommandRecords());
  }

  @Test
  public void shouldFetchAndRunNewCommandsFromCommandTopic() {
    // Given:
    final List<QueuedCommand> commands = getQueuedCommands();
    commands.forEach(
        c -> {
          statementExecutor.handleStatement(
              same(c.getCommand().get()), same(c.getCommandId()), same(c.getStatus()));
          expectLastCall();
        }
    );
    expect(commandStore.getNewCommands()).andReturn(commands);
    replay(statementExecutor, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 1);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(statementExecutor);
  }

  @Test
  public void shouldRetryCommands() {
    // Given:
    final List<QueuedCommand> commands = Collections.singletonList(getQueuedCommands().get(0));
    final QueuedCommand command = commands.get(0);
    statementExecutor.handleStatement(
        same(command.getCommand().get()),
        same(command.getCommandId()),
        same(command.getStatus()));
    expectLastCall().andThrow(new RuntimeException("Something bad happened"));
    statementExecutor.handleStatement(
        same(command.getCommand().get()),
        same(command.getCommandId()),
        same(command.getStatus()));
    expectLastCall();
    expect(commandStore.getNewCommands()).andReturn(commands);
    replay(statementExecutor, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(statementExecutor);
  }

  @Test
  public void shouldGiveUpAfterRetryLimit() {
    // Given:
    final List<QueuedCommand> commands = Collections.singletonList(getQueuedCommands().get(0));
    final QueuedCommand command = commands.get(0);
    statementExecutor.handleStatement(
        same(command.getCommand().get()),
        same(command.getCommandId()),
        same(command.getStatus()));
    final RuntimeException exception = new RuntimeException("something bad happened");
    expectLastCall().andThrow(exception).times(3);
    expect(commandStore.getNewCommands()).andReturn(commands);
    replay(statementExecutor, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);

    // When:
    try {
      commandRunner.fetchAndRunCommands();

      // Then:
      fail("Should have thrown exception");
    } catch (final RuntimeException caught) {
      assertThat(caught, equalTo(exception));
    }
    verify(statementExecutor);
  }

  @Test
  public void shouldFetchAndRunPriorCommandsFromCommandTopic() {
    // Given:
    final RestoreCommands restoreCommands = getRestoreCommands();
    restoreCommands.forEach(
        (commandId, command, terminatedQueries, wasDropped) -> {
          statementExecutor.handleStatementWithTerminatedQueries(
              command,
              commandId,
              Optional.empty(),
              terminatedQueries,
              wasDropped);
          expectLastCall();
        }
    );
    replay(statementExecutor);
    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
    replay(commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 1);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor);
  }

  @Test
  public void shouldRetryCommandsWhenRestoring() {
    // Given:
    final List<Pair<CommandId, Command>> commands = new TestUtils().getAllPriorCommandRecords();
    final CommandId failedCommandId = commands.get(0).getLeft();
    final Command failedCommand = commands.get(0).getRight();
    final RestoreCommands restoreCommands = getRestoreCommands(commands);
    statementExecutor.handleStatementWithTerminatedQueries(
        failedCommand,
        failedCommandId,
        Optional.empty(),
        Collections.emptyMap(),
        false);
    expectLastCall().andThrow(new RuntimeException("something bad happened"));
    restoreCommands.forEach(
        (commandId, command, terminatedQueries, wasDropped) -> {
          statementExecutor.handleStatementWithTerminatedQueries(
              command,
              commandId,
              Optional.empty(),
              terminatedQueries,
              wasDropped);
          expectLastCall();
        }
    );
    replay(statementExecutor);
    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
    replay(commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor);
  }
}
