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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.ksql.util.PersistentQueryMetadata;
import org.junit.Before;
import org.junit.Test;

public class CommandRunnerTest {
  final StatementExecutor statementExecutor = mock(StatementExecutor.class);
  final KsqlEngine ksqlEngine = mock(KsqlEngine.class);
  final CommandStore commandStore = mock(CommandStore.class);

  private List<QueuedCommand> getQueuedCommands() {
    final List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
    return commandList.stream()
        .map(
            c -> new QueuedCommand(
                c.getLeft(), c.getRight(), Optional.empty()))
        .collect(Collectors.toList());
  }

  private List<QueuedCommand> getRestoreCommands(final List<Pair<CommandId, Command>> commandList) {
    return commandList.stream()
        .map(p -> new QueuedCommand(p.getLeft(), p.getRight(), Optional.empty()))
        .collect(Collectors.toList());
  }

  private List<QueuedCommand> getRestoreCommands() {
    return getRestoreCommands(new TestUtils().getAllPriorCommandRecords());
  }

  @Before
  public void setUp() {
    expect(statementExecutor.getKsqlEngine()).andStubReturn(ksqlEngine);
  }

  @Test
  public void shouldFetchAndRunNewCommandsFromCommandTopic() {
    // Given:
    final StatementExecutor statementExecutor = mock(StatementExecutor.class);
    final List<QueuedCommand> commands = getQueuedCommands();
    commands.forEach(
        c -> {
          statementExecutor.handleStatement(same(c));
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
    statementExecutor.handleStatement(command);
    expectLastCall().andThrow(new RuntimeException("Something bad happened"));
    statementExecutor.handleStatement(command);
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
    statementExecutor.handleStatement(command);
    final RuntimeException exception = new RuntimeException("something bad happened");
    expectLastCall().andThrow(exception).times(4);
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
    final List<QueuedCommand> restoreCommands = getRestoreCommands();
    restoreCommands.forEach(
        command -> {
          statementExecutor.handleRestore(command);
          expectLastCall();
        }
    );
    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
    final Collection<PersistentQueryMetadata> persistentQueries
        = ImmutableList.of(mock(PersistentQueryMetadata.class), mock(PersistentQueryMetadata.class));
    expect(ksqlEngine.getPersistentQueries()).andReturn(persistentQueries);
    persistentQueries.forEach(
        q -> {
          q.start();
          expectLastCall();
        }
    );
    replay(persistentQueries.toArray());
    replay(statementExecutor, ksqlEngine, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 1);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor);
    verify(persistentQueries.toArray());
  }

  @Test
  public void shouldRetryCommandsWhenRestoring() {
    // Given:
    final List<QueuedCommand> restoreCommands = getRestoreCommands();
    final QueuedCommand failedCommand = restoreCommands.get(0);
    statementExecutor.handleRestore(failedCommand);
    expectLastCall().andThrow(new RuntimeException("something bad happened"));
    restoreCommands.forEach(
        command -> {
          statementExecutor.handleRestore(command);
          expectLastCall();
        }
    );
    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
    expect(ksqlEngine.getPersistentQueries()).andReturn(Collections.emptySet());
    replay(statementExecutor, ksqlEngine, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor);
  }
}
