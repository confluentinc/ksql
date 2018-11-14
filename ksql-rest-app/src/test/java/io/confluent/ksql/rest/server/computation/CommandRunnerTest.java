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

import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

public class CommandRunnerTest {

  private List<QueuedCommand> getQueuedCommands() {
    final List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
    return commandList.stream()
        .map(
            c -> new QueuedCommand(
                c.getLeft(), c.getRight(), Optional.empty()))
        .collect(Collectors.toList());
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
    replay(statementExecutor);
    final CommandStore commandStore = mock(CommandStore.class);
    expect(commandStore.getNewCommands()).andReturn(commands);
    replay(commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(statementExecutor);
  }

  @Test
  public void shouldFetchAndRunPriorCommandsFromCommandTopic() {
    // Given:
    final List<QueuedCommand> commands = new LinkedList<>();
    final CommandStore commandStore = mock(CommandStore.class);
    expect(commandStore.getRestoreCommands()).andReturn(commands);
    final StatementExecutor statementExecutor = mock(StatementExecutor.class);
    statementExecutor.handleRestoration(commands);
    expectLastCall();
    replay(statementExecutor, commandStore);
    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor);
  }
}
