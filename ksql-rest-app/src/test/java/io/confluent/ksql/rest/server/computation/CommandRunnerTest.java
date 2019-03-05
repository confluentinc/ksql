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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class CommandRunnerTest {

  @Mock
  private StatementExecutor statementExecutor;
  @Mock
  private CommandStore commandStore;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private ClusterTerminator clusterTerminator;
  @Mock
  private Command command;
  @Mock
  private Command clusterTerminate;
  @Mock
  private QueuedCommand queuedCommand1;
  @Mock
  private QueuedCommand queuedCommand2;
  @Mock
  private QueuedCommand queuedCommand3;
  @Mock
  private ExecutorService executor;
  private CommandRunner commandRunner;

  @Before
  public void setup() {
    when(statementExecutor.getKsqlEngine()).thenReturn(ksqlEngine);

    when(command.getStatement()).thenReturn("something that is not terminate");
    when(clusterTerminate.getStatement())
        .thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);

    when(queuedCommand1.getCommand()).thenReturn(command);
    when(queuedCommand2.getCommand()).thenReturn(command);
    when(queuedCommand3.getCommand()).thenReturn(command);

    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    commandRunner = new CommandRunner(
        statementExecutor,
        commandStore,
        ksqlEngine,
        1,
        clusterTerminator,
        executor);
  }

  @Test
  public void shouldRunThePriorCommandsCorrectly() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand3));
  }

  @Test
  public void shouldRunThePriorCommandsWithTerminateCorrectly() {
    // Given:
    givenQueuedCommands(queuedCommand1);
    when(queuedCommand1.getCommand()).thenReturn(clusterTerminate);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(ksqlEngine).stopAcceptingStatements();
    verify(commandStore).close();
    verify(clusterTerminator).terminateCluster(anyList());
    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldEarlyOutIfRestoreContainsTerminate() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand2.getCommand()).thenReturn(clusterTerminate);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(ksqlEngine).stopAcceptingStatements();
    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldPullAndRunStatements() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleStatement(queuedCommand1);
    inOrder.verify(statementExecutor).handleStatement(queuedCommand2);
    inOrder.verify(statementExecutor).handleStatement(queuedCommand3);
  }

  @Test
  public void shouldEarlyOutIfNewCommandsContainsTerminate() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand2.getCommand()).thenReturn(clusterTerminate);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(ksqlEngine).stopAcceptingStatements();
    verify(statementExecutor, never()).handleRestore(queuedCommand1);
    verify(statementExecutor, never()).handleRestore(queuedCommand2);
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
  }

  @Test
  public void shouldEarlyOutOnShutdown() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2);
    doAnswer(closeRunner()).when(statementExecutor).handleStatement(queuedCommand1);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(statementExecutor, never()).handleRestore(queuedCommand2);
  }

  @Test
  public void shouldNotBlockIndefinitelyPollingForNewCommands() {
    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(commandStore).getNewCommands(argThat(not(Duration.ofMillis(Long.MAX_VALUE))));
  }

  @Test
  public void shouldSubmitTaskOnStart() {
    // When:
    commandRunner.start();

    // Then:
    final InOrder inOrder = inOrder(executor);
    inOrder.verify(executor).submit(any(Runnable.class));
    inOrder.verify(executor).shutdown();
  }

  @Test
  public void shouldCloseTheCommandRunnerCorrectly() throws Exception {
    // When:
    commandRunner.close();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionIfCannotCloseCommandStore() {
    // Given:
    doThrow(RuntimeException.class).when(commandStore).close();

    // When:
    commandRunner.close();
  }

  private void givenQueuedCommands(final QueuedCommand... cmds) {
    when(commandStore.getRestoreCommands()).thenReturn(Arrays.asList(cmds));
    when(commandStore.getNewCommands(any())).thenReturn(Arrays.asList(cmds));
  }

  private Answer closeRunner() {
    return inv -> {
      commandRunner.close();
      return null;
    };
  }
}
