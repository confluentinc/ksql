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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

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
  private Command command1;
  @Mock
  private CommandId commandId1;
  @Mock
  private Command command2;
  @Mock
  private CommandId commandId2;
  @Mock
  private Command command3;
  @Mock
  private CommandId commandId3;

  private CommandRunner commandRunner;
  private List<QueuedCommand> queuedCommandList;


  @Before
  public void setup() {
    when(command1.getStatement()).thenReturn("command1");
    when(command2.getStatement()).thenReturn("command2");
    when(command3.getStatement()).thenReturn("command3");
    when(statementExecutor.getKsqlEngine()).thenReturn(ksqlEngine);

    queuedCommandList = getQueuedCommands(
        commandId1, command1,
        commandId2, command2,
        commandId3, command3);
    when(commandStore.getRestoreCommands()).thenReturn(queuedCommandList);
    when(commandStore.getNewCommands()).thenReturn(queuedCommandList);
    commandRunner = new CommandRunner(statementExecutor, commandStore, ksqlEngine, 1,
        clusterTerminator);
  }

  @Test
  public void shouldRunThePriorCommandsCorrectly() {
    // Given:

    // When:
    commandRunner.processPriorCommands();

    // Then:
    final InOrder inOrder = Mockito.inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommandList.get(0)));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommandList.get(1)));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommandList.get(2)));
  }

  @Test
  public void shouldRunThePriorCommandsWithTerminateCorrectly() {
    // Given:
    when(command3.getStatement()).thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(ksqlEngine).stopAcceptingStatements();
    verify(commandStore).close();
    verify(clusterTerminator).terminateCluster(anyList());
    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldPullAndRunStatements() {

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    final InOrder inOrder = Mockito.inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleStatement(queuedCommandList.get(0));
    inOrder.verify(statementExecutor).handleStatement(queuedCommandList.get(1));
    inOrder.verify(statementExecutor).handleStatement(queuedCommandList.get(2));

  }


  @Test
  public void shouldCloseTheCommandRunnerCorrectly() {
    // When:
    commandRunner.close();

    // Then:
    verify(commandStore).close();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowExceptionIfCannotCloseCommandStore() {
    // Given:
    doThrow(RuntimeException.class).when(commandStore).close();

    // When:
    commandRunner.close();
  }

  private static List<QueuedCommand> getQueuedCommands(final Object... args) {
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
