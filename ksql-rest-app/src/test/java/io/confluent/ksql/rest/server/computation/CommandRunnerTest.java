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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.server.computation.RestoreCommands.ForEach;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
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

  @Mock
  private RestoreCommands restoreCommands;

  @Captor
  private ArgumentCaptor<Command> commandCaptor;
  @Captor
  private ArgumentCaptor<CommandId> commandIdCaptor;
  private CommandRunner commandRunner;

//  @Before
//  public void setup() {
//    commandRunner = new CommandRunner(statementExecutor, commandStore, ksqlEngine, 1, clusterTerminator);
//    when(command1.getStatement()).thenReturn("");
//    when(command2.getStatement()).thenReturn("");
//    when(command3.getStatement()).thenReturn("");
//=======
//  final StatementExecutor statementExecutor = mock(StatementExecutor.class);
//  final KsqlEngine ksqlEngine = mock(KsqlEngine.class);
//  final CommandStore commandStore = mock(CommandStore.class);
//
//  private List<QueuedCommand> getQueuedCommands() {
//    final List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
//    return commandList.stream()
//        .map(
//            c -> new QueuedCommand(
//                c.getLeft(), c.getRight(), Optional.empty()))
//        .collect(Collectors.toList());
//  }
//
//  private List<QueuedCommand> getRestoreCommands(final List<Pair<CommandId, Command>> commandList) {
//    return commandList.stream()
//        .map(p -> new QueuedCommand(p.getLeft(), p.getRight(), Optional.empty()))
//        .collect(Collectors.toList());
//  }
//
//  private List<QueuedCommand> getRestoreCommands() {
//    return getRestoreCommands(new TestUtils().getAllPriorCommandRecords());
//>>>>>>> upstream/master
//  }
//
//  @Before
//  public void setUp() {
//    expect(statementExecutor.getKsqlEngine()).andStubReturn(ksqlEngine);
//  }
//
//  @Test
//  @SuppressWarnings("unchecked")
//  public void shouldFetchAndRunNewCommandsFromCommandTopic() {
//    // Given:
//<<<<<<< HEAD
//    final List<QueuedCommand> queuedCommandList = givenQueuedCommands(
//        ImmutableList.of(
//            Pair.of(commandId1, command1),
//            Pair.of(commandId2, command2),
//            Pair.of(commandId3, command3)
//        )
//=======
//    final StatementExecutor statementExecutor = mock(StatementExecutor.class);
//    final List<QueuedCommand> commands = getQueuedCommands();
//    commands.forEach(
//        c -> {
//          statementExecutor.handleStatement(same(c));
//          expectLastCall();
//        }
//>>>>>>> upstream/master
//    );
//    when(commandStore.getNewCommands()).thenReturn(queuedCommandList);
//
//    // When:
//    commandRunner.fetchAndRunCommands();
//
//    // Then:
//    final InOrder ordered = inOrder(statementExecutor);
//    verify(statementExecutor).handleStatement(eq(command1), eq(commandId1), any());
//    verify(statementExecutor).handleStatement(eq(command2), eq(commandId2), any());
//    verify(statementExecutor).handleStatement(eq(command3), eq(commandId3), any());
//  }
//
//  @Test
//  @SuppressWarnings("unchecked")
//  public void shouldFetchAndRunTerminateCommandFromCommandTopic() {
//    // Given:
//<<<<<<< HEAD
//    when(command1.getStatement()).thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);
//    when(command1.getOverwriteProperties()).thenReturn(ImmutableMap.of("deleteTopicList", ImmutableList.of("FOO")));
//    final QueuedCommand queuedCommand = makeQueuedCommand(commandId1, command1);
//    when(commandStore.getNewCommands()).thenReturn(ImmutableList.of(queuedCommand));
//=======
//    final List<QueuedCommand> commands = Collections.singletonList(getQueuedCommands().get(0));
//    final QueuedCommand command = commands.get(0);
//    statementExecutor.handleStatement(command);
//    expectLastCall().andThrow(new RuntimeException("Something bad happened"));
//    statementExecutor.handleStatement(command);
//    expectLastCall();
//    expect(commandStore.getNewCommands()).andReturn(commands);
//    replay(statementExecutor, commandStore);
//    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);
//>>>>>>> upstream/master
//
//    // When:
//    commandRunner.fetchAndRunCommands();
//
//    //Then:
//    verify(ksqlEngine).stopAcceptingStatements();
//    verify(clusterTerminator).terminateCluster(ImmutableList.of("FOO"));
//  }
//
//<<<<<<< HEAD
////  @Test
////  @SuppressWarnings("unchecked")
////  public void shouldProcessPriorCommandsFromCommandTopic() {
////    // Given:
////    when(command1.getStatement()).thenReturn("");
////    when(commandId1.getEntity()).thenReturn("");
////    when(restoreCommands.getToRestore()).thenReturn(ImmutableMap.of(new Pair<>(1, commandId1), command1));
////    when(restoreCommands.terminatedQueries()).thenReturn(Collections.emptyMap());
////    when(restoreCommands.getDropped()).thenReturn(Collections.emptyMap());
////    when(commandStore.getRestoreCommands()).thenReturn(restoreCommands);
////
////    // When:
////    commandRunner.processPriorCommands();
////
////    // Then:
////    verify(statementExecutor).handleStatementWithTerminatedQueries(
////        eq(command1),
////        eq(commandId1),
////        any(Optional.class),
////        any(Map.class),
////        anyBoolean());
////  }
//
////  @Test
////  public void shouldTerminateIfTerminateIsInPriorCommands() {
////    // Given:
////    when(command1.getStatement()).thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);
////    when(restoreCommands.getToRestore()).thenReturn(ImmutableMap.of(new Pair<>(1, commandId1), command1));
////    when(commandStore.getRestoreCommands()).thenReturn(restoreCommands);
////
////    // When:
////    commandRunner.processPriorCommands();
////
////    //Then:
////    verify(ksqlEngine).stopAcceptingStatements();
////    verify(command1).getOverwriteProperties();
////  }
//
//
////  @Test
////  public void shouldFetchAndRunPriorCommandsFromCommandTopic() {
////    // Given:
////    final RestoreCommands restoreCommands = getRestoreCommands();
////    restoreCommands.forEach(
////        (commandId, command, terminatedQueries, wasDropped) -> {
////          statementExecutor.handleStatementWithTerminatedQueries(
////              command,
////              commandId,
////              Optional.empty(),
////              terminatedQueries,
////              wasDropped);
////          expectLastCall();
////        }
////    );
////    replay(statementExecutor);
////    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
////    replay(commandStore);
////    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 1);
////
////    // When:
////    commandRunner.processPriorCommands();
////
////    // Then:
////    verify(statementExecutor);
////  }
//
//
//  private List<QueuedCommand> givenQueuedCommands(final List<Pair<CommandId, Command>> commandIdCommandPairs ) {
//    return commandIdCommandPairs.stream()
//        .map(commandIdCommandPair ->
//            makeQueuedCommand(commandIdCommandPair.getLeft(), commandIdCommandPair.getRight()))
//        .collect(Collectors.toList());
//  }
//
//  private QueuedCommand makeQueuedCommand(final CommandId commandId, final Command command) {
//    final QueuedCommand queuedCommand = mock(QueuedCommand.class);
//    when(queuedCommand.getCommand()).thenReturn(Optional.of(command));
//    when(queuedCommand.getCommandId()).thenReturn(commandId);
//    return queuedCommand;
//=======
//  @Test
//  public void shouldGiveUpAfterRetryLimit() {
//    // Given:
//    final List<QueuedCommand> commands = Collections.singletonList(getQueuedCommands().get(0));
//    final QueuedCommand command = commands.get(0);
//    statementExecutor.handleStatement(command);
//    final RuntimeException exception = new RuntimeException("something bad happened");
//    expectLastCall().andThrow(exception).times(4);
//    expect(commandStore.getNewCommands()).andReturn(commands);
//    replay(statementExecutor, commandStore);
//    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);
//
//    // When:
//    try {
//      commandRunner.fetchAndRunCommands();
//
//      // Then:
//      fail("Should have thrown exception");
//    } catch (final RuntimeException caught) {
//      assertThat(caught, equalTo(exception));
//    }
//    verify(statementExecutor);
//  }
//
//  @Test
//  public void shouldFetchAndRunPriorCommandsFromCommandTopic() {
//    // Given:
//    final List<QueuedCommand> restoreCommands = getRestoreCommands();
//    restoreCommands.forEach(
//        command -> {
//          statementExecutor.handleRestore(command);
//          expectLastCall();
//        }
//    );
//    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
//    final Collection<PersistentQueryMetadata> persistentQueries
//        = ImmutableList.of(mock(PersistentQueryMetadata.class), mock(PersistentQueryMetadata.class));
//    expect(ksqlEngine.getPersistentQueries()).andReturn(persistentQueries);
//    persistentQueries.forEach(
//        q -> {
//          q.start();
//          expectLastCall();
//        }
//    );
//    replay(persistentQueries.toArray());
//    replay(statementExecutor, ksqlEngine, commandStore);
//    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 1);
//
//    // When:
//    commandRunner.processPriorCommands();
//
//    // Then:
//    verify(statementExecutor);
//    verify(persistentQueries.toArray());
//  }
//
//  @Test
//  public void shouldRetryCommandsWhenRestoring() {
//    // Given:
//    final List<QueuedCommand> restoreCommands = getRestoreCommands();
//    final QueuedCommand failedCommand = restoreCommands.get(0);
//    statementExecutor.handleRestore(failedCommand);
//    expectLastCall().andThrow(new RuntimeException("something bad happened"));
//    restoreCommands.forEach(
//        command -> {
//          statementExecutor.handleRestore(command);
//          expectLastCall();
//        }
//    );
//    expect(commandStore.getRestoreCommands()).andReturn(restoreCommands);
//    expect(ksqlEngine.getPersistentQueries()).andReturn(Collections.emptySet());
//    replay(statementExecutor, ksqlEngine, commandStore);
//    final CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore, 3);
//
//    // When:
//    commandRunner.processPriorCommands();
//
//    // Then:
//    verify(statementExecutor);
//>>>>>>> upstream/master
//  }
}
