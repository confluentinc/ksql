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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.server.resources.IncompatibleKsqlCommandVersionException;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.PersistentQueryCleanupImpl;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class CommandRunnerTest {
  private static final long COMMAND_RUNNER_HEALTH_TIMEOUT = 1000;
  private static final String CORRUPTED_ERROR_MESSAGE = "corrupted";
  private static final String INCOMPATIBLE_COMMANDS_ERROR_MESSAGE = "incompatible";

  @Mock
  private InteractiveStatementExecutor statementExecutor;
  @Mock
  private CommandStore commandStore;
  @Mock
  private ClusterTerminator clusterTerminator;
  @Mock
  private ServerState serverState;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private Clock clock;
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
  @Mock
  private Function<List<QueuedCommand>, List<QueuedCommand>> compactor;
  @Mock
  private Consumer<QueuedCommand> incompatibleCommandChecker;
  @Mock
  private Deserializer<Command> commandDeserializer;
  @Mock
  private Supplier<Boolean> commandTopicExists;
  @Mock
  private Errors errorHandler;
  @Mock
  private PersistentQueryMetadata queryMetadata1;
  @Mock
  private PersistentQueryMetadata queryMetadata2;
  @Mock
  private PersistentQueryMetadata queryMetadata3;
  @Mock
  private PersistentQueryCleanupImpl persistentQueryCleanupImpl;
  @Captor
  private ArgumentCaptor<Runnable> threadTaskCaptor;
  private CommandRunner commandRunner;

  @Before
  public void setup() {
    when(statementExecutor.getKsqlEngine()).thenReturn(ksqlEngine);

    when(command.getStatement()).thenReturn("something that is not terminate");
    when(clusterTerminate.getStatement())
        .thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);

    when(queuedCommand1.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    when(queuedCommand2.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    when(queuedCommand3.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    when(queuedCommand1.getAndDeserializeCommandId()).thenReturn(
        new CommandId(Type.STREAM, "foo1", Action.CREATE));
    when(queuedCommand2.getAndDeserializeCommandId()).thenReturn(
        new CommandId(Type.STREAM, "foo2", Action.CREATE));
    when(queuedCommand3.getAndDeserializeCommandId()).thenReturn(
        new CommandId(Type.STREAM, "foo3", Action.CREATE));
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand1);
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand2);
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand3);

    when(commandStore.corruptionDetected()).thenReturn(false);
    when(commandTopicExists.get()).thenReturn(true);
    when(compactor.apply(any())).thenAnswer(inv -> inv.getArgument(0));
    when(errorHandler.commandRunnerDegradedIncompatibleCommandsErrorMessage()).thenReturn(INCOMPATIBLE_COMMANDS_ERROR_MESSAGE);
    when(errorHandler.commandRunnerDegradedCorruptedErrorMessage()).thenReturn(CORRUPTED_ERROR_MESSAGE);
    
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    commandRunner = new CommandRunner(
        statementExecutor,
        commandStore,
        3,
        clusterTerminator,
        executor,
        serverState,
        "ksql-service-id",
        Duration.ofMillis(COMMAND_RUNNER_HEALTH_TIMEOUT),
        "",
        clock,
        compactor,
        incompatibleCommandChecker,
        commandDeserializer,
        errorHandler,
        commandTopicExists,
        new Metrics()
    );
  }

  @Test
  public void shouldRunThePriorCommandsCorrectly() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(queryMetadata1, queryMetadata2, queryMetadata3));

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand3));
    verify(queryMetadata1).start();
    verify(queryMetadata2).start();
    verify(queryMetadata3).start();
    verify(queryMetadata1, never()).setCorruptionQueryError();
    verify(queryMetadata2, never()).setCorruptionQueryError();
    verify(queryMetadata3, never()).setCorruptionQueryError();
  }

  @Test
  public void shouldNotStartQueriesDuringRestoreWhenCorrupted() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(queryMetadata1, queryMetadata2, queryMetadata3));
    when(commandStore.corruptionDetected()).thenReturn(true);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand3));
    verify(queryMetadata1, never()).start();
    verify(queryMetadata2, never()).start();
    verify(queryMetadata3, never()).start();
    verify(queryMetadata1).setCorruptionQueryError();
    verify(queryMetadata2).setCorruptionQueryError();
    verify(queryMetadata3).setCorruptionQueryError();
  }

  @Test
  public void shouldRunThePriorCommandsWithTerminateCorrectly() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand1.getAndDeserializeCommand(commandDeserializer)).thenReturn(clusterTerminate);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    final InOrder inOrder = inOrder(serverState, commandStore, clusterTerminator, statementExecutor);
    inOrder.verify(serverState).setTerminating();
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(clusterTerminator).terminateCluster(anyList());

    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldEarlyOutIfRestoreContainsTerminate() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand2.getAndDeserializeCommand(commandDeserializer)).thenReturn(clusterTerminate);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldCompactOnRestore() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    verify(compactor).apply(ImmutableList.of(queuedCommand1, queuedCommand2, queuedCommand3));
  }

  @Test
  public void shouldOnlyRestoreCompacted() {
    // Given:
    when(compactor.apply(any())).thenReturn(ImmutableList.of(queuedCommand1, queuedCommand3));

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand3));

    verify(statementExecutor, never()).handleRestore(queuedCommand2);
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnDeserializationExceptionInRestore() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new SerializationException()).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);
    commandRunner.start();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
    assertThat(commandRunner.getCommandRunnerDegradedReason(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND));
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnDeserializationExceptionInFetch() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new SerializationException()).when(incompatibleCommandChecker).accept(queuedCommand2);

    // When:
    commandRunner.start();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    verify(statementExecutor).handleStatement(eq(queuedCommand1));
    verify(statementExecutor, never()).handleStatement(queuedCommand2);
    verify(statementExecutor, never()).handleStatement(queuedCommand3);
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
    assertThat(commandRunner.getCommandRunnerDegradedReason(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND));
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnIncompatibleCommandInRestore() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new IncompatibleKsqlCommandVersionException("")).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);
    commandRunner.start();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
    assertThat(commandRunner.getCommandRunnerDegradedReason(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND));
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnIncompatibleCommandInFetch() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new IncompatibleKsqlCommandVersionException("")).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.start();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleStatement(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleStatement(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
    assertThat(commandRunner.getCommandRunnerDegradedReason(), is(CommandRunner.CommandRunnerDegradedReason.INCOMPATIBLE_COMMAND));
    verify(statementExecutor, never()).handleStatement(queuedCommand3);
  }

  @Test
  public void shouldNotProcessCommandTopicIfBackupCorrupted() throws InterruptedException {
    // Given:
    when(commandStore.corruptionDetected()).thenReturn(true);

    // When:
    commandRunner.start();
    verify(commandStore, never()).close();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(CORRUPTED_ERROR_MESSAGE));
    assertThat(commandRunner.getCommandRunnerDegradedReason(), is(CommandRunner.CommandRunnerDegradedReason.CORRUPTED));
  }

  @Test
  public void shouldEnterDegradedStateIfCommandTopicMissing() {
    // Given:
    givenQueuedCommands();
    when(commandTopicExists.get()).thenReturn(false);

    // When:
    commandRunner.start();

    final Runnable threadTask = getThreadTask();
    threadTask.run();

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    assertThat(commandRunner.getCommandRunnerDegradedWarning(), is(CORRUPTED_ERROR_MESSAGE));
    assertThat(
        commandRunner.getCommandRunnerDegradedReason(),
        is(CommandRunner.CommandRunnerDegradedReason.CORRUPTED));
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
  public void shouldRetryOnException() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2);
    doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doNothing().when(statementExecutor).handleStatement(queuedCommand2);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor, times(1)).handleStatement(queuedCommand1);
    inOrder.verify(statementExecutor, times(3)).handleStatement(queuedCommand2);
  }

  @Test
  public void shouldThrowExceptionIfOverMaxRetries() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2);
    doThrow(new RuntimeException()).when(statementExecutor).handleStatement(queuedCommand2);

    // When:
    assertThrows(
        RuntimeException.class,
        () -> commandRunner.fetchAndRunCommands()
    );
  }

  @Test
  public void shouldEarlyOutIfNewCommandsContainsTerminate() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand2.getAndDeserializeCommand(commandDeserializer)).thenReturn(clusterTerminate);

    // When:
    commandRunner.fetchAndRunCommands();

    // Then:
    verify(statementExecutor, never()).handleRestore(queuedCommand1);
    verify(statementExecutor, never()).handleRestore(queuedCommand2);
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
  }

  @Test
  public void shouldTransitionFromRunningToErrorWhenStuckOnCommand() throws InterruptedException {
    // Given:
    givenQueuedCommands(queuedCommand1);

    final Instant current = Instant.now();
    final CountDownLatch handleStatementLatch = new CountDownLatch(1);
    final CountDownLatch commandSetLatch = new CountDownLatch(1);
    when(clock.instant()).thenReturn(current)
        .thenReturn(current.plusMillis(500))
        .thenReturn(current.plusMillis(500))
        .thenReturn(current.plusMillis(1500))
        .thenReturn(current.plusMillis(2500));
    doAnswer(invocation -> {
      commandSetLatch.countDown();
      handleStatementLatch.await();
      return null;
    }).when(statementExecutor).handleStatement(queuedCommand1);

    // When:
    final AtomicReference<Exception> expectedException = new AtomicReference<>(null);
    final Thread commandRunnerThread = (new Thread(() -> {
      try {
        commandRunner.fetchAndRunCommands();
      } catch (final Exception e) {
        expectedException.set(e);
      }
    }));

    // Then:
    commandRunnerThread.start();
    commandSetLatch.await();
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.RUNNING));
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.ERROR));
    handleStatementLatch.countDown();
    commandRunnerThread.join();
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.RUNNING));
    assertThat(expectedException.get(), equalTo(null));
  }

  @Test
  public void shouldTransitionFromRunningToErrorWhenNotPollingCommandTopic() {
    // Given:
    givenQueuedCommands();

    final Instant current = Instant.now();
    when(clock.instant()).thenReturn(current)
        .thenReturn(current.plusMillis(15100));

    // Then:
    commandRunner.fetchAndRunCommands();
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.ERROR));
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
    inOrder.verify(executor).execute(any(Runnable.class));
    inOrder.verify(executor).shutdown();
  }

  @Test
  public void shouldNotStartCommandRunnerThreadIfSerializationExceptionInRestore() throws Exception {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new SerializationException()).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.processPriorCommands(persistentQueryCleanupImpl);
    commandRunner.start();

    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
    verify(commandStore, never()).getNewCommands(any());
    verify(statementExecutor, times(2)).handleRestore(any());
  }

  @Test
  public void shouldCloseEarlyWhenSerializationExceptionInFetch() throws Exception {
    // Given:
    when(commandStore.getNewCommands(any()))
        .thenReturn(Collections.singletonList(queuedCommand1))
        .thenReturn(Collections.singletonList(queuedCommand2));
    doThrow(new SerializationException()).when(incompatibleCommandChecker).accept(queuedCommand2);
    
    // When:
    commandRunner.start();
    verify(commandStore, never()).close();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
  }
  
  @Test
  public void shouldCloseEarlyOnTerminate() throws InterruptedException {
    // Given:
    when(commandStore.getNewCommands(any())).thenReturn(Collections.singletonList(queuedCommand1));
    when(queuedCommand1.getAndDeserializeCommand(commandDeserializer)).thenReturn(clusterTerminate);

    // When:
    commandRunner.start();
    verify(commandStore, never()).close();
    final Runnable threadTask = getThreadTask();
    threadTask.run();
    
    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
  }

  @Test
  public void shouldCloseEarlyWhenOffsetOutOfRangeException() throws Exception {
    // Given:
    when(commandStore.getNewCommands(any()))
        .thenReturn(Collections.singletonList(queuedCommand1))
        .thenThrow(new OffsetOutOfRangeException(Collections.singletonMap(new TopicPartition("command_topic", 0), 0L)));

    // When:
    commandRunner.start();
    verify(commandStore, never()).close();
    final Runnable threadTask = getThreadTask();
    threadTask.run();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore).close();
  }

  @Test
  public void shouldCloseTheCommandRunnerCorrectly() throws Exception {
    // Given:
    commandRunner.start();

    // When:
    commandRunner.close();

    // Then:
    final InOrder inOrder = inOrder(executor, commandStore);
    inOrder.verify(commandStore).wakeup();
    inOrder.verify(executor).awaitTermination(anyLong(), any());
    inOrder.verify(commandStore, never()).close(); // commandStore must be closed by runner thread

    final Runnable threadTask = getThreadTask();
    threadTask.run();

    verify(commandStore).close();
  }

  private Runnable getThreadTask() {
    verify(executor).execute(threadTaskCaptor.capture());
    return threadTaskCaptor.getValue();
  }

  private void givenQueuedCommands(final QueuedCommand... cmds) {
    when(commandStore.getRestoreCommands()).thenReturn(Arrays.asList(cmds));
    when(commandStore.getNewCommands(any())).thenReturn(Arrays.asList(cmds));
  }

  private Answer<?> closeRunner() {
    return inv -> {
      commandRunner.close();
      return null;
    };
  }
}
