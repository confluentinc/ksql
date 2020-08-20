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
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.rest.server.resources.IncomaptibleKsqlCommandVersionException;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.confluent.ksql.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
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
  @Captor
  private ArgumentCaptor<Runnable> threadTaskCaptor;
  private CommandRunner commandRunner;

  @Before
  public void setup() {
    MetricCollectors.initialize();
    when(statementExecutor.getKsqlEngine()).thenReturn(ksqlEngine);

    when(command.getStatement()).thenReturn("something that is not terminate");
    when(clusterTerminate.getStatement())
        .thenReturn(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT);

    when(queuedCommand1.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    when(queuedCommand2.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    when(queuedCommand3.getAndDeserializeCommand(commandDeserializer)).thenReturn(command);
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand1);
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand2);
    doNothing().when(incompatibleCommandChecker).accept(queuedCommand3);

    when(compactor.apply(any())).thenAnswer(inv -> inv.getArgument(0));

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
        commandDeserializer
    );
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
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    when(queuedCommand1.getAndDeserializeCommand(commandDeserializer)).thenReturn(clusterTerminate);

    // When:
    commandRunner.processPriorCommands();

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
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor, never()).handleRestore(any());
  }

  @Test
  public void shouldCompactOnRestore() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(compactor).apply(ImmutableList.of(queuedCommand1, queuedCommand2, queuedCommand3));
  }

  @Test
  public void shouldOnlyRestoreCompacted() {
    // Given:
    when(compactor.apply(any())).thenReturn(ImmutableList.of(queuedCommand1, queuedCommand3));

    // When:
    commandRunner.processPriorCommands();

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
    commandRunner.processPriorCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
    
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnDeserializationExceptionInFetch() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new SerializationException()).when(incompatibleCommandChecker).accept(queuedCommand2);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    verify(statementExecutor).handleRestore(eq(queuedCommand1));
    verify(statementExecutor, never()).handleRestore(queuedCommand2);
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnIncomaptibleCommandInRestore() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new IncomaptibleKsqlCommandVersionException("")).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
  }

  @Test
  public void shouldProcessPartialListOfCommandsOnIncomaptibleCommandInFetch() {
    // Given:
    givenQueuedCommands(queuedCommand1, queuedCommand2, queuedCommand3);
    doThrow(new IncomaptibleKsqlCommandVersionException("")).when(incompatibleCommandChecker).accept(queuedCommand3);

    // When:
    commandRunner.processPriorCommands();

    // Then:
    final InOrder inOrder = inOrder(statementExecutor);
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand1));
    inOrder.verify(statementExecutor).handleRestore(eq(queuedCommand2));

    assertThat(commandRunner.checkCommandRunnerStatus(), is(CommandRunner.CommandRunnerStatus.DEGRADED));
    verify(statementExecutor, never()).handleRestore(queuedCommand3);
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
    commandRunner.processPriorCommands();
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
