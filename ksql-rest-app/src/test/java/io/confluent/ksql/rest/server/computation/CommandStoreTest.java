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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  private static final TopicPartition COMMAND_TOPIC_PARTITION =
      new TopicPartition(COMMAND_TOPIC, 0);
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(
      Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
  private static final Map<String, Object> OVERRIDE_PROPERTIES =
      Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
  private static final Duration TIMEOUT = Duration.ofMillis(1000);
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final String statementText = "test-statement";
  private static final Duration NEW_CMDS_TIMEOUT = Duration.ofSeconds(30);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SequenceNumberFutureStore sequenceNumberFutureStore;
  @Mock
  private CompletableFuture<Void> future;
  @Mock
  private CommandTopic commandTopic;
  @Mock
  private Statement statement;
  @Mock
  private CommandIdAssigner commandIdAssigner;
  private ConfiguredStatement<?> configured;

  private final CommandId commandId =
      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
  private final Command command =
      new Command(statementText, true, Collections.emptyMap(), Collections.emptyMap());
  private final RecordMetadata recordMetadata = new RecordMetadata(
      COMMAND_TOPIC_PARTITION, 0, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0);

  private CommandStore commandStore;

  @Before
  public void setUp() {
    when(commandIdAssigner.getCommandId(any()))
        .thenAnswer(invocation -> new CommandId(
            CommandId.Type.STREAM, "foo" + COUNTER.getAndIncrement(), CommandId.Action.CREATE));

    when(commandTopic.send(any(), any())).thenReturn(recordMetadata);

    when(commandTopic.getNewCommands(any())).thenReturn(buildRecords(commandId, command));

    when(commandTopic.getCommandTopicName()).thenReturn(COMMAND_TOPIC);

    when(sequenceNumberFutureStore.getFutureForSequenceNumber(anyLong())).thenReturn(future);

    configured = ConfiguredStatement.of(
        PreparedStatement.of(statementText, statement), OVERRIDE_PROPERTIES, KSQL_CONFIG);

    commandStore = new CommandStore(
        commandTopic,
        commandIdAssigner,
        sequenceNumberFutureStore
    );
  }

  @Test
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId);
    commandStore.enqueueCommand(configured);

    expectedException.expect(IllegalStateException.class);

    // When:
    commandStore.enqueueCommand(configured);
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(commandTopic.send(any(), any()))
        .thenThrow(new RuntimeException("oops"))
        .thenReturn(recordMetadata);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not write the statement 'test-statement' into the command topic.");
    commandStore.enqueueCommand(configured);

    // When:
    commandStore.enqueueCommand(configured);
  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId);
    commandStore.enqueueCommand(configured);
    commandStore.getNewCommands(NEW_CMDS_TIMEOUT);

    // Should:
    commandStore.enqueueCommand(configured);
  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId);
    when(commandTopic.send(any(), any())).thenAnswer(
        invocation -> {
          final QueuedCommand queuedCommand = commandStore.getNewCommands(NEW_CMDS_TIMEOUT).get(0);
          assertThat(queuedCommand.getCommandId(), equalTo(commandId));
          assertThat(queuedCommand.getStatus().isPresent(), equalTo(true));
          assertThat(
              queuedCommand.getStatus().get().getStatus().getStatus(),
              equalTo(CommandStatus.Status.QUEUED));
          assertThat(queuedCommand.getOffset(), equalTo(0L));
          return recordMetadata;
        }
    );

    // When:
    commandStore.enqueueCommand(configured);

    // Then:
    verify(commandTopic).send(any(), any());
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    final ConsumerRecords<CommandId, Command> records = buildRecords(
        commandId, null,
        commandId, command);
    when(commandTopic.getNewCommands(any())).thenReturn(records);

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands(NEW_CMDS_TIMEOUT);

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(commandId));
    assertThat(commands.get(0).getCommand(), equalTo(command));
  }


  @Test
  public void shouldDistributeCommand() {
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId);
    when(commandTopic.send(any(), any())).thenReturn(recordMetadata);

    // When:
    commandStore.enqueueCommand(configured);

    // Then:
    verify(commandTopic).send(same(commandId), any());
  }

  @Test
  public void shouldIncludeCommandSequenceNumberInSuccessfulQueuedCommandStatus() {
    // When:
    final QueuedCommandStatus commandStatus =
        commandStore.enqueueCommand(configured);

    // Then:
    assertThat(commandStatus.getCommandSequenceNumber(), equalTo(recordMetadata.offset()));
  }

  @Test
  public void shouldWaitOnSequenceNumberFuture() throws Exception {
    // When:
    commandStore.ensureConsumedPast(2, TIMEOUT);

    // Then:
    verify(future).get(eq(TIMEOUT.toMillis()), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void shouldThrowExceptionOnTimeout() throws Exception {
    // Given:
    when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException());

    expectedException.expect(TimeoutException.class);
    expectedException.expectMessage(String.format(
        "Timeout reached while waiting for command sequence number of 2. (Timeout: %d ms)",
        TIMEOUT.toMillis()));

    // When:
    commandStore.ensureConsumedPast(2, TIMEOUT);
  }

  @Test
  public void shouldCompleteFuturesWhenGettingNewCommands() {
    // Given:
    when(commandTopic.getCommandTopicConsumerPosition()).thenReturn(22L);

    // When:
    commandStore.getNewCommands(NEW_CMDS_TIMEOUT);

    // Then:
    final InOrder inOrder = inOrder(sequenceNumberFutureStore, commandTopic);
    inOrder.verify(sequenceNumberFutureStore)
        .completeFuturesUpToAndIncludingSequenceNumber(eq(21L));
    inOrder.verify(commandTopic).getNewCommands(any());
  }

  @Test
  public void shouldComputeNotEmptyCorrectly() {
    // Given:
    when(commandTopic.getEndOffset()).thenReturn(1L);

    // When/Then:
    assertThat(commandStore.isEmpty(), is(false));
  }

  @Test
  public void shouldComputeEmptyCorrectly() {
    // Given:
    when(commandTopic.getEndOffset()).thenReturn(0L);

    // When/Then:
    assertThat(commandStore.isEmpty(), is(true));
  }

  @Test
  public void shouldWakeUp() {
    // When:
    commandStore.wakeup();

    // Then:
    verify(commandTopic).wakeup();
  }

  @Test
  public void shouldClose() {
    // When:
    commandStore.close();

    // Then:
    verify(commandTopic).close();
  }

  @Test
  public void shouldGetCommandTopicName() {
    assertThat(commandStore.getCommandTopicName(), equalTo(COMMAND_TOPIC));
  }

  @Test
  public void shouldStartCommandTopicOnStart() {
    // When:
    commandStore.start();

    // Then:
    verify(commandTopic).start();
  }

  private static ConsumerRecords<CommandId, Command> buildRecords(final Object... args) {
    assertThat(args.length % 2, equalTo(0));
    final List<ConsumerRecord<CommandId, Command>> records = new ArrayList<>();
    for (int i = 0; i < args.length; i += 2) {
      assertThat(args[i], instanceOf(CommandId.class));
      assertThat(args[i + 1], anyOf(is(nullValue()), instanceOf(Command.class)));
      records.add(
          new ConsumerRecord<>(COMMAND_TOPIC, 0, 0, (CommandId) args[i], (Command) args[i + 1]));
    }
    return new ConsumerRecords<>(Collections.singletonMap(COMMAND_TOPIC_PARTITION, records));
  }
}
