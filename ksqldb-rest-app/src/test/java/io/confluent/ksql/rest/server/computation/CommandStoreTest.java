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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.rest.server.CommandTopicBackup;
import io.confluent.ksql.util.KsqlException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC_NAME = "command";
  private static final TopicPartition COMMAND_TOPIC_PARTITION =
      new TopicPartition(COMMAND_TOPIC_NAME, 0);
  private static final Duration TIMEOUT = Duration.ofMillis(1000);
  private static final String statementText = "test-statement";
  private static final Duration NEW_CMDS_TIMEOUT = Duration.ofSeconds(30);

  @Mock
  private SequenceNumberFutureStore sequenceNumberFutureStore;
  @Mock
  private CompletableFuture<Void> future;
  @Mock
  private CommandTopic commandTopic;
  @Mock
  private Producer<CommandId, Command> transactionalProducer;
  @Mock
  private Serializer<CommandId> commandIdSerializer;
  @Mock
  private Serializer<Command> commandSerializer;
  @Mock
  private Deserializer<CommandId> commandIdDeserializer;
  @Mock
  private CommandTopicBackup commandTopicBackup;

  private final CommandId commandId =
      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
  private final Command command = new Command(
      statementText,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Optional.empty()
  );
  private final RecordMetadata recordMetadata = new RecordMetadata(
      COMMAND_TOPIC_PARTITION, 0, 0, RecordBatch.NO_TIMESTAMP, 0, 0);

  private final Future<RecordMetadata> testFuture = new Future<RecordMetadata>() {
    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public RecordMetadata get() {
      return recordMetadata;
    }

    @Override
    public RecordMetadata get(final long timeout, @NotNull final TimeUnit unit) {
      return null;
    }
  };

  private CommandStore commandStore;

  @Before
  public void setUp() {
    when(transactionalProducer.send(any(ProducerRecord.class))).thenReturn(testFuture);

    when(commandTopic.getNewCommands(any())).thenReturn(buildRecords(commandId, command));

    when(commandTopic.getCommandTopicName()).thenReturn(COMMAND_TOPIC_NAME);

    when(sequenceNumberFutureStore.getFutureForSequenceNumber(anyLong())).thenReturn(future);

    commandStore = new CommandStore(
        COMMAND_TOPIC_NAME,
        commandTopic,
        sequenceNumberFutureStore,
        Collections.emptyMap(),
        Collections.emptyMap(),
        TIMEOUT,
        commandIdSerializer,
        commandSerializer,
        commandIdDeserializer,
        commandTopicBackup
    );
  }

  @Test
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);

    // When:
    assertThrows(
        IllegalStateException.class,
        () -> commandStore.enqueueCommand(commandId, command, transactionalProducer)
    );
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(transactionalProducer.send(any(ProducerRecord.class)))
        .thenThrow(new RuntimeException("oops"))
        .thenReturn(testFuture);

    final Exception e = assertThrows(
        KsqlException.class,
        () -> commandStore.enqueueCommand(commandId, command, transactionalProducer)
    );

    assertThat(e.getMessage(), containsString(
        "Could not write the statement into the command topic"
    ));

    // When:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);

    // Then: did not throw.
  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    when(commandIdDeserializer.deserialize(any(), any())).thenReturn(commandId);
    commandStore.enqueueCommand(commandId, command, transactionalProducer);
    commandStore.getNewCommands(NEW_CMDS_TIMEOUT);

    // Should:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);
  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(transactionalProducer.send(any(ProducerRecord.class))).thenAnswer(
        invocation -> {
          final QueuedCommand command = commandStore.getNewCommands(NEW_CMDS_TIMEOUT).get(0);
          assertThat(
              command.getAndDeserializeCommandId(),
              equalTo(commandId));
          assertThat(command.getStatus().isPresent(), equalTo(true));
          assertThat(
              command.getStatus().get().getStatus().getStatus(),
              equalTo(CommandStatus.Status.QUEUED));
          assertThat(command.getOffset(), equalTo(0L));
          return testFuture;
        }
    );
    when(commandIdDeserializer.deserialize(any(),any())).thenReturn(commandId);

    // When:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);

    // Then:
    verify(transactionalProducer).send(any(ProducerRecord.class));
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    final ConsumerRecords<byte[], byte[]> records = buildRecords(
        commandId, null,
        commandId, command);
    final Deserializer<Command> commandDeserializer = mock(Deserializer.class);
    when(commandDeserializer.deserialize(any(), any())).thenReturn(command);
    when(commandTopic.getNewCommands(any())).thenReturn(records);

    // When:	
    final List<QueuedCommand> commands =
        commandStore.getNewCommands(NEW_CMDS_TIMEOUT);

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(
        commands.get(0).getAndDeserializeCommandId(),
        equalTo(commandId));
    assertThat(
        commands.get(0).getAndDeserializeCommand(commandDeserializer),
        equalTo(command));
  }

  @Test
  public void shouldDistributeCommand() {
    when(transactionalProducer.send(any(ProducerRecord.class))).thenReturn(testFuture);

    // When:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);

    // Then:
    verify(transactionalProducer).send(new ProducerRecord<>(
        COMMAND_TOPIC_NAME,
        COMMAND_TOPIC_PARTITION.partition(),
        commandId,
        command
    ));
  }

  @Test
  public void shouldIncludeCommandSequenceNumberInSuccessfulQueuedCommandStatus() {
    // When:
    final QueuedCommandStatus commandStatus =
        commandStore.enqueueCommand(commandId, command, transactionalProducer);

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

    // When:
    final TimeoutException e = assertThrows(
        TimeoutException.class,
        () -> commandStore.ensureConsumedPast(2, TIMEOUT)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Timeout reached while waiting for command sequence number of 2. "
            + "Caused by: null (Timeout: 1000 ms)"
    ));
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
    assertThat(commandStore.getCommandTopicName(), equalTo(COMMAND_TOPIC_NAME));
  }

  @Test
  public void shouldStartCommandTopicOnStart() {
    // When:
    commandStore.start();

    // Then:
    verify(commandTopic).start();
  }

  @Test
  public void shouldSuccessfullyAbortAndRetry() {
    // Given:
    commandStore.enqueueCommand(commandId, command, transactionalProducer);

    // When/Then:
    commandStore.abortCommand(commandId);
    commandStore.enqueueCommand(commandId, command, transactionalProducer);
  }

  private static ConsumerRecords<byte[], byte[]> buildRecords(final Object... args) {
    assertThat(args.length % 2, equalTo(0));
    final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < args.length; i += 2) {
      assertThat(args[i], instanceOf(CommandId.class));
      assertThat(args[i + 1], anyOf(is(nullValue()), instanceOf(Command.class)));

      records.add(
          new ConsumerRecord<>(
              COMMAND_TOPIC_NAME,
              0,
              0,
              InternalTopicSerdes.serializer().serialize(null, args[i]),
              args[i + 1] == null ? null : InternalTopicSerdes.serializer().serialize(null, args[i + 1])));
    }
    return new ConsumerRecords<>(Collections.singletonMap(COMMAND_TOPIC_PARTITION, records));
  }
}
