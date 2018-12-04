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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(
      Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
  private static final Map<String, Object> OVERRIDE_PROPERTIES =
      Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
  private static final long TIMEOUT = 1000;
  private static final String statementText = "test-statement";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Consumer<CommandId, Command> commandConsumer;
  @Mock
  private Producer<CommandId, Command> commandProducer;
  @Mock
  private SequenceNumberFutureStore sequenceNumberFutureStore;
  @Mock
  private CompletableFuture<Void> future;
  @Mock
  private Statement statement;
  @Mock
  private Future<RecordMetadata> recordMetadataFuture;
  @Mock
  private Node node;
  @Mock
  private CommandIdAssigner commandIdAssigner;

  private final CommandId commandId =
      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
  private final Command command =
      new Command(statementText, Collections.emptyMap(), Collections.emptyMap());
  private final RecordMetadata recordMetadata = new RecordMetadata(
      new TopicPartition("topic", 0), 0, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0);

  private CommandStore commandStore;

  @Before
  public void setUp() throws Exception {
    when(commandIdAssigner.getCommandId(any())).thenReturn(commandId);

    when(commandProducer.send(any(ProducerRecord.class))).thenReturn(recordMetadataFuture);
    when(recordMetadataFuture.get()).thenReturn(recordMetadata);

    when(commandConsumer.poll(any(Duration.class))).thenReturn(buildRecords(commandId, command));
    when(commandConsumer.partitionsFor(COMMAND_TOPIC))
        .thenReturn(ImmutableList.of(
            new PartitionInfo(COMMAND_TOPIC, 0, node, new Node[]{node}, new Node[]{node})
        ));

    when(sequenceNumberFutureStore.getFutureForSequenceNumber(anyLong())).thenReturn(future);

    commandStore = new CommandStore(
        COMMAND_TOPIC,
        commandConsumer,
        commandProducer,
        commandIdAssigner,
        sequenceNumberFutureStore
    );
  }

  @Test
  public void shouldHaveAllCreateCommandsInOrder() {
    // Given:
    final CommandId createId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final CommandId dropId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.DROP);
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final Command originalCommand = new Command(
        "some statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final Command dropCommand = new Command(
        "drop", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final Command latestCommand = new Command(
        "a new statement", Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final ConsumerRecords<CommandId, Command> records = buildRecords(
        createId, originalCommand,
        dropId, dropCommand,
        createId, latestCommand
    );

    when(commandConsumer.poll(any()))
        .thenReturn(records)
        .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    // When:
    final List<Pair<CommandId, Command>> commands = getPriorCommands(commandStore);

    // Then:
    assertThat(commands, equalTo(Arrays.asList(new Pair<>(createId, originalCommand),
        new Pair<>(dropId, dropCommand),
        new Pair<>(createId, latestCommand))));
  }

  @Test
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    expectedException.expect(IllegalStateException.class);

    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(commandProducer.send(any(ProducerRecord.class)))
        .thenThrow(new RuntimeException("oops"))
        .thenReturn(recordMetadataFuture);
    try {
      commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);
      fail("enqueueCommand should have raised an exception");
    } catch (final KsqlException e) {
    }

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then: condition being verified is that the above call doesn't throw
  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);
    commandStore.getNewCommands();

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then: condition being verified is that the above call doesn't throw
  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(commandProducer.send(any(ProducerRecord.class))).thenAnswer(
        invocation -> {
          final QueuedCommand queuedCommand = commandStore.getNewCommands().get(0);
          assertThat(queuedCommand.getCommandId(), equalTo(commandId));
          assertThat(queuedCommand.getStatus().isPresent(), equalTo(true));
          assertThat(
              queuedCommand.getStatus().get().getStatus().getStatus(),
              equalTo(CommandStatus.Status.QUEUED));
          return recordMetadataFuture;
        }
    );

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    // verifying the commandProducer also verifies the assertions in its Answer were run
    verify(commandProducer).send(any(ProducerRecord.class));
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    final ConsumerRecords<CommandId, Command> records = buildRecords(
        commandId, null,
        commandId, command);
    when(commandConsumer.poll(any())).thenReturn(records);

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands();

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(commandId));
    assertThat(commands.get(0).getCommand(), equalTo(command));
  }

  @Test
  public void shouldFilterNullPriorCommand() {
    // Given:
    final CommandId id = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final Command command = new Command(
        "some statement", Collections.emptyMap(), Collections.emptyMap());
    final ConsumerRecords<CommandId, Command> records = buildRecords(
        id, null,
        id, command);
    when(commandConsumer.poll(any()))
        .thenReturn(records)
        .thenReturn(ConsumerRecords.empty());

    // When:
    final List<QueuedCommand> commands = commandStore.getRestoreCommands();

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(id));
    assertThat(commands.get(0).getCommand(), equalTo(command));
  }

  @Test
  public void shouldDistributeCommand() {
    ArgumentCaptor<ProducerRecord<CommandId, Command>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    when(commandProducer.send(recordCaptor.capture())).thenReturn(recordMetadataFuture);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    final ProducerRecord<CommandId, Command> record = recordCaptor.getValue();
    assertThat(record.key(), equalTo(commandId));
    assertThat(record.value().getStatement(), equalTo(statementText));
    assertThat(record.value().getOverwriteProperties(), equalTo(OVERRIDE_PROPERTIES));
    assertThat(record.value().getOriginalProperties(),
        equalTo(KSQL_CONFIG.getAllConfigPropsWithSecretsObfuscated()));
  }

  @Test
  public void shouldIncludeCommandSequenceNumberInSuccessfulQueuedCommandStatus() {
    // When:
    final QueuedCommandStatus commandStatus =
        commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    assertThat(commandStatus.getCommandSequenceNumber(), equalTo(recordMetadata.offset()));
  }

  @Test
  public void shouldWaitOnSequenceNumberFuture() throws Exception {
    // When:
    commandStore.ensureConsumedUpThrough(2, TIMEOUT);

    // Then:
    verify(future).get(eq(TIMEOUT), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void shouldThrowExceptionOnTimeout() throws Exception {
    // Given:
    expectedException.expect(TimeoutException.class);
    expectedException.expectMessage(String.format(
        "Timeout reached while waiting for command sequence number of 2. (Timeout: %d ms)", TIMEOUT));

    when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException());

    // When:
    commandStore.ensureConsumedUpThrough(2, TIMEOUT);
  }

  @Test
  public void shouldCompleteFuturesWhenGettingNewCommands() {
    // When:
    commandStore.getNewCommands();

    // Then:
    verify(sequenceNumberFutureStore).completeFuturesUpThroughSequenceNumber(eq(-1L));
  }

  private static List<Pair<CommandId, Command>> getPriorCommands(final CommandStore commandStore) {
    return commandStore.getRestoreCommands().stream()
        .map(
            queuedCommand -> new Pair<>(
                queuedCommand.getCommandId(), queuedCommand.getCommand()))
        .collect(Collectors.toList());
  }

  private static ConsumerRecords<CommandId, Command> buildRecords(final Object ...args) {
    assertThat(args.length % 2, equalTo(0));
    final List<ConsumerRecord<CommandId, Command>> records = new ArrayList<>();
    for (int i = 0; i < args.length; i += 2) {
      assertThat(args[i], instanceOf(CommandId.class));
      assertThat(args[i + 1], anyOf(is(nullValue()), instanceOf(Command.class)));
      records.add(
          new ConsumerRecord<>("topic", 0, 0, (CommandId) args[i], (Command) args[i + 1]));
    }
    return new ConsumerRecords<>(
        Collections.singletonMap(
            new TopicPartition("topic", 0),
            records
        )
    );
  }
}
