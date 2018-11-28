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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
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
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());
  private static final Map<String, Object> OVERRIDE_PROPERTIES = Collections.emptyMap();
  private static final long TIMEOUT = 1000;

  private final Consumer<CommandId, Command> commandConsumer = niceMock(Consumer.class);
  private final Producer<CommandId, Command> commandProducer = mock(Producer.class);
  private CommandIdAssigner commandIdAssigner =
      new CommandIdAssigner(new MetaStoreImpl(new InternalFunctionRegistry()));
  private final SequenceNumberFutureStore sequenceNumberFutureStore = mock(
      SequenceNumberFutureStore.class);
  private final CompletableFuture<Void> future = niceMock(CompletableFuture.class);
  private final String statementText = "test-statement";
  private final CommandId commandId =
      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
  private final Statement statement = mock(Statement.class);
  private final Future<RecordMetadata> recordMetadataFuture = niceMock(Future.class);
  private final Command command =
      new Command(statementText, Collections.emptyMap(), Collections.emptyMap());
  private final Node node = mock(Node.class);
  private final RecordMetadata recordMetadata = new RecordMetadata(
      new TopicPartition("topic", 0), 0, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0);

  private CommandStore commandStore;

  @Before
  public void setUp() throws Exception {
    expect(recordMetadataFuture.get()).andReturn(recordMetadata);
    replay(recordMetadataFuture);

    setUpCommandStore();
  }

  @Test
  public void shouldHaveAllCreateCommandsInOrder() {
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

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyObject())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final List<Pair<CommandId, Command>> commands = getPriorCommands(commandStore);
    assertThat(commands, equalTo(Arrays.asList(new Pair<>(createId, originalCommand),
        new Pair<>(dropId, dropCommand),
        new Pair<>(createId, latestCommand))));
  }

  @Test
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    givenCommandStoreThatAssignsSameId(commandId);

    // Given:
    expect(commandProducer.send(anyObject())).andReturn(recordMetadataFuture);
    replay(commandProducer);
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    try {
      // When:
      commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

      // Then:
      fail("Second enqueue call should throw IllegalStateException");
    } catch (final IllegalStateException e) {
    }
  }

  @Test
  public void shouldCleanupCommandStatusOnProduceError() {
    givenCommandStoreThatAssignsSameId(commandId);

    // Given:
    expect(commandProducer.send(anyObject())).andThrow(new RuntimeException("oops")).times(1);
    expect(commandProducer.send(anyObject())).andReturn(recordMetadataFuture).times(1);
    replay(commandProducer);
    try {
      commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);
      fail("enqueueCommand should have raised an exception");
    } catch (final KsqlException e) {
    }

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    // main condition being verified is that the above call doesn't throw
    verify(commandProducer);
  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() throws Exception {
    givenCommandStoreThatAssignsSameId(commandId);

    // Given:
    setupConsumerToReturnCommand(commandId, command);
    expect(commandProducer.send(anyObject(ProducerRecord.class))).andAnswer(
        () -> {
          commandStore.getNewCommands();
          return recordMetadataFuture;
        }
    ).times(1);
    expect(commandProducer.send(anyObject(ProducerRecord.class))).andReturn(recordMetadataFuture);
    reset(recordMetadataFuture);
    expect(recordMetadataFuture.get()).andReturn(recordMetadata).times(2);
    replay(recordMetadataFuture, commandProducer);
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    verify(recordMetadataFuture, commandProducer, commandConsumer);
  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    givenCommandStoreThatAssignsSameId(commandId);

    // Given:
    setupConsumerToReturnCommand(commandId, command);
    expect(commandProducer.send(anyObject(ProducerRecord.class))).andAnswer(
        () -> {
          final QueuedCommand queuedCommand = commandStore.getNewCommands().get(0);
          assertThat(queuedCommand.getCommandId(), equalTo(commandId));
          assertThat(queuedCommand.getStatus().isPresent(), equalTo(true));
          assertThat(
              queuedCommand.getStatus().get().getStatus().getStatus(),
              equalTo(CommandStatus.Status.QUEUED));
          return recordMetadataFuture;
        }
    );
    replay(commandProducer);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    // verifying the commandProducer also verifies the assertions in its IAnswer were run
    verify(recordMetadataFuture, commandProducer, commandConsumer);
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    final CommandId id = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final Command command = new Command(
        "some statement", Collections.emptyMap(), Collections.emptyMap());
    final ConsumerRecords<CommandId, Command> records = buildRecords(
        id, null,
        id, command);
    expect(commandConsumer.poll(anyObject())).andReturn(records);
    replay(commandConsumer);

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands();

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(id));
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
    expect(commandConsumer.partitionsFor(COMMAND_TOPIC))
        .andReturn(ImmutableList.of(
            new PartitionInfo(COMMAND_TOPIC, 0, node, new Node[]{node}, new Node[]{node})
        ));
    expect(commandConsumer.poll(anyObject())).andReturn(records);
    expect(commandConsumer.poll(anyObject())).andReturn(ConsumerRecords.empty());
    replay(commandConsumer);

    // When:
    final List<QueuedCommand> commands = commandStore.getRestoreCommands();

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(id));
    assertThat(commands.get(0).getCommand(), equalTo(command));
  }

  @Test
  public void shouldDistributeCommand() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
    final Map<String, Object> overrideProperties = Collections.singletonMap(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final String statementText = "test-statement";

    final Statement statement = mock(Statement.class);
    final Capture<ProducerRecord<CommandId, Command>> recordCapture = Capture.newInstance();

    expect(commandProducer.send(capture(recordCapture))).andReturn(recordMetadataFuture);
    replay(commandProducer);

    givenCommandStoreThatAssignsSameId(commandId);
    commandStore.enqueueCommand(statementText, statement, ksqlConfig, overrideProperties);

    verify(commandProducer, recordMetadataFuture);

    final ProducerRecord<CommandId, Command> record = recordCapture.getValue();
    assertThat(record.key(), equalTo(commandId));
    assertThat(record.value().getStatement(), equalTo(statementText));
    assertThat(record.value().getOverwriteProperties(), equalTo(overrideProperties));
    assertThat(record.value().getOriginalProperties(), equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
  }

  @Test
  public void shouldIncludeCommandSequenceNumberInSuccessfulQueuedCommandStatus() {
    // Given:
    givenCommandStoreThatAssignsSameId(commandId);

    expect(commandProducer.send(anyObject(ProducerRecord.class))).andReturn(recordMetadataFuture);
    replay(commandProducer);

    // When:
    final QueuedCommandStatus commandStatus =
        commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    assertThat(commandStatus.getCommandSequenceNumber(), equalTo(recordMetadata.offset()));

    verify(commandProducer, recordMetadataFuture);
  }

  @Test
  public void shouldNotWaitIfSequenceNumberReached() throws Exception {
    // Given:
    givenCmdStoreUpThroughPosition(1);
    expect(sequenceNumberFutureStore.getFutureForSequenceNumber(EasyMock.anyLong()))
        .andThrow(new AssertionError()).anyTimes();
    replay(sequenceNumberFutureStore);

    // When:
    commandStore.ensureConsumedUpThrough(0, TIMEOUT);

    // Then:
    verify(commandConsumer, sequenceNumberFutureStore);
  }

  @Test
  public void shouldWaitIfSequenceNumberNotReached() throws Exception {
    // Given:
    givenCmdStoreUpThroughPosition(2);
    expect(sequenceNumberFutureStore.getFutureForSequenceNumber(EasyMock.anyLong())).andReturn(future);
    expect(future.get(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class))).andReturn(null);
    replay(future, sequenceNumberFutureStore);

    // When:
    commandStore.ensureConsumedUpThrough(2, TIMEOUT);

    // Then:
    verify(commandConsumer, sequenceNumberFutureStore, future);
  }

  @Test
  public void shouldThrowExceptionOnTimeout() throws Exception {
    // Given:
    givenCmdStoreUpThroughPosition(0);
    expect(sequenceNumberFutureStore.getFutureForSequenceNumber(EasyMock.anyLong())).andReturn(future);
    expect(future.get(EasyMock.anyLong(), EasyMock.anyObject(TimeUnit.class)))
        .andThrow(new TimeoutException());
    replay(future, sequenceNumberFutureStore);

    try {
      // When:
      commandStore.ensureConsumedUpThrough(2, TIMEOUT);

      // Then:
      fail("TimeoutException should be propagated.");
    } catch (final TimeoutException e) {
      assertThat(e.getMessage(),
          is(String.format(
              "Timeout reached while waiting for command sequence number of 2. (Timeout: %d ms)", TIMEOUT)));
    }
    verify(commandConsumer, future, sequenceNumberFutureStore);
  }

  @Test
  public void shouldCompleteFuturesWhenGettingNewCommands() {
    // Given:
    sequenceNumberFutureStore.completeFuturesUpToSequenceNumber(EasyMock.anyLong());
    expectLastCall();
    expect(commandConsumer.poll(anyObject(Duration.class))).andReturn(buildRecords());
    replay(sequenceNumberFutureStore, commandConsumer);

    // When:
    commandStore.getNewCommands();

    // Then:
    verify(sequenceNumberFutureStore);
  }

  private void setupConsumerToReturnCommand(final CommandId commandId, final Command command) {
    reset(commandConsumer);
    expect(commandConsumer.poll(anyObject(Duration.class))).andReturn(
        buildRecords(commandId, command)
    ).times(1);
    replay(commandConsumer);
  }

  private void givenCmdStoreUpThroughPosition(long position) {
    reset(commandConsumer);
    expect(commandConsumer.position(anyObject(TopicPartition.class))).andReturn(position);
    replay(commandConsumer);
  }

  private void givenCommandStoreThatAssignsSameId(final CommandId commandId) {
    commandIdAssigner = mock(CommandIdAssigner.class);
    expect(commandIdAssigner.getCommandId(anyObject())).andStubAnswer(
        () -> new CommandId(commandId.getType(), commandId.getEntity(), commandId.getAction())
    );
    replay(commandIdAssigner);
    setUpCommandStore();
  }

  private void setUpCommandStore() {
    commandStore = new CommandStore(
        COMMAND_TOPIC,
        commandConsumer,
        commandProducer,
        commandIdAssigner,
        sequenceNumberFutureStore
    );
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
