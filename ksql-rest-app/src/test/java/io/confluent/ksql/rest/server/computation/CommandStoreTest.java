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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import org.junit.Test;
import org.junit.runner.RunWith;


@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());
  private static final Map<String, Object> OVERRIDE_PROPERTIES = Collections.emptyMap();

  private final Consumer<CommandId, Command> commandConsumer = niceMock(Consumer.class);
  private final Producer<CommandId, Command> commandProducer = mock(Producer.class);
  private final String statementText = "test-statement";
  private final CommandId commandId =
      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
  private final Statement statement = mock(Statement.class);
  private final Future<RecordMetadata> future = niceMock(Future.class);
  private final Command command =
      new Command(statementText, Collections.emptyMap(), Collections.emptyMap());
  private final Node node = mock(Node.class);
  private final RecordMetadata recordMetadata = new RecordMetadata(
      new TopicPartition("topic", 0), 0, 0, RecordBatch.NO_TIMESTAMP, 0L, 0, 0);

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

    final CommandStore command = createCommandStore();
    final List<Pair<CommandId, Command>> commands = getPriorCommands(command);
    assertThat(commands, equalTo(Arrays.asList(new Pair<>(createId, originalCommand),
        new Pair<>(dropId, dropCommand),
        new Pair<>(createId, latestCommand))));
  }

  @Test
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() throws InterruptedException, ExecutionException {
    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);

    // Given:
    expect(commandProducer.send(anyObject())).andReturn(future);
    expect(future.get()).andReturn(recordMetadata);
    replay(commandProducer, future);
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
  public void shouldCleanupCommandStatusOnProduceError() throws InterruptedException, ExecutionException {
    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);

    // Given:
    expect(commandProducer.send(anyObject())).andThrow(new RuntimeException("oops")).times(1);
    expect(commandProducer.send(anyObject())).andReturn(future).times(1);
    expect(future.get()).andReturn(recordMetadata);
    replay(commandProducer, future);
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
  public void shouldEnqueueNewAfterHandlingExistingCommand() throws InterruptedException, ExecutionException {
    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);

    // Given:
    setupConsumerToReturnCommand(commandId, command);
    expect(commandProducer.send(anyObject(ProducerRecord.class))).andAnswer(
        () -> {
          commandStore.getNewCommands();
          return future;
        }
    ).times(1);
    expect(commandProducer.send(anyObject(ProducerRecord.class))).andReturn(future);
    expect(future.get()).andReturn(recordMetadata).times(2);
    replay(future, commandProducer);
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    verify(future, commandProducer, commandConsumer);
  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands()
      throws ExecutionException, InterruptedException {
    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);

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
          return future;
        }
    );
    expect(future.get()).andReturn(recordMetadata);
    replay(future, commandProducer);

    // When:
    commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    // verifying the commandProducer also verifies the assertions in its IAnswer were run
    verify(future, commandProducer, commandConsumer);
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
    expectConsumerToReturnPartitionInfo();
    expect(commandConsumer.poll(anyObject())).andReturn(records);
    replay(commandConsumer);

    // When:
    final List<QueuedCommand> commands = createCommandStore().getNewCommands();

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
    expectConsumerToReturnPartitionInfo();
    expect(commandConsumer.poll(anyObject())).andReturn(records);
    expect(commandConsumer.poll(anyObject())).andReturn(ConsumerRecords.empty());
    replay(commandConsumer);

    // When:
    final List<QueuedCommand> commands = createCommandStore().getRestoreCommands();

    // Then:
    assertThat(commands, hasSize(1));
    assertThat(commands.get(0).getCommandId(), equalTo(id));
    assertThat(commands.get(0).getCommand(), equalTo(command));
  }

  @Test
  public void shouldDistributeCommand() throws ExecutionException, InterruptedException {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
    final Map<String, Object> overrideProperties = Collections.singletonMap(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final String statementText = "test-statement";

    final Statement statement = mock(Statement.class);
    final Capture<ProducerRecord<CommandId, Command>> recordCapture = Capture.newInstance();

    expect(commandProducer.send(capture(recordCapture))).andReturn(future);
    expect(future.get()).andReturn(recordMetadata);
    replay(commandProducer, future);

    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);
    commandStore.enqueueCommand(statementText, statement, ksqlConfig, overrideProperties);

    verify(commandProducer, future);

    final ProducerRecord<CommandId, Command> record = recordCapture.getValue();
    assertThat(record.key(), equalTo(commandId));
    assertThat(record.value().getStatement(), equalTo(statementText));
    assertThat(record.value().getOverwriteProperties(), equalTo(overrideProperties));
    assertThat(record.value().getOriginalProperties(), equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
  }

  @Test
  public void shouldIncludeTopicOffsetInSuccessfulQueuedCommandStatus()
      throws InterruptedException, ExecutionException {
    // Given:
    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);

    expect(commandProducer.send(anyObject(ProducerRecord.class))).andReturn(future);
    expect(future.get()).andReturn(recordMetadata);
    replay(commandProducer, future);

    // When:
    final QueuedCommandStatus commandStatus =
        commandStore.enqueueCommand(statementText, statement, KSQL_CONFIG, OVERRIDE_PROPERTIES);

    // Then:
    assertThat(commandStatus.getCommandOffset(), equalTo(recordMetadata.offset()));

    verify(commandProducer, future);
  }

  @Test
  public void shouldReturnCompletedFutureIfOffsetReached() {
    // Given:
    givenCmdConsumerAtPosition(1);

    final CommandStore commandStore = createCommandStore();

    // When:
    final CompletableFuture<Void> future = commandStore.getConsumerPositionFuture(0);

    // Then:
    assertFutureIsCompleted(future);
    verify(commandConsumer);
  }

  @Test
  public void shouldReturnUncompletedFutureIfOffsetNotReached() {
    // Given:
    givenCmdConsumerAtPosition(2);

    final CommandStore commandStore = createCommandStore();

    // When:
    final CompletableFuture<Void> future = commandStore.getConsumerPositionFuture(2);

    // Then:
    assertFutureIsNotCompleted(future);
    verify(commandConsumer);
  }

  @Test
  public void shouldCompleteFutureWhenOffsetIsReached() {
    // Given:
    final CommandStore commandStore = createCommandStore();

    givenCmdConsumerAtPosition(0);
    final CompletableFuture future = commandStore.getConsumerPositionFuture(2);
    givenCmdConsumerAtPosition(3, true);

    // When:
    commandStore.getNewCommands();

    // Then:
    assertFutureIsCompleted(future);
    verify(commandConsumer);
  }

  @Test
  public void shouldNotCompleteFutureWhenOffsetIsNotReached() {
    // Given:
    final CommandStore commandStore = createCommandStore();

    givenCmdConsumerAtPosition(0);
    final CompletableFuture future = commandStore.getConsumerPositionFuture(2);
    givenCmdConsumerAtPosition(2, true);

    // When:
    commandStore.getNewCommands();

    // Then:
    assertFutureIsNotCompleted(future);
    verify(commandConsumer);
  }

  private void setupConsumerToReturnCommand(final CommandId commandId, final Command command) {
    reset(commandConsumer);
    expect(commandConsumer.poll(anyObject(Duration.class))).andReturn(
        buildRecords(commandId, command)
    ).times(1);
    expectConsumerToReturnPartitionInfo();
    replay(commandConsumer);
  }

  private void givenCmdConsumerAtPosition(long position) {
    givenCmdConsumerAtPosition(position, false);
  }

  private void givenCmdConsumerAtPosition(long position, boolean poll) {
    reset(commandConsumer);
    expectConsumerToReturnPartitionInfo();
    expect(commandConsumer.position(anyObject(TopicPartition.class))).andReturn(position);
    if (poll) {
      expect(commandConsumer.poll(anyObject()))
          .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    }
    replay(commandConsumer);
  }

  private void expectConsumerToReturnPartitionInfo() {
    expect(commandConsumer.partitionsFor(COMMAND_TOPIC))
        .andReturn(ImmutableList.of(
            new PartitionInfo(COMMAND_TOPIC, 0, node, new Node[]{node}, new Node[]{node})
        ));
  }

  private CommandStore createCommandStoreThatAssignsSameId(final CommandId commandId) {
    final CommandIdAssigner commandIdAssigner = mock(CommandIdAssigner.class);
    expect(commandIdAssigner.getCommandId(anyObject())).andStubAnswer(
        () -> new CommandId(commandId.getType(), commandId.getEntity(), commandId.getAction())
    );
    replay(commandIdAssigner);
    return createCommandStore(commandIdAssigner);
  }

  private CommandStore createCommandStore() {
    return createCommandStore(new CommandIdAssigner(new MetaStoreImpl(new InternalFunctionRegistry())));
  }

  private CommandStore createCommandStore(final CommandIdAssigner commandIdAssigner) {
    return new CommandStore(
        COMMAND_TOPIC,
        commandConsumer,
        commandProducer,
        commandIdAssigner);
  }

  private List<Pair<CommandId, Command>> getPriorCommands(final CommandStore commandStore) {
    return commandStore.getRestoreCommands().stream()
        .map(
            queuedCommand -> new Pair<>(
                queuedCommand.getCommandId(), queuedCommand.getCommand()))
        .collect(Collectors.toList());
  }

  private ConsumerRecords<CommandId, Command> buildRecords(final Object ...args) {
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

  private void assertFutureIsCompleted(CompletableFuture<Void> future) {
    assertThat(future.isDone(), is(true));
    assertThat(future.isCancelled(), is(false));
    assertThat(future.isCompletedExceptionally(), is(false));
  }

  private void assertFutureIsNotCompleted(CompletableFuture<Void> future) {
    assertThat(future.isDone(), is(false));
  }
}
