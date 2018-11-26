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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.server.CommandTopic;
import io.confluent.ksql.util.KsqlConfig;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;


@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class CommandStoreTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private CommandIdAssigner commandIdAssigner;
  @Mock
  private CommandTopic commandTopic;
  @Mock
  private Statement statement;

  @Mock
  private CommandId commandId1;
  @Mock
  private Command command1;
  @Mock
  private CommandId commandId2;
  @Mock
  private Command command2;
  @Mock
  private CommandId commandId3;
  @Mock
  private Command command3;
  @Mock
  private RestoreCommands restoreCommands;
  @Captor
  private ArgumentCaptor<Duration> durationCaptor;

  private CommandStore commandStore;

  @Before
  public void setup() {
    commandStore = new CommandStore(commandIdAssigner, commandTopic);
//=======
//  private static final String COMMAND_TOPIC = "command";
//  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.emptyMap());
//  private static final Map<String, Object> OVERRIDE_PROPERTIES = Collections.emptyMap();
//
//  private final Consumer<CommandId, Command> commandConsumer = niceMock(Consumer.class);
//  private final Producer<CommandId, Command> commandProducer = mock(Producer.class);
//  private final String statementText = "test-statement";
//  private final CommandId commandId =
//      new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
//  private final Statement statement = mock(Statement.class);
//  private final Future<RecordMetadata> future = niceMock(Future.class);
//  private final Command command =
//      new Command(statementText, Collections.emptyMap(), Collections.emptyMap());
//  private final Node node = mock(Node.class);
//
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

    final List<QueuedCommand> queuedCommandList = buildRecords(
      createId, originalCommand,
      dropId, dropCommand,
      createId, latestCommand
    );
    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(queuedCommandList);

    // When:
    final List<QueuedCommand> commands = commandStore.getRestoreCommands();

    // Then:
    assertThat(commands, equalTo(Arrays.asList(new QueuedCommand(createId, originalCommand),
        new QueuedCommand(dropId, dropCommand),
        new QueuedCommand(createId, latestCommand))));
  }

  @Test (expected = IllegalStateException.class)
  public void shouldFailEnqueueIfCommandWithSameIdRegistered() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());
  }

  @Test (expected = KsqlException.class)
  public void shouldCleanupCommandStatusOnProduceError() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    doThrow(new RuntimeException("oops!")).doNothing().when(commandTopic).send(any(), any());

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

  }

  @Test
  public void shouldEnqueueNewAfterHandlingExistingCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());


    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));

  }

  @Test
  public void shouldRegisterBeforeDistributeAndReturnStatusOnGetNewCommands() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1)));
    doAnswer(
        (Answer) invocation -> {
          List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();
          assertThat(queuedCommandList.size(), equalTo(1));
          assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
          return null;

        }
    ).when(commandTopic).send(any(CommandId.class), any(Command.class));

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));
    verify(commandTopic).getNewCommands(any(Duration.class));
  }

  @Test
  public void shouldFilterNullCommands() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(
            new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1),
            new ConsumerRecord<CommandId, Command>("", 1, 1, commandId2, null)
            ));

    // When:
    final List<QueuedCommand> commands = commandStore.getNewCommands();

    // Then:
    assertThat(commands, equalTo(ImmutableList.of(new QueuedCommand(commandId1, command1))));
    verify(commandTopic).getNewCommands(any(Duration.class));
  }

  @Test
  public void shouldFilterNullPriorCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getRestoreCommands(any(Duration.class)))
        .thenReturn(ImmutableList.of(
            new QueuedCommand(commandId1, command1),
            new QueuedCommand(commandId2, null)
        ));
//    final CommandId id = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
//    final Command command = new Command(
//        "some statement", Collections.emptyMap(), Collections.emptyMap());
//    final ConsumerRecords<CommandId, Command> records = buildRecords(
//        id, null,
//        id, command);
//    expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andStubReturn(
//        ImmutableList.of(
//            new PartitionInfo(COMMAND_TOPIC, 0, node, new Node[]{node}, new Node[]{node})
//        )
//    );
//    expect(commandConsumer.poll(anyObject())).andReturn(records);
//    expect(commandConsumer.poll(anyObject())).andReturn(ConsumerRecords.empty());
//    replay(commandConsumer);

    // When:
    final List<QueuedCommand> commands = commandStore.getRestoreCommands();

    // Then:
    verify(commandTopic).getRestoreCommands(any(Duration.class));


//    assertThat(commands, hasSize(1));
//    assertThat(commands.get(0).getCommandId(), equalTo(id));
//    assertThat(commands.get(0).getCommand(), equalTo(command));
  }

  @Test
  public void shouldDistributeCommand() {
    // Given:
    when(commandIdAssigner.getCommandId(any(Statement.class))).thenReturn(commandId1).thenReturn(commandId1);
    when(commandTopic.getNewCommands(any(Duration.class)))
        .thenReturn((Iterable) ImmutableList.of(new ConsumerRecord<CommandId, Command>("", 1, 1, commandId1, command1)));
    doAnswer(
        (Answer) invocation -> {
          List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();
          assertThat(queuedCommandList.size(), equalTo(1));
          assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
          return null;

        }
    ).when(commandTopic).send(any(CommandId.class), any(Command.class));

    // When:
    commandStore.enqueueCommand("foo", statement, ksqlConfig, Collections.emptyMap());

    // Then:
    verify(commandTopic).send(same(commandId1), any(Command.class));
    verify(commandTopic).getNewCommands(any(Duration.class));

//    final KsqlConfig ksqlConfig = new KsqlConfig(
//        Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
//    final Map<String, Object> overrideProperties = Collections.singletonMap(
//        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    final String statementText = "test-statement";
//
//    final Statement statement = mock(Statement.class);
//    final Capture<ProducerRecord<CommandId, Command>> recordCapture = Capture.newInstance();
//    final Future<RecordMetadata> future = mock(Future.class);
//
//    expect(commandProducer.send(capture(recordCapture))).andReturn(future);
//    future.get();
//    expectLastCall().andReturn(null);
//    replay(commandProducer, future);
//
//    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);
//    commandStore.enqueueCommand(statementText, statement, ksqlConfig, overrideProperties);
//
//    verify(commandProducer, future);
//
//    final ProducerRecord<CommandId, Command> record = recordCapture.getValue();
//    assertThat(record.key(), equalTo(commandId));
//    assertThat(record.value().getStatement(), equalTo(statementText));
//    assertThat(record.value().getOverwriteProperties(), equalTo(overrideProperties));
//    assertThat(record.value().getOriginalProperties(), equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
  }

  private void setupCommandTopicToReturnCommand(final CommandId commandId, final Command command) {
//    expect(commandConsumer.poll(anyObject(Duration.class))).andReturn(
//        buildRecords(commandId, command)
//    ).times(1);
//    replay(commandConsumer);
    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(buildRecords(commandId, command));
  }

  private CommandStore createCommandStoreThatAssignsSameId(final CommandId commandId) {
    final CommandIdAssigner commandIdAssigner = mock(CommandIdAssigner.class);
    when(commandIdAssigner.getCommandId(any())).thenReturn(new CommandId(commandId.getType(), commandId.getEntity(), commandId.getAction()));
//    expect(commandIdAssigner.getCommandId(anyObject())).andStubAnswer(
//        () -> new CommandId(commandId.getType(), commandId.getEntity(), commandId.getAction())
//    );
//    replay(commandIdAssigner);
    return createCommandStore(commandIdAssigner);
  }

  private CommandStore createCommandStore() {
    return createCommandStore(new CommandIdAssigner(new MetaStoreImpl(new InternalFunctionRegistry())));
  }

  private CommandStore createCommandStore(final CommandIdAssigner commandIdAssigner) {
    return new CommandStore(commandIdAssigner, commandTopic);
  }

  private List<Pair<CommandId, Command>> getPriorCommands(final CommandStore commandStore) {
    return commandStore.getRestoreCommands().stream()
        .map(
            queuedCommand -> new Pair<>(
                queuedCommand.getCommandId(), queuedCommand.getCommand()))
        .collect(Collectors.toList());
  }

//  private ConsumerRecords<CommandId, Command> buildRecords(final Object ...args) {
  private List<QueuedCommand> buildRecords(final Object ...args) {
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

//  @Test
//  public void shouldCallCloseCorrectly() {
//    // When:
//    commandStore.close();
//
//    // Then:
//    verify(commandTopic).close();
//  }
//
//  @Test
//  public void shouldEnqueueCommandCorrectly() throws Exception {
//    // Given:
//    when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId1);
//    when(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()).thenReturn(Collections.emptyMap());
//
//    // When:
//    commandStore.enqueueCommand(
//        "",
//        statement,
//        ksqlConfig,
//        Collections.emptyMap()
//    );
//
//    // Then:
//    verify(commandTopic).send(eq(commandId1), any(Command.class));
//  }
//
//  @Test (expected = KsqlException.class)
//  public void shouldNotEnqueueCommandIfThereIsAnException() throws Exception {
//    // Given:
//    when(commandIdAssigner.getCommandId(statement)).thenReturn(commandId1);
//    doThrow(new KsqlException("")).when(commandTopic).send(eq(commandId1), any(Command.class));
//
//    // When:
//    commandStore.enqueueCommand(
//        "",
//        statement,
//        ksqlConfig,
//        Collections.emptyMap()
//    );
//  }
//
//  @Test
//  public void shouldGetNewCommandsCorrectly() {
//    // Given:
//    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(
//        Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
//            new ConsumerRecord<>("topic", 0, 0, commandId1, command1),
//            new ConsumerRecord<>("topic", 0, 0, commandId2, command2),
//            new ConsumerRecord<>("topic", 0, 0, commandId3, command3))
//        ));
//    when(commandTopic.getNewCommands(any(Duration.class))).thenReturn(records);
//
//    // When:
//    final List<QueuedCommand> queuedCommandList = commandStore.getNewCommands();
//
//    // Then:
//<<<<<<< HEAD
//    assertThat(queuedCommandList.get(0).getCommand(), equalTo(Optional.of(command1)));
//    assertThat(queuedCommandList.get(0).getCommandId(), equalTo(commandId1));
//    assertThat(queuedCommandList.get(1).getCommand(), equalTo(Optional.of(command2)));
//    assertThat(queuedCommandList.get(1).getCommandId(), equalTo(commandId2));
//    assertThat(queuedCommandList.get(2).getCommand(), equalTo(Optional.of(command3)));
//    assertThat(queuedCommandList.get(2).getCommandId(), equalTo(commandId3));
//=======
//    // verifying the commandProducer also verifies the assertions in its IAnswer were run
//    verify(future, commandProducer, commandConsumer);
//  }
//
//  @Test
//  public void shouldFilterNullCommands() {
//    // Given:
//    final CommandId id = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
//    final Command command = new Command(
//        "some statement", Collections.emptyMap(), Collections.emptyMap());
//    final ConsumerRecords<CommandId, Command> records = buildRecords(
//        id, null,
//        id, command);
//    expect(commandConsumer.poll(anyObject())).andReturn(records);
//    replay(commandConsumer);
//
//    // When:
//    final List<QueuedCommand> commands = createCommandStore().getNewCommands();
//
//    // Then:
//    assertThat(commands, hasSize(1));
//    assertThat(commands.get(0).getCommandId(), equalTo(id));
//    assertThat(commands.get(0).getCommand(), equalTo(command));
//  }
//
//  @Test
//  public void shouldFilterNullPriorCommand() {
//    // Given:
//    final CommandId id = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
//    final Command command = new Command(
//        "some statement", Collections.emptyMap(), Collections.emptyMap());
//    final ConsumerRecords<CommandId, Command> records = buildRecords(
//        id, null,
//        id, command);
//    expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andStubReturn(
//        ImmutableList.of(
//            new PartitionInfo(COMMAND_TOPIC, 0, node, new Node[]{node}, new Node[]{node})
//        )
//    );
//    expect(commandConsumer.poll(anyObject())).andReturn(records);
//    expect(commandConsumer.poll(anyObject())).andReturn(ConsumerRecords.empty());
//    replay(commandConsumer);
//
//    // When:
//    final List<QueuedCommand> commands = createCommandStore().getRestoreCommands();
//
//    // Then:
//    assertThat(commands, hasSize(1));
//    assertThat(commands.get(0).getCommandId(), equalTo(id));
//    assertThat(commands.get(0).getCommand(), equalTo(command));
//  }
//
//  @Test
//  public void shouldDistributeCommand() throws ExecutionException, InterruptedException {
//    final KsqlConfig ksqlConfig = new KsqlConfig(
//        Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
//    final Map<String, Object> overrideProperties = Collections.singletonMap(
//        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    final String statementText = "test-statement";
//
//    final Statement statement = mock(Statement.class);
//    final Capture<ProducerRecord<CommandId, Command>> recordCapture = Capture.newInstance();
//    final Future<RecordMetadata> future = mock(Future.class);
//
//    expect(commandProducer.send(capture(recordCapture))).andReturn(future);
//    future.get();
//    expectLastCall().andReturn(null);
//    replay(commandProducer, future);
//
//    final CommandStore commandStore = createCommandStoreThatAssignsSameId(commandId);
//    commandStore.enqueueCommand(statementText, statement, ksqlConfig, overrideProperties);
//>>>>>>> upstream/master
//
//  }
//
//<<<<<<< HEAD
//  @Test
//  public void shouldGetRestoreCommands() {
//    // Given:
//    when(commandTopic.getRestoreCommands(any(Duration.class))).thenReturn(restoreCommands);
//
//    // When:
//    final RestoreCommands restoreCommands1 = commandStore.getRestoreCommands();
//=======
//  private void setupConsumerToReturnCommand(final CommandId commandId, final Command command) {
//    reset(commandConsumer);
//    expect(commandConsumer.poll(anyObject(Duration.class))).andReturn(
//        buildRecords(commandId, command)
//    ).times(1);
//    replay(commandConsumer);
//  }
//>>>>>>> upstream/master
//
//    // Then:
//    assertThat(restoreCommands1, equalTo(restoreCommands));
//  }
//
//  @Test
//  public void shouldPassTheCorrectDuration() {
//
//    // When:
//    final RestoreCommands restoreCommands1 = commandStore.getRestoreCommands();
//
//<<<<<<< HEAD
//    // Then:
//    verify(commandTopic).getRestoreCommands(durationCaptor.capture());
//    assertThat(durationCaptor.getValue(), equalTo(Duration.ofMillis(5000)));
//=======
//
//>>>>>>> upstream/master
//  }
}
