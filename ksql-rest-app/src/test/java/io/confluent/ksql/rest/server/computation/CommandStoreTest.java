/**
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

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(EasyMockRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  @Mock(type = MockType.NICE)
  private Consumer<CommandId, Command> commandConsumer;
  @Mock(type = MockType.NICE)
  private Producer<CommandId, Command> commandProducer;


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

    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, createId, originalCommand),
        new ConsumerRecord<>("topic", 0, 0, dropId, dropCommand),
        new ConsumerRecord<>("topic", 0, 0, createId, latestCommand))
    ));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore command = createCommandStore();
    final List<Pair<CommandId, Command>> commands = getPriorCommands(command);
    assertThat(commands, equalTo(Arrays.asList(new Pair<>(createId, originalCommand),
        new Pair<>(dropId, dropCommand),
        new Pair<>(createId, latestCommand))));
  }

  @Test
  public void shoudlDistributeCommand() throws ExecutionException, InterruptedException {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "foo"));
    final Map<String, Object> overrideProperties = Collections.singletonMap(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final String statementText = "test-statement";

    final Statement statement = mock(Statement.class);
    final CommandIdAssigner commandIdAssigner = mock(CommandIdAssigner.class);
    final Capture<ProducerRecord<CommandId, Command>> recordCapture = Capture.newInstance();
    final Future<RecordMetadata> future = mock(Future.class);

    final CommandId commandId = new CommandId(CommandId.Type.STREAM, "foo", CommandId.Action.CREATE);
    expect(commandIdAssigner.getCommandId(statement)).andReturn(commandId);
    expect(commandProducer.send(capture(recordCapture))).andReturn(future);
    future.get();
    expectLastCall().andReturn(null);
    replay(commandIdAssigner, commandProducer, future);

    final CommandStore commandStore = createCommandStore(commandIdAssigner);
    commandStore.distributeStatement(statementText, statement, ksqlConfig, overrideProperties);

    verify(commandIdAssigner, commandProducer, future);

    final ProducerRecord<CommandId, Command> record = recordCapture.getValue();
    assertThat(record.key(), equalTo(commandId));
    assertThat(record.value().getStatement(), equalTo(statementText));
    assertThat(record.value().getOverwriteProperties(), equalTo(overrideProperties));
    assertThat(record.value().getOriginalProperties(), equalTo(ksqlConfig.getAllConfigPropsWithSecretsObfuscated()));
  }


  @Test
  public void shouldCollectTerminatedQueries() {
    final CommandId terminated = new CommandId(CommandId.Type.TERMINATE, "queryId", CommandId.Action.EXECUTE);
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("topic", 0), Collections.singletonList(
        new ConsumerRecord<>("topic", 0, 0, terminated, new Command(
            "terminate query 'queryId'", Collections.emptyMap(), Collections.emptyMap()
    )))));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());
    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore commandStore = createCommandStore();
    final RestoreCommands restoreCommands = commandStore.getRestoreCommands();
    assertThat(restoreCommands.terminatedQueries(), equalTo(Collections.singletonMap(new QueryId("queryId"), terminated)));
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

  private List<Pair<CommandId, Command>> getPriorCommands(final CommandStore command) {
    final RestoreCommands priorCommands = command.getRestoreCommands();
    final List<Pair<CommandId, Command>> commands = new ArrayList<>();
    priorCommands.forEach(((id, cmd, terminatedQueries, droppedEntities) -> commands.add(new Pair<>(id, cmd))));
    return commands;
  }

}