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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockCommandStore;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.mock.MockKsqkEngine;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;

public class CommandRunnerTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @AfterClass
  public static void cleanUp() {
    CLUSTER.stop();
  }

  private CommandRunner getCommanRunner(StatementExecutor statementExecutor) {

    Map<String, Object> commandConsumerProperties = new HashMap<>();
    commandConsumerProperties.put("bootstrap.servers", CLUSTER.bootstrapServers());
    Deserializer<Command> commandDeserializer = new KafkaJsonDeserializer<>();
    Deserializer<CommandId> commandIdDeserializer = new KafkaJsonDeserializer<>();

    KafkaConsumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        commandIdDeserializer,
        commandDeserializer
    );

    CommandRunner commandRunner = new CommandRunner(statementExecutor, new MockCommandStore
        ("CT", commandConsumer, null,
         new CommandIdAssigner(new MetaStoreImpl())));
    return commandRunner;
  }

  private StatementExecutor getStatementExecutor() {
    Map<String, Object> props = new HashMap<>();
    props.put("application.id", "ksqlStatementExecutorTest");
    props.put("bootstrap.servers", CLUSTER.bootstrapServers());

    MockKsqkEngine mockKsqkEngine = new MockKsqkEngine(
        new KsqlConfig(props), new MockKafkaTopicClient());

    StatementParser statementParser = new StatementParser(mockKsqkEngine);

    return new StatementExecutor(mockKsqkEngine, statementParser);
  }

  @Test
  public void testThread() throws InterruptedException {
    StatementExecutor statementExecutor = getStatementExecutor();
    CommandRunner commandRunner = getCommanRunner(statementExecutor);
    new Thread(commandRunner).start();
    Thread.sleep(2000);
    commandRunner.close();
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    if (statusStore.containsKey(csasCommandId)) {
      Assert.assertTrue((statusStore.get(csasCommandId).getStatus() == CommandStatus.Status
          .EXECUTING) || (statusStore.get(csasCommandId).getStatus() == CommandStatus.Status
          .SUCCESS));
    }
    if (statusStore.containsKey(ctasCommandId)) {
      Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
    }
  }

  @Test
  public void testPriorCommandsRun() throws Exception {
    StatementExecutor statementExecutor = getStatementExecutor();
    CommandRunner commandRunner = getCommanRunner(statementExecutor);
    commandRunner.processPriorCommands();
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(4, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }

}
