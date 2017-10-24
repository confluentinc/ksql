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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;

import static org.easymock.EasyMock.*;

public class CommandRunnerTest {

  /**
   *   Get a command runner instance using mock values
    */
  private CommandRunner getCommanRunner(StatementExecutor statementExecutor) {
    List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
    List<ConsumerRecord<CommandId, Command>> recordList = new ArrayList<>();
    for (Pair commandPair: commandList) {
      recordList.add(new ConsumerRecord<CommandId, Command>("T", 1, 1, (CommandId) commandPair
          .getLeft(), (Command) commandPair.getRight()));
    }
    Map<TopicPartition, List<ConsumerRecord<CommandId, Command>>> recordMap = new HashMap<>();
    recordMap.put(new TopicPartition("T", 1), recordList);
    CommandStore commandStore = mock(CommandStore.class);
    expect(commandStore.getNewCommands()).andReturn(new ConsumerRecords<>(recordMap));
    expect(commandStore.getPriorCommands()).andReturn(Collections.emptyList());
    replay(commandStore);
    return new CommandRunner(statementExecutor, commandStore);
  }

  private StatementExecutor getMockStatementExecutor() throws Exception {
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");
    Map<CommandId, CommandStatus> statusStore = new HashMap<>();
    statusStore.put(topicCommandId, new CommandStatus(CommandStatus.Status.SUCCESS, "Success"));
    statusStore.put(csCommandId, new CommandStatus(CommandStatus.Status.SUCCESS, "Success"));
    statusStore.put(csasCommandId, new CommandStatus(CommandStatus.Status.SUCCESS, "Success"));
    statusStore.put(ctasCommandId, new CommandStatus(CommandStatus.Status.ERROR, "Error"));

    StatementExecutor statementExecutor = mock(StatementExecutor.class);
    expect(statementExecutor.getStatuses()).andReturn(statusStore);

    return statementExecutor;
  }

  @Test
  public void testNewCommandRun() throws Exception {
    StatementExecutor statementExecutor = getMockStatementExecutor();
    statementExecutor.handleStatement(anyObject(), anyObject());
    expectLastCall().times(4);
    replay(statementExecutor);
    CommandRunner commandRunner = getCommanRunner(statementExecutor);
    commandRunner.fetchAndRunCommands();
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
    verify(statementExecutor);
  }

  @Test
  public void testPriorCommandsRun() throws Exception {
    StatementExecutor statementExecutor = getMockStatementExecutor();
    statementExecutor.handleStatements(anyObject());
    expectLastCall();
    replay(statementExecutor);
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
    verify(statementExecutor);
  }

}
