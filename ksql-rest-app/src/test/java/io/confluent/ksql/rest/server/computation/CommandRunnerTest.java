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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.verify;

public class CommandRunnerTest {

  private Map<TopicPartition, List<ConsumerRecord<CommandId, Command>>> getRecordMap() {
    List<Pair<CommandId, Command>> commandList = new TestUtils().getAllPriorCommandRecords();
    List<ConsumerRecord<CommandId, Command>> recordList = new ArrayList<>();
    for (Pair commandPair: commandList) {
      recordList.add(new ConsumerRecord<>("T", 1, 1, (CommandId) commandPair
          .getLeft(), (Command) commandPair.getRight()));
    }
    Map<TopicPartition, List<ConsumerRecord<CommandId, Command>>> recordMap = new HashMap<>();
    recordMap.put(new TopicPartition("T", 1), recordList);
    return recordMap;
  }

  @Test
  public void shouldFetchAndRunNewCommandsFromCommandTopic() throws Exception {
    StatementExecutor statementExecutor = mock(StatementExecutor.class);
    statementExecutor.handleStatement(anyObject(), anyObject());
    expectLastCall().times(4);
    replay(statementExecutor);

    CommandStore commandStore = mock(CommandStore.class);
    expect(commandStore.getNewCommands()).andReturn(new ConsumerRecords<>(getRecordMap()));
    replay(commandStore);
    CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore);
    commandRunner.fetchAndRunCommands();
    verify(statementExecutor);
  }

  @Test
  public void shouldFetchAndRunPriorCommandsFromCommandTopic() throws Exception {
    StatementExecutor statementExecutor = mock(StatementExecutor.class);
    statementExecutor.handleRestoration(anyObject());
    expectLastCall();
    replay(statementExecutor);
    CommandStore commandStore = mock(CommandStore.class);
    expect(commandStore.getRestoreCommands()).andReturn(new RestoreCommands());
    replay(commandStore);
    CommandRunner commandRunner = new CommandRunner(statementExecutor, commandStore);
    commandRunner.processPriorCommands();

    verify(statementExecutor);
  }

}
