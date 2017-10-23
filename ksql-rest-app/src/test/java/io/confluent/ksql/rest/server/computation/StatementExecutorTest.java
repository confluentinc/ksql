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

import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.mock.MockKsqkEngine;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.util.Pair;

public class StatementExecutorTest extends EasyMockSupport {


  private StatementExecutor getStatementExecutor() {
    MockKsqkEngine mockKsqkEngine = new MockKsqkEngine(
        TestUtils.getMockKsqlConfig(), new MockKafkaTopicClient());

    StatementParser statementParser = new StatementParser(mockKsqkEngine);

    return new StatementExecutor(mockKsqkEngine, statementParser);
  }

  @Test
  public void handleCorrectDDLStatement() throws Exception {
    StatementExecutor statementExecutor = getStatementExecutor();
    Command command = new Command("REGISTER TOPIC users_topic WITH (value_format = 'json', "
                                  + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_CorrectTopicGen");
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.SUCCESS);

  }

  @Test
  public void handleIncorrectDDLStatement() throws Exception {
    StatementExecutor statementExecutor = getStatementExecutor();
    Command command = new Command("REGIST ER TOPIC users_topic WITH (value_format = 'json', "
                                  + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_IncorrectTopicGen");
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.ERROR);

  }

  @Test
  public void handleCSAS_CTASStatement() throws Exception {
    StatementExecutor statementExecutor = getStatementExecutor();

    Command topicCommand = new Command("REGISTER TOPIC pageview_topic WITH "
                                       + "(value_format = 'json', "
                                       + "kafka_topic='pageview_topic_json');", new HashMap<>());
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    statementExecutor.handleStatement(topicCommand, topicCommandId);

    Command csCommand = new Command("CREATE STREAM pageview "
                                    + "(viewtime bigint, pageid varchar, userid varchar) "
                                    + "WITH (registered_topic = 'pageview_topic');",
                                    new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    statementExecutor.handleStatement(csCommand, csCommandId);

    Command csasCommand = new Command("CREATE STREAM user1pv "
                                    + " AS select * from pageview WHERE userid = 'user1';",
                                    new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    statementExecutor.handleStatement(csasCommand, csasCommandId);

    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
                                      + " AS select * from pageview window tumbling(size 5 "
                                      + "second) WHERE userid = "
                                      + "'user1' group by pageid;",
                                      new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");

    statementExecutor.handleStatement(ctasCommand, ctasCommandId);

    Command terminateCommand = new Command("TERMINATE 1;",
                                      new HashMap<>());

    CommandId terminateCommandId =  new CommandId(CommandId.Type.TABLE, "_TerminateGen");
    statementExecutor.handleStatement(terminateCommand, terminateCommandId);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(6, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(terminateCommandId).getStatus());

  }

  @Test
  public void handlePriorStatement() throws Exception {
    StatementExecutor statementExecutor = getStatementExecutor();
    TestUtils testUtils = new TestUtils();
    List<Pair<CommandId, Command>> priorCommands = testUtils.getAllPriorCommandRecords();

    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen");
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen");
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen");
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen");

    statementExecutor.handleStatements(priorCommands);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(4, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }

}
