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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.MatcherAssert.assertThat;

public class StatementExecutorTest extends EasyMockSupport {

  private KsqlEngine ksqlEngine;
  private StatementExecutor statementExecutor;

  @Before
  public void setUp() {
    Map<String, Object> props = new HashMap<>();
    props.put("application.id", "ksqlStatementExecutorTest");
    props.put("bootstrap.servers", CLUSTER.bootstrapServers());

    ksqlEngine = new KsqlEngine(
        new KsqlConfig(props), new MockKafkaTopicClient());

    StatementParser statementParser = new StatementParser(ksqlEngine);

    statementExecutor = new StatementExecutor(ksqlEngine, statementParser);
  }

  @After
  public void tearDown() throws IOException {
    ksqlEngine.close();
  }

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @Test
  public void shouldHandleCorrectDDLStatement() throws Exception {
    Command command = new Command("REGISTER TOPIC users_topic WITH (value_format = 'json', "
        + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_CorrectTopicGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.SUCCESS);

  }

  @Test
  public void shouldHandleIncorrectDDLStatement() throws Exception {
    Command command = new Command("REGIST ER TOPIC users_topic WITH (value_format = 'json', "
        + "kafka_topic='user_topic_json');", new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC, "_IncorrectTopicGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.ERROR);

  }

  @Test
  public void shouldNotRunNullStatementList() {
    try{
      statementExecutor.handleRestoration(null);
    } catch (Exception nex) {
      assertThat("Statement list should not be null.", nex instanceof NullPointerException);
    }


  }

  @Test
  public void shouldHandleCSAS_CTASStatement() throws Exception {

    Command topicCommand = new Command("REGISTER TOPIC pageview_topic WITH "
        + "(value_format = 'json', "
        + "kafka_topic='pageview_topic_json');", new HashMap<>());
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(topicCommand, topicCommandId);

    Command csCommand = new Command("CREATE STREAM pageview "
        + "(viewtime bigint, pageid varchar, userid varchar) "
        + "WITH (registered_topic = 'pageview_topic');",
        new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(csCommand, csCommandId);

    Command csasCommand = new Command("CREATE STREAM user1pv "
        + " AS select * from pageview WHERE userid = 'user1';",
        new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(csasCommand, csasCommandId);

    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
        + " AS select * from pageview window tumbling(size 5 "
        + "second) WHERE userid = "
        + "'user1' group by pageid;",
        new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen", CommandId.Action.CREATE);

    statementExecutor.handleStatement(ctasCommand, ctasCommandId);

    Command terminateCommand = new Command("TERMINATE CSAS_USER1PV;",
                                      new HashMap<>());

    CommandId terminateCommandId =  new CommandId(CommandId.Type.TABLE, "_TerminateGen", CommandId.Action.CREATE);
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
  public void shouldHandlePriorStatement() throws Exception {
    TestUtils testUtils = new TestUtils();
    List<Pair<CommandId, Command>> priorCommands = testUtils.getAllPriorCommandRecords();
    final RestoreCommands restoreCommands = new RestoreCommands();
    priorCommands.forEach(pair -> restoreCommands.addCommand(pair.left, pair.right));

    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC, "_CSASTopicGen", CommandId.Action.CREATE);
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASStreamGen", CommandId.Action.CREATE);
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE, "_CTASGen", CommandId.Action.CREATE);

    statementExecutor.handleRestoration(restoreCommands);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(4, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }

}
