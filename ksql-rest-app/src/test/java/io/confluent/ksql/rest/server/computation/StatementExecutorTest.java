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
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

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
    Command command = new Command("REGISTER TOPIC users_topic "
                                  + "WITH (value_format = 'json', kafka_topic='user_topic_json');",
                                  new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC,
                                         "_CorrectTopicGen",
                                         CommandId.Action.CREATE);
    statementExecutor.handleStatement(command, commandId);
    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.SUCCESS);

  }

  @Test
  public void shouldHandleIncorrectDDLStatement() throws Exception {
    Command command = new Command("REGIST ER TOPIC users_topic "
                                  + "WITH (value_format = 'json', kafka_topic='user_topic_json');",
                                  new HashMap<>());
    CommandId commandId =  new CommandId(CommandId.Type.TOPIC,
                                         "_IncorrectTopicGen",
                                         CommandId.Action.CREATE);
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
    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC,
                                              "_CSASTopicGen",
                                              CommandId.Action.CREATE);
    statementExecutor.handleStatement(topicCommand, topicCommandId);

    Command csCommand = new Command("CREATE STREAM pageview "
        + "(viewtime bigint, pageid varchar, userid varchar) "
        + "WITH (registered_topic = 'pageview_topic');",
        new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    statementExecutor.handleStatement(csCommand, csCommandId);

    Command csasCommand = new Command("CREATE STREAM user1pv "
        + " AS select * from pageview WHERE userid = 'user1';",
        new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(csasCommand, csasCommandId);

    Command ctasCommand = new Command("CREATE TABLE user1pvtb "
        + " AS select * from pageview window tumbling(size 5 "
        + "second) WHERE userid = "
        + "'user1' group by pageid;",
        new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);

    statementExecutor.handleStatement(ctasCommand, ctasCommandId);

    Command terminateCommand = new Command("TERMINATE CSAS_USER1PV;",
                                      new HashMap<>());

    CommandId terminateCommandId =  new CommandId(CommandId.Type.TABLE,
                                                  "_TerminateGen",
                                                  CommandId.Action.CREATE);
    statementExecutor.handleStatement(terminateCommand, terminateCommandId);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    assertThat(statusStore.size(), equalTo(6));
    assertThat(statusStore.get(topicCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(csCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(csasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(ctasCommandId).getStatus(), equalTo(CommandStatus.Status.ERROR));
    assertThat(statusStore.get(terminateCommandId).getStatus(),
               equalTo(CommandStatus.Status.SUCCESS));

  }

  @Test
  public void shouldHandlePriorStatement() throws Exception {
    TestUtils testUtils = new TestUtils();
    List<Pair<CommandId, Command>> priorCommands = testUtils.getAllPriorCommandRecords();
    final RestoreCommands restoreCommands = new RestoreCommands();
    priorCommands.forEach(pair -> restoreCommands.addCommand(pair.left, pair.right));

    CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC,
                                              "_CSASTopicGen",
                                              CommandId.Action.CREATE);
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);

    statementExecutor.handleRestoration(restoreCommands);

    Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(4, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }


  @Test
  public void shouldEnforceReferentialIntegrity() throws Exception {

    // First create streams/tables and start queries
    createStrreamsAndTables();

    // Now try to drop streams/tables to test referential integrity
    tryDropThatViolatesReferentialIntegrity();


    // Terminate the queries using the stream/table
    terminateQueries();

    // Now drop should be successful
    Command dropTableCommand2 = new Command("drop table table1;", new HashMap<>());
    CommandId dropTableCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    statementExecutor.handleStatement(dropTableCommand2, dropTableCommandId2);

    // DROP should succed since no query is using the table
    Optional<CommandStatus> dropTableCommandStatus2 =
        statementExecutor.getStatus(dropTableCommandId2);

    Assert.assertTrue(dropTableCommandStatus2.isPresent());
    assertThat(dropTableCommandStatus2.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));


    // DROP should succeed since no query is using the stream.
    Command dropStreamCommand3 = new Command("drop stream pageview;", new HashMap<>());
    CommandId dropStreamCommandId3 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    statementExecutor.handleStatement(dropStreamCommand3, dropStreamCommandId3);

    Optional<CommandStatus> dropStreamCommandStatus3 =
        statementExecutor.getStatus(dropStreamCommandId3);
    assertThat(dropStreamCommandStatus3.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.SUCCESS));

  }

  private void createStrreamsAndTables() throws Exception {
    Command csCommand = new Command("CREATE STREAM pageview ("
                                    + "viewtime bigint,"
                                    + " pageid varchar, "
                                    + "userid varchar) "
                                    + "WITH (kafka_topic = 'pageview_topic_json', "
                                    + "value_format = 'json');",
                                    new HashMap<>());
    CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    statementExecutor.handleStatement(csCommand, csCommandId);

    Command csasCommand = new Command("CREATE STREAM user1pv AS "
                                      + "select * from pageview"
                                      + " WHERE userid = 'user1';",
                                      new HashMap<>());

    CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(csasCommand, csasCommandId);

    Command ctasCommand = new Command("CREATE TABLE table1  AS "
                                      + "SELECT pageid, count(pageid) "
                                      + "FROM pageview "
                                      + "WINDOW TUMBLING ( SIZE 10 SECONDS) "
                                      + "GROUP BY pageid;",
                                      new HashMap<>());

    CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(ctasCommand, ctasCommandId);
  }

  private void tryDropThatViolatesReferentialIntegrity() throws Exception {
    Command dropStreamCommand1 = new Command("drop stream pageview;", new HashMap<>());
    CommandId dropStreamCommandId1 =  new CommandId(CommandId.Type.STREAM,
                                                    "_PAGEVIEW",
                                                    CommandId.Action.DROP);
    statementExecutor.handleStatement(dropStreamCommand1, dropStreamCommandId1);

    // DROP statement should fail since the stream is being used.
    Optional<CommandStatus> dropStreamCommandStatus1 =
        statementExecutor.getStatus(dropStreamCommandId1);

    Assert.assertTrue(dropStreamCommandStatus1.isPresent());
    assertThat(dropStreamCommandStatus1.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    Assert.assertTrue(
        dropStreamCommandStatus1
            .get()
            .getMessage()
            .startsWith("io.confluent.ksql.util.KsqlReferentialIntegrityException: "
                        + "Cannot drop the data source. The following queries read from this source:"
                        + " [CSAS_USER1PV, CTAS_TABLE1] and the following queries write into this "
                        + "source: []. You need to terminate them before dropping this source."));


    Command dropStreamCommand2 = new Command("drop stream user1pv;", new HashMap<>());
    CommandId dropStreamCommandId2 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    statementExecutor.handleStatement(dropStreamCommand2, dropStreamCommandId2);

    // DROP statement should fail since the stream is being used.
    Optional<CommandStatus> dropStreamCommandStatus2 =
        statementExecutor.getStatus(dropStreamCommandId2);

    assertThat(dropStreamCommandStatus2.isPresent(), equalTo(true));
    assertThat(dropStreamCommandStatus2.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString(
            "io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop the "
            + "data source. The following queries read from this source: [] and the "
            + "following queries write into this source: [CSAS_USER1PV]. You need to "
            + "terminate them before dropping this source."));

    Command dropTableCommand1 = new Command("drop table table1;", new HashMap<>());
    CommandId dropTableCommandId1 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    statementExecutor.handleStatement(dropTableCommand1, dropTableCommandId1);

    Optional<CommandStatus> dropTableCommandStatus1 =
        statementExecutor.getStatus(dropTableCommandId1);

    // DROP statement should fail since the table is being used.
    Assert.assertTrue(dropTableCommandStatus1.isPresent());
    assertThat(dropTableCommandStatus1.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString(
            "io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop the "
            + "data source. The following queries read from this source: [] and the following "
            + "queries write into this source: [CTAS_TABLE1]. You need to terminate them before "
            + "dropping this source."));


  }

  private void terminateQueries() throws Exception {
    Command terminateCommand1 = new Command("TERMINATE CSAS_USER1PV;", new HashMap<>());
    CommandId terminateCommandId1 =
        new CommandId(CommandId.Type.STREAM, "_TerminateGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(terminateCommand1, terminateCommandId1);
    Optional<CommandStatus> terminateCommandStatus1 =
        statementExecutor.getStatus(terminateCommandId1);
    assertThat(terminateCommandStatus1.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    Command terminateCommand2 = new Command("TERMINATE CTAS_TABLE1;", new HashMap<>());
    CommandId terminateCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TerminateGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(terminateCommand2, terminateCommandId2);
    Optional<CommandStatus> terminateCommandStatus2 =
        statementExecutor.getStatus(terminateCommandId2);
    assertThat(terminateCommandStatus2.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

}
