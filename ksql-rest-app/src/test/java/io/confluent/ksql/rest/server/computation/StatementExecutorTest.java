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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reportMatcher;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.commands.DdlCommandExec;
import io.confluent.ksql.ddl.commands.DdlCommandResult;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("ConstantConditions")
public class StatementExecutorTest extends EasyMockSupport {

  private KsqlEngine ksqlEngine;
  private StatementExecutor statementExecutor;
  private KsqlConfig ksqlConfig;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    final Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", CLUSTER.bootstrapServers());

    ksqlConfig = new KsqlConfig(props);
    ksqlEngine = TestUtils.createKsqlEngine(
        ksqlConfig,
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get);

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    statementExecutor = new StatementExecutor(ksqlConfig, ksqlEngine, statementParser, mock(CommandStore.class));
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
  }

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster.build();

  @Test
  public void shouldHandleCorrectDDLStatement() {
    final Command command = new Command("REGISTER TOPIC users_topic "
                                  + "WITH (value_format = 'json', kafka_topic='user_topic_json');",
                                  Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.TOPIC,
                                         "_CorrectTopicGen",
                                         CommandId.Action.CREATE);
    statementExecutor.handleStatement(command, commandId, Optional.empty());
    final Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.SUCCESS);

  }

  @Test
  public void shouldHandleIncorrectDDLStatement() {
    final Command command = new Command("REGIST ER TOPIC users_topic "
                                  + "WITH (value_format = 'json', kafka_topic='user_topic_json');",
                                  Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.TOPIC,
                                         "_IncorrectTopicGen",
                                         CommandId.Action.CREATE);
    statementExecutor.handleStatement(command, commandId, Optional.empty());
    final Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(statusStore.size(), 1);
    Assert.assertEquals(statusStore.get(commandId).getStatus(), CommandStatus.Status.ERROR);
  }

  @Test
  public void shouldThrowOnUnexpectedException() {
    // Given:
    final String statementText = "mama said knock you out";
    final StatementParser statementParser = mock(StatementParser.class);
    final KsqlEngine mockEngine = mock(KsqlEngine.class);
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig, mockEngine, statementParser, niceMock(CommandStore.class));
    final RuntimeException exception = new RuntimeException("i'm gonna knock you out");
    expect(statementParser.parseSingleStatement(statementText)).andThrow(
        exception);
    expect(mockEngine.isAcceptingStatements()).andStubReturn(true);
    final Command command = new Command(
        statementText,
        Collections.emptyMap(),
        Collections.emptyMap());
    final CommandId commandId =  new CommandId(
        CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    replay(statementParser, mockEngine);

    // When:
    try {
      statementExecutor.handleStatement(command, commandId, Optional.empty());
      Assert.fail("handleStatement should throw");
    } catch (final RuntimeException caughtException) {
      // Then:
      assertThat(caughtException, is(exception));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildQueriesWithPersistedConfig() {
    final KsqlConfig originalConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "not-the-default"));

    // get a statement instance
    final String ddlText
        = "CREATE STREAM pageviews (viewtime bigint, pageid varchar) " +
        "WITH (kafka_topic='pageview_topic', VALUE_FORMAT='json');";
    final String statementText
        = "CREATE STREAM user1pv AS select * from pageviews WHERE userid = 'user1';";
    final StatementParser realParser = new StatementParser(ksqlEngine);
    final DdlStatement ddlStatement = (DdlStatement) realParser.parseSingleStatement(ddlText);
    ksqlEngine.executeDdlStatement(ddlText, ddlStatement, Collections.emptyMap());
    final CreateStreamAsSelect csasStatement =
        (CreateStreamAsSelect) realParser.parseSingleStatement(statementText);

    final StatementParser statementParser = mock(StatementParser.class);
    final KsqlEngine mockEngine = mock(KsqlEngine.class);
    final MetaStore mockMetaStore = mock(MetaStore.class);
    final PersistentQueryMetadata mockQueryMetadata = mock(PersistentQueryMetadata.class);
    expect(mockQueryMetadata.getQueryId()).andReturn(mock(QueryId.class));

    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final KsqlConfig expectedConfig = ksqlConfig.overrideBreakingConfigsWithOriginalValues(
        originalConfig.getAllConfigPropsWithSecretsObfuscated());

    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig, mockEngine, statementParser, mock(CommandStore.class));

    final Command csasCommand = new Command(
        statementText,
        Collections.emptyMap(),
        originalConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csasCommandId =  new CommandId(
        CommandId.Type.STREAM,
        "_CSASGen",
        CommandId.Action.CREATE);

    expect(statementParser.parseSingleStatement(statementText)).andReturn(csasStatement);
    expect(
        mockEngine.addInto(
            (QuerySpecification)csasStatement.getQuery().getQueryBody(),
            csasStatement.getName().getSuffix(),
            csasStatement.getQuery().getLimit(),
            csasStatement.getProperties(),
            csasStatement.getPartitionByColumn(),
            true))
        .andReturn(csasStatement.getQuery());
    expect(mockEngine.getMetaStore()).andReturn(mockMetaStore);
    expect(mockMetaStore.getSource(anyObject())).andReturn(null);
    expect(mockEngine.buildMultipleQueries(statementText, expectedConfig, Collections.emptyMap()))
        .andReturn(Collections.singletonList(mockQueryMetadata));
    mockQueryMetadata.start();
    expectLastCall();

    expect(mockEngine.isAcceptingStatements()).andReturn(true).anyTimes();

    replay(statementParser, mockEngine, mockMetaStore, mockQueryMetadata);

    statementExecutor.handleStatement(csasCommand, csasCommandId, Optional.empty());

    verify(statementParser, mockEngine, mockMetaStore, mockQueryMetadata);
  }

  @Test
  public void shouldHandleCSAS_CTASStatement() {

    final Command topicCommand = new Command("REGISTER TOPIC pageview_topic WITH "
        + "(value_format = 'json', "
        + "kafka_topic='pageview_topic_json');", Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC,
                                              "_CSASTopicGen",
                                              CommandId.Action.CREATE);
    statementExecutor.handleStatement(topicCommand, topicCommandId, Optional.empty());

    final Command csCommand = new Command("CREATE STREAM pageview "
        + "(viewtime bigint, pageid varchar, userid varchar) "
        + "WITH (registered_topic = 'pageview_topic');",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    statementExecutor.handleStatement(csCommand, csCommandId, Optional.empty());

    final Command csasCommand = new Command("CREATE STREAM user1pv "
        + " AS select * from pageview WHERE userid = 'user1';",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(csasCommand, csasCommandId, Optional.empty());

    final Command badCtasCommand = new Command("CREATE TABLE user1pvtb "
        + " AS select * from pageview window tumbling(size 5 "
        + "second) WHERE userid = "
        + "'user1' group by pageid;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);

    statementExecutor.handleStatement(badCtasCommand, ctasCommandId, Optional.empty());

    final Command terminateCommand = new Command(
        "TERMINATE CSAS_USER1PV_0;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId terminateCmdId =  new CommandId(CommandId.Type.TABLE,
                                                  "_TerminateGen",
                                                  CommandId.Action.CREATE);
    statementExecutor.handleStatement(terminateCommand, terminateCmdId, Optional.empty());

    final Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    assertThat(statusStore, is(notNullValue()));
    assertThat(statusStore.keySet(),
        containsInAnyOrder(topicCommandId, csCommandId, csasCommandId, ctasCommandId, terminateCmdId));

    assertThat(statusStore.get(topicCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(csCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(csasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(statusStore.get(ctasCommandId).getStatus(), equalTo(CommandStatus.Status.ERROR));
    assertThat(statusStore.get(terminateCmdId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

  private static class StatusMatcher implements IArgumentMatcher {
    final CommandStatus.Status status;

    StatusMatcher(final CommandStatus.Status status) {
      this.status = status;
    }

    @Override
    public boolean matches(final Object item) {
      return item instanceof CommandStatus
          && ((CommandStatus) item).getStatus().equals(status);
    }

    @Override
    public void appendTo(final StringBuffer buffer) {
      buffer.append("status(").append(status).append(")");
    }
  }

  private CommandStatus sameStatus(final CommandStatus.Status status) {
    reportMatcher(new StatusMatcher(status));
    return null;
  }

  @Test
  public void shouldCompleteFutureOnSuccess() {
    final Command command = new Command(
        "CREATE STREAM foo ("
            + "biz bigint,"
            + " baz varchar) "
            + "WITH (kafka_topic = 'foo', "
            + "value_format = 'json');",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.STREAM,
        "foo",
        CommandId.Action.CREATE);
    final QueuedCommandStatus status = mock(QueuedCommandStatus.class);
    status.setStatus(sameStatus(CommandStatus.Status.PARSING));
    expectLastCall();
    status.setStatus(sameStatus(CommandStatus.Status.EXECUTING));
    expectLastCall();
    status.setFinalStatus(sameStatus(CommandStatus.Status.SUCCESS));
    expectLastCall();
    replay(status);

    statementExecutor.handleStatement(command, commandId, Optional.of(status));

    verify(status);
  }

  @Test
  public void shouldCompleteFutureOnFailure() {
    shouldCompleteFutureOnSuccess();

    final Command command = new Command(
        "CREATE STREAM foo ("
            + "biz bigint,"
            + " baz varchar) "
            + "WITH (kafka_topic = 'foo', "
            + "value_format = 'json');",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.STREAM,
        "foo",
        CommandId.Action.CREATE);
    final QueuedCommandStatus status = mock(QueuedCommandStatus.class);

    status.setStatus(sameStatus(CommandStatus.Status.PARSING));
    expectLastCall();
    status.setFinalStatus(sameStatus(CommandStatus.Status.ERROR));
    expectLastCall();
    replay(status);

    statementExecutor.handleStatement(command, commandId, Optional.of(status));

    verify(status);
  }

  @Test
  public void shouldHandlePriorStatements() {
    final TestUtils testUtils = new TestUtils();
    final List<Pair<CommandId, Command>> priorCommands = testUtils.getAllPriorCommandRecords();
    final RestoreCommands restoreCommands = new RestoreCommands();
    priorCommands.forEach(pair -> restoreCommands.addCommand(pair.left, pair.right));

    final CommandId topicCommandId =  new CommandId(CommandId.Type.TOPIC,
                                              "_CSASTopicGen",
                                              CommandId.Action.CREATE);
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);

    restoreCommands.forEach(
        (command, commandId, terminatedQueries, wasDropped) ->
            statementExecutor.handleStatementWithTerminatedQueries(
                commandId,
                command,
                Optional.empty(),
                terminatedQueries,
                wasDropped
        )
    );

    final Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(4, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(topicCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }


  @Test
  public void shouldEnforceReferentialIntegrity() {

    // First create streams/tables and start queries
    createStreamsAndTables();

    // Now try to drop streams/tables to test referential integrity
    tryDropThatViolatesReferentialIntegrity();


    // Terminate the queries using the stream/table
    terminateQueries();

    // Now drop should be successful
    final Command dropTableCommand2 = new Command(
        "drop table table1;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropTableCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    statementExecutor.handleStatement(
        dropTableCommand2, dropTableCommandId2, Optional.empty());

    // DROP should succed since no query is using the table
    final Optional<CommandStatus> dropTableCommandStatus2 =
        statementExecutor.getStatus(dropTableCommandId2);

    Assert.assertTrue(dropTableCommandStatus2.isPresent());
    assertThat(dropTableCommandStatus2.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));


    // DROP should succeed since no query is using the stream.
    final Command dropStreamCommand3 = new Command(
        "drop stream pageview;", Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId3 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    statementExecutor.handleStatement(
        dropStreamCommand3, dropStreamCommandId3, Optional.empty());

    final Optional<CommandStatus> dropStreamCommandStatus3 =
        statementExecutor.getStatus(dropStreamCommandId3);
    assertThat(dropStreamCommandStatus3.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.SUCCESS));

  }

  @Test
  public void shouldTerminateClusterCorrectly() throws IOException {
    // Given:
    final KsqlConfig mockitoKsqlConfig = Mockito.mock(KsqlConfig.class);
    final KsqlEngine mockitoKsqlEngine = Mockito.mock(KsqlEngine.class);
    final StatementParser mockitoStatementParser = Mockito.mock(StatementParser.class);
    final CommandStore mockitoCommandStore = Mockito.mock(CommandStore.class);
    final ClusterTerminator clusterTerminator = Mockito.mock(ClusterTerminator.class);
    final StatementExecutor terminateStatementExecutor = new StatementExecutor(
        mockitoKsqlConfig, mockitoKsqlEngine, mockitoStatementParser, mockitoCommandStore
    );
    final CommandId commandId = Mockito.mock(CommandId.class);
    final Command command = new Command(
        TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT,
        Collections.emptyMap(),
        Collections.emptyMap());

    // When:
    terminateStatementExecutor.handleStatement(command, commandId, Optional.empty());

    // Then:
    Mockito.verify(clusterTerminator).terminateCluster(Mockito.anyList());
  }

  private void createStreamsAndTables() {
    final Command csCommand = new Command(
        "CREATE STREAM pageview ("
            + "viewtime bigint,"
            + " pageid varchar, "
            + "userid varchar) "
            + "WITH (kafka_topic = 'pageview_topic_json', "
            + "value_format = 'json');",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    statementExecutor.handleStatement(csCommand, csCommandId, Optional.empty());

    final Command csasCommand = new Command(
        "CREATE STREAM user1pv AS "
            + "select * from pageview"
            + " WHERE userid = 'user1';",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(csasCommand, csasCommandId, Optional.empty());

    final Command ctasCommand = new Command(
        "CREATE TABLE table1  AS "
            + "SELECT pageid, count(pageid) "
            + "FROM pageview "
            + "WINDOW TUMBLING ( SIZE 10 SECONDS) "
            + "GROUP BY pageid;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);
    statementExecutor.handleStatement(ctasCommand, ctasCommandId, Optional.empty());
  }

  private void tryDropThatViolatesReferentialIntegrity() {
    final Command dropStreamCommand1 = new Command(
        "drop stream pageview;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId1 =  new CommandId(CommandId.Type.STREAM,
                                                    "_PAGEVIEW",
                                                    CommandId.Action.DROP);
    statementExecutor.handleStatement(
        dropStreamCommand1, dropStreamCommandId1, Optional.empty());

    // DROP statement should fail since the stream is being used.
    final Optional<CommandStatus> dropStreamCommandStatus1 =
        statementExecutor.getStatus(dropStreamCommandId1);

    Assert.assertTrue(dropStreamCommandStatus1.isPresent());
    assertThat(dropStreamCommandStatus1.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    assertThat(
        dropStreamCommandStatus1
            .get()
            .getMessage(),
        containsString("io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop PAGEVIEW."));
    assertThat(
        dropStreamCommandStatus1
            .get()
            .getMessage(),
        containsString("The following queries read from this source: [CTAS_TABLE1_1, CSAS_USER1PV_0]."));
    assertThat(
        dropStreamCommandStatus1
            .get()
            .getMessage(),
        containsString("The following queries write into this source: []."));
    assertThat(
        dropStreamCommandStatus1
            .get()
            .getMessage(),
        containsString("You need to terminate them before dropping PAGEVIEW."));


    final Command dropStreamCommand2 = new Command(
        "drop stream user1pv;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId2 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    statementExecutor.handleStatement(
        dropStreamCommand2, dropStreamCommandId2, Optional.empty());

    // DROP statement should fail since the stream is being used.
    final Optional<CommandStatus> dropStreamCommandStatus2 =
        statementExecutor.getStatus(dropStreamCommandId2);

    assertThat(dropStreamCommandStatus2.isPresent(), equalTo(true));
    assertThat(dropStreamCommandStatus2.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString(
            "io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop USER1PV. \n"));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString(
             "The following queries read from this source: []."));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString(
            "The following queries write into this source: [CSAS_USER1PV_0]."));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString("You need to terminate them before dropping USER1PV."));

    final Command dropTableCommand1 = new Command(
        "drop table table1;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropTableCommandId1 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    statementExecutor.handleStatement(
        dropTableCommand1, dropTableCommandId1, Optional.empty());

    final Optional<CommandStatus> dropTableCommandStatus1 =
        statementExecutor.getStatus(dropTableCommandId1);

    // DROP statement should fail since the table is being used.
    Assert.assertTrue(dropTableCommandStatus1.isPresent());
    assertThat(dropTableCommandStatus1.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.ERROR));
    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString(
            "io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop TABLE1."));

    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString(
            "The following queries read from this source: []."));

    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString(
            "The following queries write into this source: [CTAS_TABLE1_1]."));

    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString("You need to terminate them before dropping TABLE1."));


  }

  private void terminateQueries() {
    final Command terminateCommand1 = new Command(
        "TERMINATE CSAS_USER1PV_0;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId1 =
        new CommandId(CommandId.Type.STREAM, "_TerminateGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(
        terminateCommand1, terminateCommandId1, Optional.empty());
    final Optional<CommandStatus> terminateCommandStatus1 =
        statementExecutor.getStatus(terminateCommandId1);
    assertThat(terminateCommandStatus1.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    final Command terminateCommand2 = new Command(
        "TERMINATE CTAS_TABLE1_1;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TerminateGen", CommandId.Action.CREATE);
    statementExecutor.handleStatement(
        terminateCommand2, terminateCommandId2, Optional.empty());
    final Optional<CommandStatus> terminateCommandStatus2 =
        statementExecutor.getStatus(terminateCommandId2);
    assertThat(terminateCommandStatus2.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

}
