/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlEngineTestUtil;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@SuppressWarnings("ConstantConditions")
public class StatementExecutorTest extends EasyMockSupport {

  private static final Map<String, String> PRE_VERSION_5_NULL_ORIGNAL_PROPS = null;

  private KsqlEngine ksqlEngine;
  private StatementExecutor statementExecutor;
  private KsqlConfig ksqlConfig;

  private final StatementParser mockParser = niceMock(StatementParser.class);
  private final KsqlEngine mockEngine = strictMock(KsqlEngine.class);
  private final MetaStore mockMetaStore = niceMock(MetaStore.class);
  private final PersistentQueryMetadata mockQueryMetadata
      = niceMock(PersistentQueryMetadata.class);
  private StatementExecutor statementExecutorWithMocks;
  private ServiceContext serviceContext;

  @Before
  public void setUp() {
    final Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", CLUSTER.bootstrapServers());

    ksqlConfig = new KsqlConfig(props);
    serviceContext = TestServiceContext.create(new MockKafkaTopicClient());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        new MetaStoreImpl(new InternalFunctionRegistry())
    );

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    statementExecutor = new StatementExecutor(ksqlConfig, ksqlEngine, statementParser);
    statementExecutorWithMocks
        = new StatementExecutor(ksqlConfig, mockEngine, mockParser);
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster.build();

  private void handleStatement(
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatus) {
    handleStatement(statementExecutor, command, commandId, commandStatus);
  }

  private static void handleStatement(
      final StatementExecutor statementExecutor,
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatus) {
    statementExecutor.handleStatement(new QueuedCommand(commandId, command, commandStatus));
  }

  @Test
  public void shouldHandleCorrectDDLStatement() {
    final Command command = new Command("REGISTER TOPIC users_topic "
                                  + "WITH (value_format = 'json', kafka_topic='user_topic_json');",
                                  Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.TOPIC,
                                         "_CorrectTopicGen",
                                         CommandId.Action.CREATE);
    handleStatement(command, commandId, Optional.empty());
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
    handleStatement(command, commandId, Optional.empty());
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
        ksqlConfig, mockEngine, statementParser);
    final RuntimeException exception = new RuntimeException("i'm gonna knock you out");
    expect(statementParser.parseSingleStatement(statementText)).andThrow(
        exception);
    final Command command = new Command(
        statementText,
        Collections.emptyMap(),
        Collections.emptyMap());
    final CommandId commandId =  new CommandId(
        CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    replay(statementParser);

    // When:
    try {
      handleStatement(statementExecutor, command, commandId, Optional.empty());
      Assert.fail("handleStatement should throw");
    } catch (final RuntimeException caughtException) {
      // Then:
      assertThat(caughtException, is(exception));
    }
  }

  @Test
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
    final PreparedStatement<?> ddlStatement = realParser.parseSingleStatement(ddlText);
    ksqlEngine.execute(ddlStatement, originalConfig, Collections.emptyMap());

    final PreparedStatement<Statement> csasStatement =
        realParser.parseSingleStatement(statementText);

    expect(mockQueryMetadata.getQueryId()).andStubReturn(mock(QueryId.class));

    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final KsqlConfig expectedConfig = ksqlConfig.overrideBreakingConfigsWithOriginalValues(
        originalConfig.getAllConfigPropsWithSecretsObfuscated());

    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig, mockEngine, mockParser);

    final Command csasCommand = new Command(
        statementText,
        Collections.emptyMap(),
        originalConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csasCommandId =  new CommandId(
        CommandId.Type.STREAM,
        "_CSASGen",
        CommandId.Action.CREATE);

    expect(mockParser.parseSingleStatement(statementText)).andReturn(csasStatement);
    expect(mockEngine.numberOfPersistentQueries()).andReturn(0L);
    expect(mockEngine.execute(csasStatement, expectedConfig, Collections.emptyMap()))
        .andReturn(ExecuteResult.of(mockQueryMetadata));
    mockQueryMetadata.start();
    expectLastCall();

    replay(mockParser, mockEngine, mockMetaStore, mockQueryMetadata);

    handleStatement(statementExecutor, csasCommand, csasCommandId, Optional.empty());

    verify(mockParser, mockEngine, mockMetaStore, mockQueryMetadata);
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
    handleStatement(topicCommand, topicCommandId, Optional.empty());

    final Command csCommand = new Command("CREATE STREAM pageview "
        + "(viewtime bigint, pageid varchar, userid varchar) "
        + "WITH (registered_topic = 'pageview_topic');",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
                                           "_CSASStreamGen",
                                           CommandId.Action.CREATE);
    handleStatement(csCommand, csCommandId, Optional.empty());

    final Command csasCommand = new Command("CREATE STREAM user1pv "
        + " AS select * from pageview WHERE userid = 'user1';",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    handleStatement(csasCommand, csasCommandId, Optional.empty());

    final Command badCtasCommand = new Command("CREATE TABLE user1pvtb "
        + " AS select * from pageview window tumbling(size 5 "
        + "second) WHERE userid = "
        + "'user1' group by pageid;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
                                             "_CTASGen",
                                             CommandId.Action.CREATE);

    handleStatement(badCtasCommand, ctasCommandId, Optional.empty());

    final Command terminateCommand = new Command(
        "TERMINATE CSAS_USER1PV_0;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId terminateCmdId =  new CommandId(CommandId.Type.TABLE,
                                                  "_TerminateGen",
                                                  CommandId.Action.CREATE);
    handleStatement(terminateCommand, terminateCmdId, Optional.empty());

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

  private static CommandStatus sameStatus(final CommandStatus.Status status) {
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
    final CommandStatusFuture status = mock(CommandStatusFuture.class);
    status.setStatus(sameStatus(CommandStatus.Status.PARSING));
    expectLastCall();
    status.setStatus(sameStatus(CommandStatus.Status.EXECUTING));
    expectLastCall();
    status.setFinalStatus(sameStatus(CommandStatus.Status.SUCCESS));
    expectLastCall();
    replay(status);

    handleStatement(command, commandId, Optional.of(status));

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
    final CommandStatusFuture status = mock(CommandStatusFuture.class);

    status.setStatus(sameStatus(CommandStatus.Status.PARSING));
    expectLastCall();
    status.setFinalStatus(sameStatus(CommandStatus.Status.ERROR));
    expectLastCall();
    replay(status);

    handleStatement(command, commandId, Optional.of(status));

    verify(status);
  }

  @Test
  public void shouldHandlePriorStatements() {
    final TestUtils testUtils = new TestUtils();
    final List<Pair<CommandId, Command>> priorCommands = testUtils.getAllPriorCommandRecords();
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

    priorCommands.forEach(
        pair -> statementExecutor.handleRestore(
            new QueuedCommand(pair.left, pair.right)
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

    createStreamsAndStartTwoPersistentQueries();

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
    handleStatement(
        statementExecutor, dropTableCommand2, dropTableCommandId2, Optional.empty());

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
    handleStatement(
        statementExecutor, dropStreamCommand3, dropStreamCommandId3, Optional.empty());

    final Optional<CommandStatus> dropStreamCommandStatus3 =
        statementExecutor.getStatus(dropStreamCommandId3);
    assertThat(dropStreamCommandStatus3.get().getStatus(),
               CoreMatchers.equalTo(CommandStatus.Status.SUCCESS));
  }

  private Query mockCSASQuery(final String name) {
    final Query mockQuery = mock(Query.class);
    final QuerySpecification mockQuerySpec = mock(QuerySpecification.class);
    final Table mockRelation = mock(Table.class);
    expect(mockQuery.getQueryBody()).andStubReturn(mockQuerySpec);
    expect(mockQuery.getLimit()).andStubReturn(Optional.empty());
    expect(mockQuerySpec.getInto()).andStubReturn(mockRelation);
    expect(mockRelation.getName()).andStubReturn(QualifiedName.of(name));
    return mockQuery;
  }

  private CreateStreamAsSelect mockCSAS(final String name) {
    final CreateStreamAsSelect mockStatement = mock(CreateStreamAsSelect.class);
    expect(mockStatement.getName()).andStubReturn(QualifiedName.of(name));
    expect(mockStatement.getQuery()).andStubReturn(mockCSASQuery(name));
    expect(mockStatement.getProperties()).andStubReturn(Collections.emptyMap());
    expect(mockStatement.getPartitionByColumn()).andStubReturn(Optional.empty());
    return mockStatement;
  }

  private DropStream mockDropStream(final String name) {
    final DropStream mockDropStream = mock(DropStream.class);
    expect(mockDropStream.getName()).andStubReturn(QualifiedName.of(name));
    expect(mockDropStream.getStreamName()).andStubReturn(QualifiedName.of(name));
    expect(mockParser.parseSingleStatement("DROP"))
        .andReturn(PreparedStatement.of("DROP", mockDropStream));
    return mockDropStream;
  }

  private Statement mockTerminate(final QueryId queryId) {
    final TerminateQuery mockStatement = mock(TerminateQuery.class);
    expect(mockStatement.getQueryId()).andStubReturn(queryId);
    return mockStatement;
  }

  private PersistentQueryMetadata mockReplayCSAS(
      final String statement,
      final String name,
      final QueryId queryId) {
    final CreateStreamAsSelect mockCSAS = mockCSAS(name);
    final PersistentQueryMetadata mockQuery = mock(PersistentQueryMetadata.class);
    expect(mockQuery.getQueryId()).andStubReturn(queryId);
    final PreparedStatement<Statement> csas = PreparedStatement.of("CSAS", mockCSAS);
    expect(mockParser.parseSingleStatement(statement))
        .andReturn(csas);
    expect(mockMetaStore.getSource(name)).andStubReturn(null);
    expect(mockEngine.numberOfPersistentQueries()).andReturn(0L);
    expect(mockEngine.execute(eq(csas), anyObject(), anyObject()))
        .andReturn(ExecuteResult.of(mockQuery));
    return mockQuery;
  }

  private PersistentQueryMetadata mockReplayRunScript(
      final String runScriptStatement,
      final String queryStatement
  ) {
    final PersistentQueryMetadata mockQuery = mock(PersistentQueryMetadata.class);
    final Statement mockRunScript = mock(RunScript.class);
    final PreparedStatement<Statement> runScript =
        PreparedStatement.of(queryStatement, mockRunScript);
    expect(mockParser.parseSingleStatement(runScriptStatement))
        .andReturn(runScript);
    final ImmutableList<PreparedStatement<?>> statements = ImmutableList
        .of(PreparedStatement.of(queryStatement, mock(Statement.class)));
    expect(mockEngine.parseStatements(eq(queryStatement))).andReturn(statements);
    expect(mockEngine.execute(eq(statements.get(0)), anyObject(), anyObject()))
        .andReturn(ExecuteResult.of(mockQuery));
    expect(mockEngine.numberOfPersistentQueries()).andReturn(0L);
    return mockQuery;
  }

  @Test
  public void shouldSkipStartWhenReplayingLog() {
    // Given:
    final QueryId queryId = new QueryId("csas-query-id");
    final String name = "foo";
    final PersistentQueryMetadata mockQuery = mockReplayCSAS("CSAS", name, queryId);
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, name, Action.CREATE),
            new Command("CSAS", Collections.emptyMap(), Collections.emptyMap())
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockQuery);
  }

  @Test
  public void shouldCascade4Dot1DropStreamCommand() {
    // Given:
    final DropStream mockDropStream = mockDropStream("foo");
    expect(mockMetaStore.getSource("foo"))
        .andStubReturn(mock(StructuredDataSource.class));
    expect(mockMetaStore.getQueriesWithSink("foo"))
        .andStubReturn(ImmutableSet.of("query-id"));
    expect(mockEngine.getMetaStore()).andStubReturn(mockMetaStore);
    expect(mockEngine.getPersistentQuery(new QueryId("query-id"))).andReturn(Optional.of(mockQueryMetadata));
    mockQueryMetadata.close();
    expectLastCall();
    expect(mockEngine
        .execute(eq(PreparedStatement.of("DROP", mockDropStream)), anyObject(), anyObject()))
        .andReturn(ExecuteResult.of("SUCCESS"));
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, "foo", Action.DROP),
            new Command("DROP", Collections.emptyMap(), PRE_VERSION_5_NULL_ORIGNAL_PROPS)
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockMetaStore);
  }

  @Test
  public void shouldNotCascadeDropStreamCommand() {
    // Given:
    final String drop = "DROP";
    final DropStream mockDropStream = mockDropStream("foo");
    final PreparedStatement<DropStream> statement = PreparedStatement.of(drop, mockDropStream);

    expect(mockEngine.execute(eq(statement), anyObject(), anyObject()))
        .andReturn(ExecuteResult.of("SUCCESS"));
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, "foo", Action.DROP),
            new Command(drop, Collections.emptyMap(), Collections.emptyMap())
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockMetaStore);
  }

  @Test
  public void shouldFailCreateAsSelectIfExceedActivePersistentQueriesLimit() {
    // Given:
    createStreamsAndStartTwoPersistentQueries();
    // Prepare to try adding a third
    final KsqlConfig cmdConfig =
        givenCommandConfig(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 2);
    final Command csasCommand =
        givenCommand("CREATE STREAM user2pv AS select * from pageview;", cmdConfig);
    final CommandId csasCommandId =
        new CommandId(CommandId.Type.STREAM, "_CSASGen2", CommandId.Action.CREATE);

    // When:
    handleStatement(csasCommand, csasCommandId, Optional.empty());

    // Then:
    final CommandStatus commandStatus = getCommandStatus(csasCommandId);
    assertThat("CSAS statement should fail since exceeds limit of 2 active persistent queries",
        commandStatus.getStatus(), is(CommandStatus.Status.ERROR));
    assertThat(
        commandStatus.getMessage(),
        containsString("would cause the number of active, persistent queries "
            + "to exceed the configured limit"));
  }

  @Test
  public void shouldFailInsertIntoIfExceedActivePersistentQueriesLimit() {
    // Given:
    createStreamsAndStartTwoPersistentQueries();
    // Set limit and prepare to try adding a query that exceeds the limit
    final KsqlConfig cmdConfig =
        givenCommandConfig(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 1);
    final Command insertIntoCommand =
        givenCommand("INSERT INTO user1pv select * from pageview;", cmdConfig);
    final CommandId insertIntoCommandId =
        new CommandId(CommandId.Type.STREAM, "_InsertQuery1", CommandId.Action.CREATE);

    // When:
    handleStatement(insertIntoCommand, insertIntoCommandId, Optional.empty());

    // Then: statement should fail since exceeds limit of 1 active persistent query
    final CommandStatus commandStatus = getCommandStatus(insertIntoCommandId);
    assertThat(commandStatus.getStatus(), is(CommandStatus.Status.ERROR));
    assertThat(
        commandStatus.getMessage(),
        containsString("would cause the number of active, persistent queries "
            + "to exceed the configured limit"));
  }

  @Test
  public void shouldHandleRunScriptCommand() {
    // Given:
    final String runScriptStatement = "run script";
    final String queryStatement = "a query";
    final PersistentQueryMetadata mockQuery = mockReplayRunScript(runScriptStatement, queryStatement);
    mockQuery.start();
    expectLastCall().once();
    replayAll();

    // When:
    statementExecutorWithMocks.handleStatement(
        new QueuedCommand(
            new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE),
            new Command(
                runScriptStatement,
                Collections
                    .singletonMap(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, queryStatement),
                Collections.emptyMap())
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockQuery);
  }

  @Test
  public void shouldRestoreRunScriptCommand() {
    // Given:
    final String runScriptStatement = "run script";
    final String queryStatement = "a persistent query";
    final PersistentQueryMetadata mockQuery = mockReplayRunScript(runScriptStatement, queryStatement);
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE),
            new Command(
                runScriptStatement,
                Collections.singletonMap(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, queryStatement),
                Collections.emptyMap())
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockQuery);
  }

  private void createStreamsAndStartTwoPersistentQueries() {
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
    handleStatement(csCommand, csCommandId, Optional.empty());

    final Command csasCommand = new Command(
        "CREATE STREAM user1pv AS "
            + "select * from pageview"
            + " WHERE userid = 'user1';",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
                                             "_CSASGen",
                                             CommandId.Action.CREATE);
    handleStatement(csasCommand, csasCommandId, Optional.empty());

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
    handleStatement(ctasCommand, ctasCommandId, Optional.empty());

    assertThat(getCommandStatus(csCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(getCommandStatus(csasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(getCommandStatus(ctasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

  private void tryDropThatViolatesReferentialIntegrity() {
    final Command dropStreamCommand1 = new Command(
        "drop stream pageview;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId1 =  new CommandId(CommandId.Type.STREAM,
                                                    "_PAGEVIEW",
                                                    CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropStreamCommand1, dropStreamCommandId1, Optional.empty());

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
    handleStatement(
        statementExecutor, dropStreamCommand2, dropStreamCommandId2, Optional.empty());

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
            "io.confluent.ksql.util.KsqlReferentialIntegrityException: Cannot drop USER1PV."));
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
    handleStatement(
        statementExecutor, dropTableCommand1, dropTableCommandId1, Optional.empty());

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
    handleStatement(
        statementExecutor, terminateCommand1, terminateCommandId1, Optional.empty());
    assertThat(
        getCommandStatus(terminateCommandId1).getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    final Command terminateCommand2 = new Command(
        "TERMINATE CTAS_TABLE1_1;",
        Collections.emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TerminateGen", CommandId.Action.CREATE);
    handleStatement(
        statementExecutor, terminateCommand2, terminateCommandId2, Optional.empty());
    assertThat(
        getCommandStatus(terminateCommandId2).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

  private CommandStatus getCommandStatus(CommandId commandId) {
    final Optional<CommandStatus> commandStatus = statementExecutor.getStatus(commandId);
    assertThat("command not registered: " + commandId,
        commandStatus,
        is(not(equalTo(Optional.empty()))));
    return commandStatus.get();
  }

  private static KsqlConfig givenCommandConfig(final String name, final Object value) {
    return new KsqlConfig(Collections.singletonMap(name, value));
  }

  private static Command givenCommand(final String statementStr, final KsqlConfig ksqlConfig) {
    return new Command(
        statementStr, Collections.emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
  }
}
