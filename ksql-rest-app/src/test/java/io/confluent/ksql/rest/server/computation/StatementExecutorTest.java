/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static java.util.Collections.emptyMap;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reportMatcher;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMockSupport;
import org.easymock.IArgumentMatcher;
import org.easymock.Mock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.integration.EasyMock2Adapter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class StatementExecutorTest extends EasyMockSupport {

  private static final Map<String, String> PRE_VERSION_5_NULL_ORIGINAL_PROPS = null;

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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private QueuedCommand queuedCommand;

  @Before
  public void setUp() {
    ksqlConfig = KsqlConfigTestUtil.create(
        CLUSTER,
        ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://host:1234")
    );

    final FakeKafkaTopicClient fakeKafkaTopicClient = new FakeKafkaTopicClient();
    fakeKafkaTopicClient.createTopic("pageview_topic", 1, (short) 1, emptyMap());
    fakeKafkaTopicClient.createTopic("foo", 1, (short) 1, emptyMap());
    fakeKafkaTopicClient.createTopic("pageview_topic_json", 1, (short) 1, emptyMap());
    serviceContext = TestServiceContext.create(fakeKafkaTopicClient);
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        new MetaStoreImpl(new InternalFunctionRegistry())
    );

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    statementExecutor = new StatementExecutor(ksqlEngine, statementParser);
    statementExecutorWithMocks = new StatementExecutor(mockEngine, mockParser);

    statementExecutor.configure(ksqlConfig);
    statementExecutorWithMocks.configure(ksqlConfig);
  }

  @After
  public void tearDown() {
    ksqlEngine.close();
    serviceContext.close();
  }

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
    .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
    .around(CLUSTER);

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnConfigureIfAppServerNotSet() {
    // Given:
    final KsqlConfig configNoAppServer = new KsqlConfig(ImmutableMap.of());

    // When:
    statementExecutorWithMocks.configure(configNoAppServer);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnHandleStatementIfNotConfigured() {
    // Given:
    statementExecutor = new StatementExecutor(mockEngine, mockParser);

    // When:
    statementExecutor.handleStatement(queuedCommand);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnHandleRestoreIfNotConfigured() {
    // Given:
    statementExecutor = new StatementExecutor(mockEngine, mockParser);

    // When:
    statementExecutor.handleRestore(queuedCommand);
  }

  @Test
  public void shouldThrowOnUnexpectedException() {
    // Given:
    final String statementText = "mama said knock you out";

    final RuntimeException exception = new RuntimeException("i'm gonna knock you out");
    expect(mockParser.parseSingleStatement(statementText)).andThrow(
        exception);
    final Command command = new Command(
        statementText,
        true,
        emptyMap(),
        emptyMap());
    final CommandId commandId =  new CommandId(
        CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);
    replay(mockParser);

    // When:
    try {
      handleStatement(statementExecutorWithMocks, command, commandId, Optional.empty());
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
    final ConfiguredStatement<?> configuredStatement =
        ConfiguredStatement.of(ddlStatement, emptyMap(), originalConfig);
    ksqlEngine.execute(configuredStatement);

    final PreparedStatement<Statement> csasStatement =
        realParser.parseSingleStatement(statementText);

    expect(mockQueryMetadata.getQueryId()).andStubReturn(mock(QueryId.class));

    final KsqlConfig expectedConfig = ksqlConfig.overrideBreakingConfigsWithOriginalValues(
        originalConfig.getAllConfigPropsWithSecretsObfuscated());

    final Command csasCommand = new Command(
        statementText,
        true,
        emptyMap(),
        originalConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csasCommandId =  new CommandId(
        CommandId.Type.STREAM,
        "_CSASGen",
        CommandId.Action.CREATE);

    final ConfiguredStatement<?> configuredCsas =
        ConfiguredStatement.of(csasStatement, emptyMap(), expectedConfig);

    expect(mockParser.parseSingleStatement(statementText)).andReturn(csasStatement);
    expect(mockEngine.getPersistentQueries()).andReturn(ImmutableList.of());
    expect(mockEngine.execute(eq(configuredCsas)))
        .andReturn(ExecuteResult.of(mockQueryMetadata));
    mockQueryMetadata.start();
    expectLastCall();

    replay(mockParser, mockEngine, mockMetaStore, mockQueryMetadata);

    handleStatement(statementExecutorWithMocks, csasCommand, csasCommandId, Optional.empty());

    verify(mockParser, mockEngine, mockMetaStore, mockQueryMetadata);
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
        true,
        emptyMap(),
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
        true,
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.STREAM,
        "foo",
        CommandId.Action.CREATE);
    final CommandStatusFuture status = mock(CommandStatusFuture.class);

    status.setStatus(sameStatus(CommandStatus.Status.PARSING));
    expectLastCall();
    status.setStatus(sameStatus(Status.EXECUTING));
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
    Assert.assertEquals(3, statusStore.size());
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
        true,
        emptyMap(),
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
        "drop stream pageview;",
        true,
        emptyMap(),
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

  private Query mockCSASQuery() {
    final Query mockQuery = mock(Query.class);
    expect(mockQuery.getLimit()).andStubReturn(OptionalInt.empty());
    return mockQuery;
  }

  private CreateStreamAsSelect mockCSAS(final String name) {
    final CreateStreamAsSelect mockStatement = mock(CreateStreamAsSelect.class);
    expect(mockStatement.getName()).andStubReturn(QualifiedName.of(name));
    expect(mockStatement.getQuery()).andStubReturn(mockCSASQuery());
    expect(mockStatement.getProperties()).andStubReturn(CreateSourceAsProperties.none());
    expect(mockStatement.getPartitionByColumn()).andStubReturn(Optional.empty());
    return mockStatement;
  }

  private DropStream mockDropStream(final String name) {
    final DropStream mockDropStream = mock(DropStream.class);
    expect(mockDropStream.getName()).andStubReturn(QualifiedName.of(name));
    expect(mockParser.parseSingleStatement("DROP"))
        .andReturn(PreparedStatement.of("DROP", mockDropStream));
    return mockDropStream;
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
    expect(mockEngine.getPersistentQueries()).andReturn(ImmutableList.of());
    expect(mockEngine.execute(eqConfigured(csas)))
        .andReturn(ExecuteResult.of(mockQuery));
    return mockQuery;
  }

  @SuppressWarnings("unchecked")
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
    final ImmutableList<ParsedStatement> parsedStatements = ImmutableList
        .of(ParsedStatement.of(queryStatement, mock(SingleStatementContext.class)));
    final PreparedStatement<?> preparedStatement =
        PreparedStatement.of(queryStatement, mock(Statement.class));
    expect(mockEngine.parse(eq(queryStatement))).andReturn(parsedStatements);
    expect(mockEngine.prepare(parsedStatements.get(0)))
        .andReturn((PreparedStatement)preparedStatement);
    expect(mockEngine.execute(eqConfigured(preparedStatement)))
        .andReturn(ExecuteResult.of(mockQuery));
    expect(mockEngine.getPersistentQueries()).andReturn(ImmutableList.of());
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
            new Command("CSAS", true, emptyMap(), emptyMap())
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
        .andStubReturn(mock(DataSource.class));
    expect(mockMetaStore.getQueriesWithSink("foo"))
        .andStubReturn(ImmutableSet.of("query-id"));
    expect(mockEngine.getMetaStore()).andStubReturn(mockMetaStore);
    expect(mockEngine.getPersistentQuery(new QueryId("query-id"))).andReturn(Optional.of(mockQueryMetadata));
    mockQueryMetadata.close();
    expectLastCall();

    expect(mockEngine
        .execute(eqConfigured(PreparedStatement.of("DROP", mockDropStream))))
        .andReturn(ExecuteResult.of("SUCCESS"));
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, "foo", Action.DROP),
            new Command("DROP", true, emptyMap(), PRE_VERSION_5_NULL_ORIGINAL_PROPS)
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockMetaStore);
  }

  private static <T extends Statement> ConfiguredStatement<T> eqConfigured(
      final PreparedStatement<T> preparedStatement
  ) {
    EasyMock2Adapter.adapt(configured(preparedStatement(
        equalTo(preparedStatement.getStatementText()),
        equalTo(preparedStatement.getStatement()))
    ));
    return null;
  }

  @Test
  public void shouldNotCascadeDropStreamCommand() {
    // Given:
    final String drop = "DROP";
    final DropStream mockDropStream = mockDropStream("foo");
    final PreparedStatement<DropStream> statement = PreparedStatement.of(drop, mockDropStream);

    expect(mockEngine.execute(eqConfigured(statement)))
        .andReturn(ExecuteResult.of("SUCCESS"));
    replayAll();

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, "foo", Action.DROP),
            new Command(drop, true, emptyMap(), emptyMap())
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
  public void shouldHandleLegacyRunScriptCommand() {
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
                true,
                Collections.singletonMap("ksql.run.script.statements", queryStatement),
                emptyMap())
        )
    );

    // Then:
    verify(mockParser, mockEngine, mockQuery);
  }

  @Test
  public void shouldRestoreLegacyRunScriptCommand() {
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
                runScriptStatement, true,
                    Collections.singletonMap("ksql.run.script.statements", queryStatement), emptyMap())
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
            + "value_format = 'json');", true,
            emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
        "_CSASStreamGen",
        CommandId.Action.CREATE);
    handleStatement(csCommand, csCommandId, Optional.empty());

    final Command csasCommand = new Command(
        "CREATE STREAM user1pv AS "
            + "select * from pageview"
            + " WHERE userid = 'user1';", true,
            emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

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
        true,
        emptyMap(),
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
        true,
        emptyMap(),
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
        true,
        emptyMap(),
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
        true,
        emptyMap(),
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

  private void terminateQueries() {
    final Command terminateCommand1 = new Command(
        "TERMINATE CSAS_USER1PV_0;", true,
            emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId1 =
        new CommandId(CommandId.Type.STREAM, "_TerminateGen", CommandId.Action.CREATE);
    handleStatement(
        statementExecutor, terminateCommand1, terminateCommandId1, Optional.empty());
    assertThat(
        getCommandStatus(terminateCommandId1).getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    final Command terminateCommand2 = new Command(
        "TERMINATE CTAS_TABLE1_1;",
        true,
      emptyMap(),
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
        statementStr, true, emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
  }
}