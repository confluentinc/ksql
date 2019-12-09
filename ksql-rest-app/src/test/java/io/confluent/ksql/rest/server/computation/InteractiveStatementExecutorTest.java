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

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SpecificQueryIdGenerator;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InteractiveStatementExecutorTest {

  private static final Map<String, String> PRE_VERSION_5_NULL_ORIGINAL_PROPS = null;
  private static final String CREATE_STREAM_FOO_STATMENT = "CREATE STREAM foo ("
      + "biz bigint,"
      + " baz varchar) "
      + "WITH (kafka_topic = 'foo', "
      + "value_format = 'json');";

  private KsqlEngine ksqlEngine;
  private InteractiveStatementExecutor statementExecutor;
  private KsqlConfig ksqlConfig;
  private ServiceContext serviceContext;
  private InteractiveStatementExecutor statementExecutorWithMocks;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private StatementParser mockParser;
  @Mock
  private SpecificQueryIdGenerator mockQueryIdGenerator;
  @Mock
  private KsqlEngine mockEngine;
  @Mock
  private MetaStore mockMetaStore;
  @Mock
  private PersistentQueryMetadata mockQueryMetadata;
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
    final SpecificQueryIdGenerator hybridQueryIdGenerator = new SpecificQueryIdGenerator();
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        (engine) -> new KsqlEngineMetrics("", engine, Collections.emptyMap(), Optional.empty()),
        hybridQueryIdGenerator
    );

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    statementExecutor = new InteractiveStatementExecutor(
        serviceContext,
        ksqlEngine,
        statementParser,
        hybridQueryIdGenerator
    );
    statementExecutorWithMocks = new InteractiveStatementExecutor(
        serviceContext,
        mockEngine,
        mockParser,
        mockQueryIdGenerator
    );

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
    statementExecutor = new InteractiveStatementExecutor(
        serviceContext,
        mockEngine,
        mockParser,
        mockQueryIdGenerator
    );

    // When:
    statementExecutor.handleStatement(queuedCommand);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnHandleRestoreIfNotConfigured() {
    // Given:
    statementExecutor = new InteractiveStatementExecutor(
        serviceContext,
        mockEngine,
        mockParser,
        mockQueryIdGenerator
    );

    // When:
    statementExecutor.handleRestore(queuedCommand);
  }

  @Test
  public void shouldThrowOnUnexpectedException() {
    // Given:
    final String statementText = "mama said knock you out";

    final RuntimeException exception = new RuntimeException("i'm gonna knock you out");
    when(mockParser.parseSingleStatement(statementText)).thenThrow(exception);
    final Command command = new Command(
        statementText,
        emptyMap(),
        emptyMap());
    final CommandId commandId =  new CommandId(
        CommandId.Type.STREAM, "_CSASGen", CommandId.Action.CREATE);

    // When:
    try {
      handleStatement(statementExecutorWithMocks, command, commandId, Optional.empty(),0);
      Assert.fail("handleStatement should throw");
    } catch (final RuntimeException caughtException) {
      // Then:
      assertThat(caughtException, is(exception));
    }
  }

  @Test
  public void shouldBuildQueriesWithPersistedConfig() {
    // Given:
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
    ksqlEngine.execute(serviceContext, configuredStatement);

    final PreparedStatement<Statement> csasStatement =
        realParser.parseSingleStatement(statementText);

    when(mockQueryMetadata.getQueryId()).thenReturn(mock(QueryId.class));

    final KsqlConfig expectedConfig = ksqlConfig.overrideBreakingConfigsWithOriginalValues(
        originalConfig.getAllConfigPropsWithSecretsObfuscated());

    final Command csasCommand = new Command(
        statementText,
        emptyMap(),
        originalConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csasCommandId =  new CommandId(
        CommandId.Type.STREAM,
        "_CSASGen",
        CommandId.Action.CREATE);

    final ConfiguredStatement<?> configuredCsas = ConfiguredStatement.of(
        csasStatement, emptyMap(), expectedConfig);

    when(mockParser.parseSingleStatement(statementText)).thenReturn(csasStatement);
    final KsqlPlan plan = Mockito.mock(KsqlPlan.class);
    when(mockEngine.plan(eq(serviceContext), eq(configuredCsas))).thenReturn(plan);
    when(mockEngine.execute(eq(serviceContext), eqConfiguredPlan(plan)))
        .thenReturn(ExecuteResult.of(mockQueryMetadata));

    // When:
    handleStatement(statementExecutorWithMocks, csasCommand, csasCommandId, Optional.empty(), 1);

    // Then:
    verify(mockQueryMetadata, times(1)).start();
  }

  @Test
  public void shouldCompleteFutureOnSuccess() {
    // Given:
    final Command command = new Command(
        CREATE_STREAM_FOO_STATMENT,
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.STREAM,
        "foo",
        CommandId.Action.CREATE);
    final CommandStatusFuture status = mock(CommandStatusFuture.class);

    // When:
    handleStatement(command, commandId, Optional.of(status), 0L);

    // Then:
    InOrder inOrder = Mockito.inOrder(status);
    ArgumentCaptor<CommandStatus> argCommandStatus = ArgumentCaptor.forClass(CommandStatus.class);
    ArgumentCaptor<CommandStatus> argFinalCommandStatus = ArgumentCaptor.forClass(CommandStatus.class);
    inOrder.verify(status, times(2)).setStatus(argCommandStatus.capture());
    inOrder.verify(status, times(1)).setFinalStatus(argFinalCommandStatus.capture());

    List<CommandStatus> commandStatusList = argCommandStatus.getAllValues();
    assertEquals(CommandStatus.Status.PARSING, commandStatusList.get(0).getStatus());
    assertEquals(CommandStatus.Status.EXECUTING, commandStatusList.get(1).getStatus());
    assertEquals(CommandStatus.Status.SUCCESS, argFinalCommandStatus.getValue().getStatus());
  }

  @Test
  public void shouldThrowExceptionIfCommandFails() {
    // Given:
    shouldCompleteFutureOnSuccess();

    final Command command = new Command(
        CREATE_STREAM_FOO_STATMENT,
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId commandId =  new CommandId(CommandId.Type.STREAM,
        "foo",
        CommandId.Action.CREATE);
    final CommandStatusFuture status = mock(CommandStatusFuture.class);

    // When:
    try {
      handleStatement(command, commandId, Optional.of(status), 0L);
    } catch (KsqlStatementException e) {
      // Then:
      assertEquals("Cannot add stream 'FOO': A stream with the same name already exists\n" +
          "Statement: " + CREATE_STREAM_FOO_STATMENT, 
          e.getMessage());
    }
    InOrder inOrder = Mockito.inOrder(status);
    ArgumentCaptor<CommandStatus> argCommandStatus = ArgumentCaptor.forClass(CommandStatus.class);
    inOrder.verify(status, times(3)).setStatus(argCommandStatus.capture());

    List<CommandStatus> commandStatusList = argCommandStatus.getAllValues();
    assertEquals(CommandStatus.Status.PARSING, commandStatusList.get(0).getStatus());
    assertEquals(CommandStatus.Status.EXECUTING, commandStatusList.get(1).getStatus());
    assertEquals(CommandStatus.Status.ERROR, commandStatusList.get(2).getStatus());
  }

  @Test
  public void shouldHandlePriorStatements() {
    // Given:
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

    // When:
    expectedException.expect(KsqlStatementException.class);

    for (int i = 0; i < priorCommands.size(); i++) {
      final Pair<CommandId, Command> pair = priorCommands.get(i);
      statementExecutor.handleRestore(
          new QueuedCommand(pair.left, pair.right, Optional.empty(), (long) i)
      );
    }

    // Then:
    final Map<CommandId, CommandStatus> statusStore = statementExecutor.getStatuses();
    Assert.assertNotNull(statusStore);
    Assert.assertEquals(3, statusStore.size());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.SUCCESS, statusStore.get(csasCommandId).getStatus());
    Assert.assertEquals(CommandStatus.Status.ERROR, statusStore.get(ctasCommandId).getStatus());
  }

  private CreateStreamAsSelect mockCSAS(final String name) {
    final CreateStreamAsSelect mockStatement = mock(CreateStreamAsSelect.class);
    when(mockStatement.getName()).thenReturn(SourceName.of(name));
    return mockStatement;
  }

  private DropStream mockDropStream(final String name) {
    final DropStream mockDropStream = mock(DropStream.class);
    when(mockParser.parseSingleStatement("DROP"))
        .thenReturn(PreparedStatement.of("DROP", mockDropStream));
    return mockDropStream;
  }

  private PersistentQueryMetadata mockReplayCSAS(
          final QueryId queryId
  ) {
    final CreateStreamAsSelect mockCSAS = mockCSAS("foo");
    final PersistentQueryMetadata mockQuery = mock(PersistentQueryMetadata.class);
    when(mockQuery.getQueryId()).thenReturn(queryId);
    final PreparedStatement<Statement> csas = PreparedStatement.of("CSAS", mockCSAS);
    when(mockParser.parseSingleStatement("CSAS"))
        .thenReturn(csas);
    final KsqlPlan plan = Mockito.mock(KsqlPlan.class);
    when(mockEngine.plan(eq(serviceContext), eqConfiguredStatement(csas)))
        .thenReturn(plan);
    when(mockEngine.execute(eq(serviceContext), eqConfiguredPlan(plan)))
        .thenReturn(ExecuteResult.of(mockQuery));
    return mockQuery;
  }

  @Test
  public void shouldEnforceReferentialIntegrity() {
    createStreamsAndStartTwoPersistentQueries();

    // Now try to drop streams/tables to test referential integrity
    expectedException.expect(KsqlStatementException.class);
    tryDropThatViolatesReferentialIntegrity();

    // Terminate the queries using the stream/table
    terminateQueries();

    // Now drop should be successful
    final Command dropTableCommand2 = new Command(
        "drop table table1;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropTableCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropTableCommand2, dropTableCommandId2, Optional.empty(), 4);

    // DROP should succeed since no query is using the table
    final Optional<CommandStatus> dropTableCommandStatus2 =
        statementExecutor.getStatus(dropTableCommandId2);

    Assert.assertTrue(dropTableCommandStatus2.isPresent());
    assertThat(dropTableCommandStatus2.get().getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    // DROP should succeed since no query is using the stream.
    final Command dropStreamCommand3 = new Command(
        "drop stream pageview;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId3 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropStreamCommand3, dropStreamCommandId3, Optional.empty(), 5);

    final Optional<CommandStatus> dropStreamCommandStatus3 =
        statementExecutor.getStatus(dropStreamCommandId3);
    assertThat(dropStreamCommandStatus3.get().getStatus(),
        CoreMatchers.equalTo(CommandStatus.Status.SUCCESS));
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
    when(mockParser.parseSingleStatement(runScriptStatement))
        .thenReturn(runScript);
    final ImmutableList<ParsedStatement> parsedStatements = ImmutableList
        .of(ParsedStatement.of(queryStatement, mock(SingleStatementContext.class)));
    final PreparedStatement<Statement> preparedStatement =
        PreparedStatement.of(queryStatement, mock(Statement.class));
    when(mockEngine.parse(eq(queryStatement))).thenReturn(parsedStatements);
    when(mockEngine.prepare(parsedStatements.get(0)))
        .thenReturn((PreparedStatement)preparedStatement);
    when(mockEngine.execute(eq(serviceContext), eqConfiguredStatement(preparedStatement)))
        .thenReturn(ExecuteResult.of(mockQuery));
    when(mockEngine.getPersistentQueries()).thenReturn(ImmutableList.of());
    return mockQuery;
  }

  @Test
  public void shouldSkipStartWhenReplayingLog() {
    // Given:
    final QueryId queryId = new QueryId("csas-query-id");
    final String name = "foo";
    final PersistentQueryMetadata mockQuery = mockReplayCSAS(queryId);

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, name, Action.CREATE),
            new Command("CSAS", emptyMap(), emptyMap()),
            Optional.empty(),
            0L
        )
    );

    // Then:
    verify(mockQueryIdGenerator, times(1)).setNextId(anyLong());
    verify(mockQuery, times(0)).start();
  }

  private  <T extends Statement> ConfiguredStatement<T> eqConfiguredStatement(PreparedStatement<T> preparedStatement) {
    return argThat(new ConfiguredStatementMatcher<>(preparedStatement));
  }

  private ConfiguredKsqlPlan eqConfiguredPlan(final KsqlPlan plan) {
    return argThat(new ConfiguredKsqlPlanMatcher(plan));
  }

  private class ConfiguredKsqlPlanMatcher implements ArgumentMatcher<ConfiguredKsqlPlan> {

    private ConfiguredKsqlPlan plan;

    ConfiguredKsqlPlanMatcher(KsqlPlan ksqlPlan) {
      plan = ConfiguredKsqlPlan.of(ksqlPlan, Collections.emptyMap(), ksqlConfig);
    }

    @Override
    public boolean matches(ConfiguredKsqlPlan configuredKsqlPlan) {
      return plan.getPlan().equals(configuredKsqlPlan.getPlan());
    }
  }

  private class ConfiguredStatementMatcher<T extends Statement> implements ArgumentMatcher<ConfiguredStatement<T>> {

    private ConfiguredStatement<?> statement;

    ConfiguredStatementMatcher(PreparedStatement<?> preparedStatement) {
      statement = ConfiguredStatement.of(preparedStatement, Collections.<String, Object>emptyMap(), ksqlConfig);
    }

    @Override
    public boolean matches(ConfiguredStatement<T> matchStatement) {
      return statement.getStatementText().equals(matchStatement.getStatementText()) &&
          statement.getStatement().equals(matchStatement.getStatement());
    }
  }

  @Test
  public void shouldNotCascadeDropStreamCommand() {
    // Given:
    final String drop = "DROP";
    final DropStream mockDropStream = mockDropStream("foo");
    final PreparedStatement<DropStream> statement = PreparedStatement.of(drop, mockDropStream);

    final KsqlPlan plan = Mockito.mock(KsqlPlan.class);
    when(mockEngine.plan(eq(serviceContext), argThat(new ConfiguredStatementMatcher<>(statement)))).thenReturn(plan);
    when(mockEngine.execute(eq(serviceContext), eqConfiguredPlan(plan)))
        .thenReturn(ExecuteResult.of("SUCCESS"));

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(Type.STREAM, "foo", Action.DROP),
            new Command(drop, emptyMap(), emptyMap()),
            Optional.empty(),
            0L
        )
    );
  }

  @Test
  public void shouldHandleLegacyRunScriptCommand() {
    // Given:
    final String runScriptStatement = "run script";
    final String queryStatement = "a query";
    final PersistentQueryMetadata mockQuery = mockReplayRunScript(runScriptStatement, queryStatement);

    // When:
    statementExecutorWithMocks.handleStatement(
        new QueuedCommand(
            new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE),
            new Command(
                runScriptStatement,
                Collections.singletonMap("ksql.run.script.statements", queryStatement),
                emptyMap()),
            Optional.empty(),
            0L
        )
    );

    // Then:
    verify(mockQuery, times(1)).start();
  }

  @Test
  public void shouldRestoreLegacyRunScriptCommand() {
    // Given:
    final String runScriptStatement = "run script";
    final String queryStatement = "a persistent query";
    final PersistentQueryMetadata mockQuery = mockReplayRunScript(runScriptStatement, queryStatement);

    // When:
    statementExecutorWithMocks.handleRestore(
        new QueuedCommand(
            new CommandId(CommandId.Type.STREAM, "RunScript", CommandId.Action.EXECUTE),
            new Command(
                runScriptStatement,
                Collections.singletonMap("ksql.run.script.statements", queryStatement),
                emptyMap()
            ),
            Optional.empty(),
            0L
        )
    );

    // Then:
    verify(mockQuery, times(0)).start();
  }

  @Test
  public void shouldTerminateAll() {
    // Given:
    final String queryStatement = "a persistent query";

    final TerminateQuery terminateAll = mock(TerminateQuery.class);
    when(terminateAll.getQueryId()).thenReturn(Optional.empty());

    when(mockParser.parseSingleStatement(any()))
        .thenReturn(PreparedStatement.of(queryStatement, terminateAll));

    final PersistentQueryMetadata query0 = mock(PersistentQueryMetadata.class);
     PersistentQueryMetadata query1 = mock(PersistentQueryMetadata.class);

    when(mockEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));

    // When:
    statementExecutorWithMocks.handleStatement(
        new QueuedCommand(
            new CommandId(Type.TERMINATE, "-", Action.EXECUTE),
            new Command("terminate all", emptyMap(), emptyMap()),
            Optional.empty(),
            0L
        )
    );

    // Then:
    verify(query0).close();
    verify(query1).close();
  }

  private void createStreamsAndStartTwoPersistentQueries() {
    final Command csCommand = new Command(
        "CREATE STREAM pageview ("
            + "viewtime bigint,"
            + " pageid varchar, "
            + "userid varchar) "
            + "WITH (kafka_topic = 'pageview_topic_json', "
            + "value_format = 'json');",
        emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId csCommandId =  new CommandId(CommandId.Type.STREAM,
        "_CSASStreamGen",
        CommandId.Action.CREATE);
    handleStatement(csCommand, csCommandId, Optional.empty(), 0);

    final Command csasCommand = new Command(
        "CREATE STREAM user1pv AS "
            + "select * from pageview"
            + " WHERE userid = 'user1';",
        emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId csasCommandId =  new CommandId(CommandId.Type.STREAM,
        "_CSASGen",
        CommandId.Action.CREATE);
    handleStatement(csasCommand, csasCommandId, Optional.empty(), 1);

    final Command ctasCommand = new Command(
        "CREATE TABLE table1  AS "
            + "SELECT pageid, count(pageid) "
            + "FROM pageview "
            + "WINDOW TUMBLING ( SIZE 10 SECONDS) "
            + "GROUP BY pageid;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

    final CommandId ctasCommandId =  new CommandId(CommandId.Type.TABLE,
        "_CTASGen",
        CommandId.Action.CREATE);
    handleStatement(ctasCommand, ctasCommandId, Optional.empty(), 2);

    assertThat(getCommandStatus(csCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(getCommandStatus(csasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
    assertThat(getCommandStatus(ctasCommandId).getStatus(), equalTo(CommandStatus.Status.SUCCESS));
  }

  private void tryDropThatViolatesReferentialIntegrity() {
    final Command dropStreamCommand1 = new Command(
        "drop stream pageview;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId1 =  new CommandId(CommandId.Type.STREAM,
        "_PAGEVIEW",
        CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropStreamCommand1, dropStreamCommandId1, Optional.empty(), 0);

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
        containsString("The following queries read from this source: [CTAS_TABLE1_2, CSAS_USER1PV_1]."));
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
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropStreamCommandId2 =
        new CommandId(CommandId.Type.STREAM, "_user1pv", CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropStreamCommand2, dropStreamCommandId2, Optional.empty(), 0);

    // DROP statement should fail since the stream is being used.
    final Optional<CommandStatus> dropStreamCommandStatus2 = statementExecutor.getStatus(dropStreamCommandId2);

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
            "The following queries write into this source: [CSAS_USER1PV_1]."));
    assertThat(
        dropStreamCommandStatus2.get()
            .getMessage(),
        containsString("You need to terminate them before dropping USER1PV."));

    final Command dropTableCommand1 = new Command(
        "drop table table1;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId dropTableCommandId1 =
        new CommandId(CommandId.Type.TABLE, "_TABLE1", CommandId.Action.DROP);
    handleStatement(
        statementExecutor, dropTableCommand1, dropTableCommandId1, Optional.empty(), 0);

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
            "The following queries write into this source: [CTAS_TABLE1_2]."));

    assertThat(
        dropTableCommandStatus1.get().getMessage(),
        containsString("You need to terminate them before dropping TABLE1."));
  }

  private void handleStatement(
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatus,
      final long offset) {
    handleStatement(statementExecutor, command, commandId, commandStatus, offset);
  }

  private static void handleStatement(
      final InteractiveStatementExecutor statementExecutor,
      final Command command,
      final CommandId commandId,
      final Optional<CommandStatusFuture> commandStatus,
      final long offset) {
    statementExecutor.handleStatement(new QueuedCommand(commandId, command, commandStatus, offset));
  }

  private void terminateQueries() {
    final Command terminateCommand1 = new Command(
        "TERMINATE CSAS_USER1PV_1;",
        emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId1 =
        new CommandId(CommandId.Type.STREAM, "_TerminateGen", CommandId.Action.CREATE);
    handleStatement(
        statementExecutor, terminateCommand1, terminateCommandId1, Optional.empty(), 0);
    assertThat(
        getCommandStatus(terminateCommandId1).getStatus(), equalTo(CommandStatus.Status.SUCCESS));

    final Command terminateCommand2 = new Command(
        "TERMINATE CTAS_TABLE1_2;",
        emptyMap(),
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
    final CommandId terminateCommandId2 =
        new CommandId(CommandId.Type.TABLE, "_TerminateGen", CommandId.Action.CREATE);
    handleStatement(
        statementExecutor, terminateCommand2, terminateCommandId2, Optional.empty(), 0);
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
        statementStr, emptyMap(), ksqlConfig.getAllConfigPropsWithSecretsObfuscated());
  }
}
