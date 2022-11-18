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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.query.id.SpecificQueryIdGenerator;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.execution.DefaultConnectServerErrors;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.PersistentQueryCleanupImpl;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class RecoveryTest {

  private final KsqlConfig ksqlConfig = KsqlConfigTestUtil.create(
      "0.0.0.0",
      ImmutableMap.of(StreamsConfig.APPLICATION_SERVER_CONFIG, "http://localhost:23")
  );

  private final List<QueuedCommand> commands = new LinkedList<>();
  private final FakeKafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final ServiceContext serviceContext = TestServiceContext.create(topicClient);

  private KsqlSecurityContext securityContext;

  @Mock
  @SuppressWarnings("unchecked")
  private final Producer<CommandId, Command> transactionalProducer = (Producer<CommandId, Command>) mock(Producer.class);
  @Mock
  private DenyListPropertyValidator denyListPropertyValidator =
      mock(DenyListPropertyValidator.class);

  @Mock
  private Errors errorHandler = mock(Errors.class);

  private final KsqlServer server1 = new KsqlServer(commands);
  private final KsqlServer server2 = new KsqlServer(commands);


  @Before
  public void setup() {
    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);
  }

  @After
  public void tearDown() {
    server1.close();
    server2.close();
    serviceContext.close();
  }

  private KsqlEngine createKsqlEngine(final QueryIdGenerator queryIdGenerator) {
    final KsqlEngineMetrics engineMetrics = mock(KsqlEngineMetrics.class);
    when(engineMetrics.getQueryEventListener()).thenReturn(mock(QueryEventListener.class));
    return KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        ignored -> engineMetrics,
        queryIdGenerator,
        ksqlConfig,
        new MetricCollectors()
    );
  }

  private static class FakeCommandQueue implements CommandQueue {
    private final List<QueuedCommand> commandLog;
    private int offset;
    private final Producer<CommandId, Command> transactionalProducer;

    FakeCommandQueue(final List<QueuedCommand> commandLog, final Producer<CommandId, Command> transactionalProducer) {
      this.commandLog = commandLog;
      this.transactionalProducer = transactionalProducer;
    }

    @Override
    public QueuedCommandStatus enqueueCommand(
        final CommandId commandId,
        final Command command,
        final Producer<CommandId, Command> transactionalProducer
    ) {
      final long commandSequenceNumber = commandLog.size();
      commandLog.add(
          new QueuedCommand(
              commandId,
              command,
              Optional.empty(),
              commandSequenceNumber));
      return new QueuedCommandStatus(commandSequenceNumber, new CommandStatusFuture(commandId));
    }

    @Override
    public List<QueuedCommand> getNewCommands(final Duration timeout) {
      final List<QueuedCommand> commands = commandLog.subList(offset, commandLog.size());
      offset = commandLog.size();
      return commands;
    }

    @Override
    public List<QueuedCommand> getRestoreCommands() {
      final List<QueuedCommand> restoreCommands = ImmutableList.copyOf(commandLog);
      this.offset = commandLog.size();
      return restoreCommands;
    }

    @Override
    public void ensureConsumedPast(final long seqNum, final Duration timeout) {
    }

    @Override
    public Producer<CommandId, Command> createTransactionalProducer() {
      return transactionalProducer;
    }

    @Override
    public void abortCommand(CommandId commandId) {
    }

    @Override
    public boolean corruptionDetected() {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return commandLog.isEmpty();
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void waitForCommandConsumer() {
    }

    @Override
    public void close() {
    }
  }

  private class KsqlServer {
    final KsqlEngine ksqlEngine;
    final KsqlResource ksqlResource;
    final FakeCommandQueue fakeCommandQueue;
    final InteractiveStatementExecutor statementExecutor;
    final CommandRunner commandRunner;
    final ServerState serverState;

    KsqlServer(final List<QueuedCommand> commandLog) {
      final SpecificQueryIdGenerator queryIdGenerator = new SpecificQueryIdGenerator();
      this.ksqlEngine = createKsqlEngine(queryIdGenerator);
      this.fakeCommandQueue = new FakeCommandQueue(commandLog, transactionalProducer);
      serverState = new ServerState();
      serverState.setReady();

      this.statementExecutor = new InteractiveStatementExecutor(
          serviceContext,
          ksqlEngine,
          queryIdGenerator
      );

      this.commandRunner = new CommandRunner(
          statementExecutor,
          fakeCommandQueue,
          1,
          mock(ClusterTerminator.class),
          serverState,
          "ksql-service-id",
          Duration.ofMillis(2000),
          "",
          InternalTopicSerdes.deserializer(Command.class),
          errorHandler,
          topicClient,
          "command_topic",
          new Metrics()
      );

      this.ksqlResource = new KsqlResource(
          ksqlEngine,
          commandRunner,
          Duration.ofMillis(0),
          ()->{},
          Optional.of((sc, metastore, statement) -> { }),
          errorHandler,
          new DefaultConnectServerErrors(),
          denyListPropertyValidator
      );

      this.statementExecutor.configure(ksqlConfig);
      this.ksqlResource.configure(ksqlConfig);
    }

    void recover() {
      this.commandRunner.processPriorCommands(new PersistentQueryCleanupImpl(
        ksqlConfig
          .getKsqlStreamConfigProps()
          .getOrDefault(
            StreamsConfig.STATE_DIR_CONFIG,
            StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG))
          .toString(),
        serviceContext,
        ksqlConfig)
      );
    }

    void executeCommands() {
      this.commandRunner.fetchAndRunCommands();
    }

    void submitCommands(final String ...statements) {
      for (final String statement : statements) {
        final EndpointResponse response = ksqlResource.handleKsqlStatements(securityContext,
            new KsqlRequest(statement, Collections.emptyMap(), Collections.emptyMap(), null));
        assertThat("Bad response: " + response.getEntity(), response.getStatus(), equalTo(200));
        executeCommands();
      }
    }

    void close() {
      ksqlEngine.close();
    }
  }

  private static <T> boolean test(
      final Matcher<T> matcher,
      final T object,
      final Description description,
      final String errorPrefix) {
    if (!matcher.matches(object)) {
      description.appendText(errorPrefix);
      matcher.describeMismatch(object, description);
      return false;
    }
    return true;
  }

  private static class TopicMatcher extends TypeSafeDiagnosingMatcher<KsqlTopic> {

    final Matcher<String> kafkaNameMatcher;
    final Matcher<ValueFormat> valueFormatMatcher;

    TopicMatcher(final KsqlTopic topic) {
      this.kafkaNameMatcher = equalTo(topic.getKafkaTopicName());
      this.valueFormatMatcher = equalTo(topic.getValueFormat());
    }

    @Override
    public void describeTo(final Description description) {
      description.appendList(
          "Topic(", ", ", ")",
          Arrays.asList(kafkaNameMatcher, valueFormatMatcher));
    }

    @Override
    public boolean matchesSafely(final KsqlTopic other, final Description description) {
      if (!test(
          kafkaNameMatcher,
          other.getKafkaTopicName(),
          description,
          "kafka name mismatch: ")) {
        return false;
      }
      return test(
          valueFormatMatcher,
          other.getValueFormat(),
          description,
          "serde mismatch: ");
    }
  }

  private static Matcher<KsqlTopic> sameTopic(final KsqlTopic topic) {
    return new TopicMatcher(topic);
  }

  private static class StructuredDataSourceMatcher
      extends TypeSafeDiagnosingMatcher<DataSource> {
    final DataSource source;
    final Matcher<DataSource.DataSourceType> typeMatcher;
    final Matcher<SourceName> nameMatcher;
    final Matcher<LogicalSchema> schemaMatcher;
    final Matcher<String> sqlMatcher;
    final Matcher<Optional<TimestampColumn>> extractionColumnMatcher;
    final Matcher<KsqlTopic> topicMatcher;

    StructuredDataSourceMatcher(final DataSource source) {
      this.source = source;
      this.typeMatcher = equalTo(source.getDataSourceType());
      this.nameMatcher = equalTo(source.getName());
      this.schemaMatcher = equalTo(source.getSchema());
      this.sqlMatcher = equalTo(source.getSqlExpression());
      this.extractionColumnMatcher = equalTo(source.getTimestampColumn());
      this.topicMatcher = sameTopic(source.getKsqlTopic());
    }

    @Override
    public void describeTo(final Description description) {
      description.appendList(
          "Source(", ", ", ")",
          Arrays.asList(
              nameMatcher,
              typeMatcher,
              schemaMatcher,
              sqlMatcher,
              extractionColumnMatcher,
              topicMatcher)
      );
    }

    @Override
    protected boolean matchesSafely(
        final DataSource other,
        final Description description) {
      if (!test(
          typeMatcher,
          other.getDataSourceType(),
          description,
          "type mismatch: ")) {
        return false;
      }
      if (!test(
          nameMatcher,
          other.getName(),
          description,
          "name mismatch: ")) {
        return false;
      }
      if (!test(
          schemaMatcher,
          other.getSchema(),
          description,
          "schema mismatch: ")) {
        return false;
      }
      if (!test(
          sqlMatcher,
          other.getSqlExpression(),
          description,
          "sql mismatch: ")) {
        return false;
      }
      if (!test(
          extractionColumnMatcher,
          other.getTimestampColumn(),
          description,
          "timestamp extraction policy mismatch")) {
        return false;
      }
      return test(
          topicMatcher,
          other.getKsqlTopic(),
          description,
          "topic mismatch: ");
    }
  }

  private static Matcher<DataSource> sameSource(final DataSource source) {
    return new StructuredDataSourceMatcher(source);
  }

  private static class MetaStoreMatcher extends TypeSafeDiagnosingMatcher<MetaStore> {
    final Map<SourceName, Matcher<DataSource>> sourceMatchers;

    MetaStoreMatcher(final MetaStore metaStore) {
      this.sourceMatchers = metaStore.getAllDataSources().entrySet().stream()
          .collect(
              Collectors.toMap(
                  Entry::getKey,
                  e -> sameSource(e.getValue())));
    }

    @Override
    public void describeTo(final Description description) {
      description.appendText("Metastore with data sources: ");
      description.appendList(
          "Metastore with sources: ",
          ", ",
          "",
          sourceMatchers.values());
    }

    @Override
    protected boolean matchesSafely(final MetaStore other, final Description description) {
      if (!test(
          equalTo(sourceMatchers.keySet()),
          other.getAllDataSources().keySet(),
          description,
          "source set mismatch: ")) {
        return false;
      }

      for (final Entry<SourceName, Matcher<DataSource>> e : sourceMatchers.entrySet()) {
        final SourceName name = e.getKey();
        if (!test(
            e.getValue(),
            other.getSource(name),
            description,
            "source " + name + " mismatch: ")) {
          return false;
        }
      }

      return true;
    }
  }

  private static Matcher<MetaStore> sameStore(final MetaStore store) {
    return new MetaStoreMatcher(store);
  }

  private static class PersistentQueryMetadataMatcher
      extends TypeSafeDiagnosingMatcher<PersistentQueryMetadata> {
    private final Matcher<Set<SourceName>> sourcesNamesMatcher;
    private final Matcher<Optional<SourceName>> sinkNamesMatcher;
    private final Matcher<LogicalSchema> resultSchemaMatcher;
    private final Matcher<String> sqlMatcher;
    private final Matcher<String> stateMatcher;

    PersistentQueryMetadataMatcher(final PersistentQueryMetadata metadata) {
      this.sourcesNamesMatcher = equalTo(metadata.getSourceNames());
      this.sinkNamesMatcher = equalTo(metadata.getSinkName());
      this.resultSchemaMatcher = equalTo(metadata.getLogicalSchema());
      this.sqlMatcher = equalTo(metadata.getStatementString());
      this.stateMatcher = equalTo(metadata.getState().toString());
    }

    @Override
    public void describeTo(final Description description) {
      description.appendList(
          "Query(", ", ", ")",
          Arrays.asList(
              sourcesNamesMatcher,
              sinkNamesMatcher,
              resultSchemaMatcher,
              sqlMatcher,
              stateMatcher
          )
      );
    }

    @Override
    protected boolean matchesSafely(
        final PersistentQueryMetadata metadata,
        final Description description) {
      if (!test(
          sourcesNamesMatcher,
          metadata.getSourceNames(),
          description,
          "source names mismatch: "
      )) {
        return false;
      }
      if (!test(
          sinkNamesMatcher,
          metadata.getSinkName(),
          description,
          "sink names mismatch: "
      )) {
        return false;
      }
      if (!test(
          resultSchemaMatcher,
          metadata.getLogicalSchema(),
          description,
          "schema mismatch: "
      )) {
        return false;
      }
      if (!test(
          sqlMatcher,
          metadata.getStatementString(),
          description,
          "sql mismatch: "
      )) {
        return false;
      }
      return test(
          stateMatcher,
          metadata.getState().toString(),
          description,
          "state mismatch: ");
    }
  }

  private static Matcher<PersistentQueryMetadata> sameQuery(
      final PersistentQueryMetadata metadata) {
    return new PersistentQueryMetadataMatcher(metadata);
  }

  private static Map<QueryId, PersistentQueryMetadata> queriesById(
      final Collection<PersistentQueryMetadata> queries) {
    return queries.stream().collect(
        Collectors.toMap(PersistentQueryMetadata::getQueryId, q -> q)
    );
  }

  private void shouldRecover(final List<QueuedCommand> commands) {
    // Given:
    final KsqlServer executeServer = new KsqlServer(commands);
    executeServer.executeCommands();
    final KsqlEngine engine = executeServer.ksqlEngine;

    // When:
    final KsqlServer recoverServer = new KsqlServer(commands);
    recoverServer.recover();
    final KsqlEngine recovered = recoverServer.ksqlEngine;

    // Then:
    assertThat(recovered.getMetaStore(), sameStore(engine.getMetaStore()));
    final Map<QueryId, PersistentQueryMetadata> queries
        = queriesById(engine.getPersistentQueries());
    final Map<QueryId, PersistentQueryMetadata> recoveredQueries
        = queriesById(recovered.getPersistentQueries());
    assertThat(queries.keySet(), equalTo(recoveredQueries.keySet()));
    queries.forEach(
        (queryId, query) -> assertThat(query, sameQuery(recoveredQueries.get(queryId))));
  }

  @Before
  public void setUp() {
    topicClient.preconditionTopicExists("A");
    topicClient.preconditionTopicExists("B");
    topicClient.preconditionTopicExists("command_topic");
  }

  @Test
  public void shouldRecoverCreates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverCreatesWithNonExistingTopic() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='newTopic', VALUE_FORMAT='JSON', PARTITIONS=1);",
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    shouldRecover(commands);
  }
  @Test

  public void shouldRecoverRecreates() {
    server1.submitCommands(
        "CREATE STREAM A (ROWKEY STRING KEY, C1 STRING, C2 INT) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT ROWKEY, C1 FROM A;",
        "TERMINATE CsAs_b_1;",
        "DROP STREAM B;",
        "CREATE STREAM B AS SELECT ROWKEY, C2 FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverReplaces() {
    server1.submitCommands(
        "CREATE STREAM A (ROWKEY STRING KEY, C1 STRING, C2 INT) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT ROWKEY, C1 FROM A;",
        "CREATE OR REPLACE STREAM B AS SELECT ROWKEY, C1, C2 FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverReplacesWithTerminates() {
    server1.submitCommands(
        "CREATE STREAM A (ROWKEY STRING KEY, C1 STRING, C2 INT) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT ROWKEY, C1 FROM A;",
        "CREATE OR REPLACE STREAM B AS SELECT ROWKEY, C1, C2 FROM A;",
        "TERMINATE CSAS_B_1;",
        "DROP STREAM B;",
        "CREATE STREAM B AS SELECT ROWKEY, C1 FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverInsertIntos() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B (COLUMN STRING) WITH (KAFKA_TOPIC='B', VALUE_FORMAT='JSON');",
        "INSERT INTO B SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverInsertIntosWithCustomQueryId() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B (COLUMN STRING) WITH (KAFKA_TOPIC='B', VALUE_FORMAT='JSON');",
        "INSERT INTO B WITH(QUERY_ID='MY_INSERT_ID') SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverInsertIntosRecreates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B (COLUMN STRING) WITH (KAFKA_TOPIC='B', VALUE_FORMAT='JSON');",
        "INSERT INTO B SELECT * FROM A;",
        "TERMINATE InsertQuery_2;",
        "INSERT INTO B SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverIfNotExists() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM IF NOT EXISTS B AS SELECT * FROM A;",
        "CREATE STREAM IF NOT EXISTS B AS SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverTerminates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "INSERT INTO B SELECT * FROM A;",
        "TERMINATE CSAS_B_1;",
        "TERMINATE InsertQuery_2;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverTerminateAll() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "INSERT INTO B SELECT * FROM A;",
        "TERMINATE ALL;",
        "DROP STREAM B;",
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverDropWithTerminate() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_1;",
        "DROP STREAM B;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverWhenDropWithSourceConstraintsFoundOnMetastore() {
    // Verify that an upgrade will not be affected if DROP commands are not in order.

    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "INSERT INTO B SELECT * FROM A;"
    );

    // ksqlDB does not allow a DROP STREAM A because 'A' is used by 'B'.
    // However, if a ksqlDB upgrade is done, then this order can be possible.
    final Command dropACommand = new Command(
        "DROP STREAM A;",
        Optional.of(ImmutableMap.of()),
        Optional.of(ImmutableMap.of()),
        Optional.of(KsqlPlan.ddlPlanCurrent("DROP STREAM A;",
            new DropSourceCommand(SourceName.of("A")))),
        Optional.of(Command.VERSION)
    );

    // Add the DROP STREAM A manually to prevent server1 to fail if done on submitCommands()
    commands.add(new QueuedCommand(
        InternalTopicSerdes.serializer().serialize("",
            new CommandId(CommandId.Type.STREAM, "`A`", CommandId.Action.DROP)),
        InternalTopicSerdes.serializer().serialize("", dropACommand),
        Optional.empty(),
        (long) commands.size()
    ));

    final KsqlServer recovered = new KsqlServer(commands);
    recovered.recover();

    // Original server has streams 'A' and 'B' because DROP statements weren't executed, but
    // the previous hack added them to the list of command topic statements
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources().size(), is(2));
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("A")));
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("B")));
    assertThat(recovered.ksqlEngine.getAllLiveQueries().size(), is(2));

    // Recovered server has stream 'B' only. It restored the previous CREATE and DROP statements
    assertThat(recovered.ksqlEngine.getMetaStore().getAllDataSources().size(), is(1));
    assertThat(recovered.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("B")));
    assertThat(recovered.ksqlEngine.getAllLiveQueries().size(), is(2));
  }

  @Test
  public void shouldRecoverWhenDropWithSourceConstraintsAndCreateSourceAgainFoundOnMetastore() {
    // Verify that an upgrade will not be affected if DROP commands are not in order.

    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;"
    );

    // ksqlDB does not allow a DROP STREAM A because 'A' is used by 'B'.
    // However, if a ksqlDB upgrade is done, then this order can be possible.
    final Command dropACommand = new Command(
        "DROP STREAM A;",
        Optional.of(ImmutableMap.of()),
        Optional.of(ImmutableMap.of()),
        Optional.of(KsqlPlan.ddlPlanCurrent("DROP STREAM A;",
            new DropSourceCommand(SourceName.of("A")))),
        Optional.of(Command.VERSION)
    );

    // Add the DROP STREAM A manually to prevent server1 to fail if done on submitCommands()
    commands.add(new QueuedCommand(
        InternalTopicSerdes.serializer().serialize("",
            new CommandId(CommandId.Type.STREAM, "`A`", CommandId.Action.DROP)),
        InternalTopicSerdes.serializer().serialize("", dropACommand),
        Optional.empty(),
        (long) commands.size()
    ));

    // Add CREATE STREAM after the DROP again
    final Command createACommand = new Command(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        Optional.of(ImmutableMap.of()),
        Optional.of(ImmutableMap.of()),
        Optional.of(KsqlPlan.ddlPlanCurrent(
            "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
            new CreateStreamCommand(
                SourceName.of("A"),
                LogicalSchema.builder()
                    .valueColumn(ColumnName.of("COLUMN"), SqlTypes.STRING)
                    .build(),
                Optional.empty(),
                "A",
                Formats.of(
                    KeyFormat
                        .nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of())
                        .getFormatInfo(),
                    ValueFormat
                        .of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
                        .getFormatInfo(),
                    SerdeFeatures.of(),
                    SerdeFeatures.of()
                ),
                Optional.empty(),
                Optional.of(false),
                Optional.of(false)
            ))),
        Optional.of(Command.VERSION)
    );

    // Add the CREATE STREAM A manually to prevent server1 to fail if done on submitCommands()
    commands.add(new QueuedCommand(
        InternalTopicSerdes.serializer().serialize("",
            new CommandId(CommandId.Type.STREAM, "`A`", CommandId.Action.CREATE)),
        InternalTopicSerdes.serializer().serialize("", createACommand),
        Optional.empty(),
        (long) commands.size()
    ));

    final KsqlServer recovered = new KsqlServer(commands);
    recovered.recover();

    // Original server has both streams
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources().size(), is(2));
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("A")));
    assertThat(server1.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("B")));

    // Recovered server has only stream 'B'
    assertThat(recovered.ksqlEngine.getMetaStore().getAllDataSources().size(), is(2));
    assertThat(recovered.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("A")));
    assertThat(recovered.ksqlEngine.getMetaStore().getAllDataSources(), hasKey(SourceName.of("B")));
  }

  @Test
  public void shouldRecoverDrop() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "DROP STREAM B;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverDropWithRecreates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "DROP STREAM B;",
        "CREATE STREAM B AS SELECT * FROM A;",
        "DROP STREAM B;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverWithDuplicateTerminateAndDrop() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_1;"
    );

    addDuplicateOfLastCommand(); // Add duplicate of "TERMINATE CSAS_B_0;"

    server1.submitCommands(
        "DROP STREAM B;"
    );

    addDuplicateOfLastCommand(); // Add duplicate of "DROP STREAM B;"

    shouldRecover(commands);
  }

  @Test
  public void shouldNotDeleteTopicsOnRecovery() {
    topicClient.preconditionTopicExists("B");
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_1;",
        "DROP STREAM B DELETE TOPIC;"
    );

    assertThat(topicClient.listTopicNames(), not(hasItem("B")));

    topicClient.preconditionTopicExists("B");
    shouldRecover(commands);

    assertThat(topicClient.listTopicNames(), hasItem("B"));
  }

  @Test
  public void shouldRecoverQueryIDs() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM C AS SELECT * FROM A;");

    final KsqlServer server = new KsqlServer(commands);
    server.recover();
    final Set<QueryId> queryIdNames = queriesById(server.ksqlEngine.getPersistentQueries())
        .keySet();

    assertThat(queryIdNames, contains(new QueryId("CSAS_C_1")));
  }

  @Test
  public void shouldIncrementQueryIDsNoPlans() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_1;");

    final KsqlServer server = new KsqlServer(commands);
    server.recover();
    server.submitCommands("CREATE STREAM C AS SELECT * FROM A;");
    final Set<QueryId> queryIdNames = queriesById(server.ksqlEngine.getPersistentQueries())
        .keySet();

    assertThat(queryIdNames, contains(new QueryId("CSAS_C_2")));
  }

  @Test
  public void shouldIncrementQueryIDsWithPlan() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "CREATE STREAM C AS SELECT * FROM A;",
        "TERMINATE CSAS_B_1;");

    final KsqlServer server = new KsqlServer(commands);
    server.recover();
    server.submitCommands("CREATE STREAM D AS SELECT * FROM A;");
    final Set<QueryId> queryIdNames = queriesById(server.ksqlEngine.getPersistentQueries())
        .keySet();
    assertThat(queryIdNames, contains(new QueryId("CSAS_C_2"), new QueryId("CSAS_D_3")));
  }


  // Simulate bad commands that have been introduced due to race condition in logic producing to cmd topic
  private void addDuplicateOfLastCommand() {
    final QueuedCommand original = commands.get(commands.size() - 1);
    final QueuedCommand duplicate = new QueuedCommand(
        original.getCommandId(),
        original.getCommand(),
        Optional.empty(),
        (long) commands.size()
    );

    commands.add(duplicate);
  }
}
