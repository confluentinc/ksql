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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
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
import javax.ws.rs.core.Response;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryTest {

  private final KsqlConfig ksqlConfig = KsqlConfigTestUtil.create("0.0.0.0");
  private final List<QueuedCommand> commands = new LinkedList<>();
  private final FakeKafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final ServiceContext serviceContext = TestServiceContext.create(topicClient);
  private final KsqlServer server1 = new KsqlServer(commands);
  private final KsqlServer server2 = new KsqlServer(commands);

  @After
  public void tearDown() {
    server1.close();
    server2.close();
    serviceContext.close();
  }

  private KsqlEngine createKsqlEngine() {
    final KsqlEngineMetrics engineMetrics = mock(KsqlEngineMetrics.class);
    return KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        engineMetrics);
  }

  private static class FakeCommandQueue implements CommandQueue {
    private final List<QueuedCommand> commandLog;
    private final CommandIdAssigner commandIdAssigner;
    private int offset;

    FakeCommandQueue(
        final List<QueuedCommand> commandLog) {
      this.commandIdAssigner = new CommandIdAssigner();
      this.commandLog = commandLog;
    }

    @Override
    public QueuedCommandStatus enqueueCommand(final ConfiguredStatement<?> statement) {
      final CommandId commandId = commandIdAssigner.getCommandId(statement.getStatement());
      final long commandSequenceNumber = commandLog.size();
      commandLog.add(
          new QueuedCommand(
              commandId,
              new Command(
                  statement.getStatementText(),
                  Collections.emptyMap(),
                  statement.getConfig().getAllConfigPropsWithSecretsObfuscated()),
              Optional.empty()));
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
    public boolean isEmpty() {
      return commandLog.isEmpty();
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }
  }

  private class KsqlServer {
    final KsqlEngine ksqlEngine;
    final KsqlResource ksqlResource;
    final FakeCommandQueue fakeCommandQueue;
    final StatementExecutor statementExecutor;
    final CommandRunner commandRunner;
    final ServerState serverState;

    KsqlServer(final List<QueuedCommand> commandLog) {
      this.ksqlEngine = createKsqlEngine();
      this.fakeCommandQueue = new FakeCommandQueue(commandLog);
      serverState = new ServerState();
      serverState.setReady();
      this.ksqlResource = new KsqlResource(
          ksqlConfig,
          ksqlEngine,
          fakeCommandQueue,
          Duration.ofMillis(0),
          ()->{},
          Injectors.DEFAULT,
          (sc, metastore, statement) -> {
            return;
          });
      this.statementExecutor = new StatementExecutor(
          ksqlConfig,
          ksqlEngine,
          new StatementParser(ksqlEngine));
      this.commandRunner = new CommandRunner(
          statementExecutor,
          fakeCommandQueue,
          1,
          mock(ClusterTerminator.class),
          serverState
      );
    }

    void recover() {
      this.commandRunner.processPriorCommands();
    }

    void executeCommands() {
      this.commandRunner.fetchAndRunCommands();
    }

    void submitCommands(final String ...statements) {
      for (final String statement : statements) {
        final Response response = ksqlResource.handleKsqlStatements(serviceContext,
            new KsqlRequest(statement, Collections.emptyMap(), null));
        assertThat(response.getStatus(), equalTo(200));
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
      extends TypeSafeDiagnosingMatcher<DataSource<?>> {
    final DataSource<?> source;
    final Matcher<DataSource.DataSourceType> typeMatcher;
    final Matcher<String> nameMatcher;
    final Matcher<LogicalSchema> schemaMatcher;
    final Matcher<String> sqlMatcher;
    final Matcher<TimestampExtractionPolicy> extractionPolicyMatcher;
    final Matcher<KsqlTopic> topicMatcher;

    StructuredDataSourceMatcher(final DataSource<?> source) {
      this.source = source;
      this.typeMatcher = equalTo(source.getDataSourceType());
      this.nameMatcher = equalTo(source.getName());
      this.schemaMatcher = equalTo(source.getSchema());
      this.sqlMatcher = equalTo(source.getSqlExpression());
      this.extractionPolicyMatcher = equalTo(source.getTimestampExtractionPolicy());
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
              extractionPolicyMatcher,
              topicMatcher)
      );
    }

    @Override
    protected boolean matchesSafely(
        final DataSource<?> other,
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
          extractionPolicyMatcher,
          other.getTimestampExtractionPolicy(),
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

  private static Matcher<DataSource<?>> sameSource(final DataSource<?> source) {
    return new StructuredDataSourceMatcher(source);
  }

  private static class MetaStoreMatcher extends TypeSafeDiagnosingMatcher<MetaStore> {
    final Map<String, Matcher<DataSource<?>>> sourceMatchers;

    MetaStoreMatcher(final MetaStore metaStore) {
      this.sourceMatchers = metaStore.getAllDataSources().entrySet().stream()
          .collect(
              Collectors.toMap(Entry::getKey, e -> sameSource(e.getValue())));
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

      for (final Entry<String, Matcher<DataSource<?>>> e : sourceMatchers.entrySet()) {
        final String name = e.getKey();
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
    private final Matcher<Set<String>> sourcesNamesMatcher;
    private final Matcher<String> sinkNamesMatcher;
    private final Matcher<LogicalSchema> resultSchemaMatcher;
    private final Matcher<String> sqlMatcher;
    private final Matcher<String> stateMatcher;

    PersistentQueryMetadataMatcher(final PersistentQueryMetadata metadata) {
      this.sourcesNamesMatcher = equalTo(metadata.getSourceNames());
      this.sinkNamesMatcher = equalTo(metadata.getSinkName());
      this.resultSchemaMatcher = equalTo(metadata.getLogicalSchema());
      this.sqlMatcher = equalTo(metadata.getStatementString());
      this.stateMatcher = equalTo(metadata.getState());
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
          metadata.getState(),
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
  public void shouldRecoverRecreates() {
    server1.submitCommands(
        "CREATE STREAM A (C1 STRING, C2 INT) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT C1 FROM A;",
        "TERMINATE CSAS_B_0;",
        "DROP STREAM B;",
        "CREATE STREAM B AS SELECT C2 FROM A;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverTerminates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverDrop() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;",
        "DROP STREAM B;"
    );
    shouldRecover(commands);
  }

  @Test
  public void shouldNotDeleteTopicsOnRecovery() {
    topicClient.preconditionTopicExists("B");
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;",
        "DROP STREAM B DELETE TOPIC;"
    );

    assertThat(topicClient.listTopicNames(), not(hasItem("B")));

    topicClient.preconditionTopicExists("B");
    shouldRecover(commands);

    assertThat(topicClient.listTopicNames(), hasItem("B"));
  }

  @Test
  public void shouldNotDeleteTopicsOnRecoveryEvenIfLegacyDropCommandAlreadyInCommandQueue() {
    topicClient.preconditionTopicExists("B");

    shouldRecover(ImmutableList.of(
        new QueuedCommand(
            new CommandId(Type.STREAM, "B", Action.DROP),
            new Command("DROP STREAM B DELETE TOPIC;", ImmutableMap.of(), ImmutableMap.of()))
    ));

    assertThat(topicClient.listTopicNames(), hasItem("B"));
  }

  @Test
  public void shouldRecoverLogWithRepeatedTerminates() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    server2.executeCommands();
    server1.submitCommands(
        "TERMINATE CSAS_B_0;",
        "INSERT INTO B SELECT * FROM A;",
        "TERMINATE InsertQuery_1;"
    );
    server2.submitCommands("TERMINATE CSAS_B_0;");
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverLogWithDropWithRacingInsert() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;"
    );
    server2.executeCommands();
    server1.submitCommands("INSERT INTO B SELECT * FROM A;");
    server2.submitCommands("DROP STREAM B;");
    shouldRecover(commands);
  }

  @Test
  public void shouldRecoverLogWithTerminateAfterDrop() {
    topicClient.preconditionTopicExists("B");

    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B (COLUMN STRING) WITH (KAFKA_TOPIC='B', VALUE_FORMAT='JSON');"
    );
    server2.executeCommands();
    server1.submitCommands("INSERT INTO B SELECT * FROM A;");
    server2.submitCommands("DROP STREAM B;");
    server1.submitCommands("TERMINATE InsertQuery_0;");
    shouldRecover(commands);
  }

  @Test
  public void shouldCascade4Dot1Drop() {
    commands.addAll(
        ImmutableList.of(
            new QueuedCommand(
                new CommandId(Type.STREAM, "A", Action.CREATE),
                new Command(
                    "CREATE STREAM A (COLUMN STRING) "
                        + "WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
                    Collections.emptyMap(),
                    null
                )
            ),
            new QueuedCommand(
                new CommandId(Type.STREAM, "A", Action.CREATE),
                new Command(
                    "CREATE STREAM B AS SELECT * FROM A;",
                    Collections.emptyMap(),
                    null
                )
            )
        )
    );
    final KsqlServer server = new KsqlServer(commands);
    server.recover();
    assertThat(
        server.ksqlEngine.getMetaStore().getAllDataSources().keySet(),
        contains("A", "B"));
    commands.add(
        new QueuedCommand(
            new CommandId(Type.STREAM, "B", Action.DROP),
            new Command("DROP STREAM B;", Collections.emptyMap(), null)
        )
    );
    final KsqlServer recovered = new KsqlServer(commands);
    recovered.recover();
    assertThat(
        recovered.ksqlEngine.getMetaStore().getAllDataSources().keySet(),
        contains("A"));
  }
}
