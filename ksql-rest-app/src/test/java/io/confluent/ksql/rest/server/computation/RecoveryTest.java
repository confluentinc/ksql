/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandId.Action;
import io.confluent.ksql.rest.server.computation.CommandId.Type;
import io.confluent.ksql.rest.server.mock.MockKafkaTopicClient;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.utils.TestUtils;
import io.confluent.ksql.schema.registry.MockSchemaRegistryClientFactory;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.easymock.EasyMock;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.ClassRule;
import org.junit.Test;

public class RecoveryTest {
  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
          "bootstrap.servers", "0.0.0.0"
      )
  );
  private final List<QueuedCommand> commands = new LinkedList<>();
  private final KsqlServer server1 = new KsqlServer(commands);
  private final KsqlServer server2 = new KsqlServer(commands);

  private KsqlEngine createKsqlEngine() {
    final KsqlEngineMetrics engineMetrics = EasyMock.niceMock(KsqlEngineMetrics.class);
    EasyMock.replay(engineMetrics);
    return KsqlEngineTestUtil.createKsqlEngine(
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get,
        new FakeKafkaClientSupplier(),
        new MetaStoreImpl(new InternalFunctionRegistry()),
        ksqlConfig,
        new FakeKafkaClientSupplier().getAdminClient(ksqlConfig.getKsqlAdminClientConfigProps()),
        engineMetrics);
  }

  private class FakeCommandQueue implements ReplayableCommandQueue {
    private final List<QueuedCommand> commandLog;
    private final CommandIdAssigner commandIdAssigner;
    private int offset;

    FakeCommandQueue(
        final CommandIdAssigner commandIdAssigner,
        final List<QueuedCommand> commandLog) {
      this.commandIdAssigner = commandIdAssigner;
      this.commandLog = commandLog;
    }

    @Override
    public QueuedCommandStatus enqueueCommand(
        final String statementString,
        final Statement statement,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> overwriteProperties) {
      final CommandId commandId = commandIdAssigner.getCommandId(statement);
      commandLog.add(
          new QueuedCommand(
              commandId,
              new Command(
                  statementString,
                  Collections.emptyMap(),
                  ksqlConfig.getAllConfigPropsWithSecretsObfuscated()),
              Optional.empty()));
      return new QueuedCommandStatus(commandId);
    }

    @Override
    public List<QueuedCommand> getNewCommands() {
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
  }

  private class KsqlServer {
    final KsqlEngine ksqlEngine;
    final KsqlResource ksqlResource;
    final CommandIdAssigner commandIdAssigner;
    final FakeCommandQueue fakeCommandQueue;
    final StatementExecutor statementExecutor;

    KsqlServer(final List<QueuedCommand> commandLog) {
      this.ksqlEngine = createKsqlEngine();
      this.commandIdAssigner = new CommandIdAssigner(ksqlEngine.getMetaStore());
      this.fakeCommandQueue = new FakeCommandQueue(
          new CommandIdAssigner(ksqlEngine.getMetaStore()),
          commandLog);
      this.ksqlResource = new KsqlResource(
          ksqlConfig,
          ksqlEngine,
          fakeCommandQueue,
          0
      );
      this.statementExecutor = new StatementExecutor(
          ksqlConfig,
          ksqlEngine,
          new StatementParser(ksqlEngine));
    }

    void recover() {
      this.statementExecutor.handleRestoration(this.fakeCommandQueue.getRestoreCommands());
    }

    void executeCommands() {
      fakeCommandQueue.getNewCommands().forEach(
          c -> statementExecutor.handleStatement(
              new QueuedCommand(c.getCommandId(), c.getCommand()))
      );
    }

    void submitCommands(final String ...statements) {
      for (final String statement : statements) {
        final Response response = ksqlResource.handleKsqlStatements(
            new KsqlRequest(statement, Collections.emptyMap()));
        assertThat(response.getStatus(), equalTo(200));
        executeCommands();
      }
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
    final Matcher<String> nameMatcher;
    final Matcher<String> kafkaNameMatcher;
    final Matcher<KsqlTopicSerDe> serDeMatcher;

    TopicMatcher(final KsqlTopic topic) {
      this.nameMatcher = equalTo(topic.getName());
      this.kafkaNameMatcher = equalTo(topic.getKafkaTopicName());
      this.serDeMatcher = instanceOf(topic.getKsqlTopicSerDe().getClass());
    }

    @Override
    public void describeTo(final Description description) {
      description.appendList(
          "Topic(", ", ", ")",
          Arrays.asList(nameMatcher, kafkaNameMatcher, serDeMatcher));
    }

    @Override
    public boolean matchesSafely(final KsqlTopic other, final Description description) {
      if (!test(nameMatcher, other.getName(), description, "name mismatch: ")) {
        return false;
      }
      if (!test(
          kafkaNameMatcher,
          other.getKafkaTopicName(),
          description,
          "kafka name mismatch: ")) {
        return false;
      }
      return test(
          serDeMatcher,
          other.getKsqlTopicSerDe(),
          description,
          "serde mismatch: ");
    }
  }

  private static Matcher<KsqlTopic> sameTopic(final KsqlTopic topic) {
    return new TopicMatcher(topic);
  }

  private static class StructuredDataSourceMatcher
      extends TypeSafeDiagnosingMatcher<StructuredDataSource> {
    final StructuredDataSource source;
    final Matcher<StructuredDataSource.DataSourceType> typeMatcher;
    final Matcher<String> nameMatcher;
    final Matcher<Schema> schemaMatcher;
    final Matcher<String> sqlMatcher;
    final Matcher<TimestampExtractionPolicy> extractionPolicyMatcher;
    final Matcher<KsqlTopic> topicMatcher;

    StructuredDataSourceMatcher(final StructuredDataSource source) {
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
        final StructuredDataSource other,
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

  private static Matcher<StructuredDataSource> sameSource(final StructuredDataSource source) {
    return new StructuredDataSourceMatcher(source);
  }

  private static class MetaStoreMatcher extends TypeSafeDiagnosingMatcher<MetaStore> {
    final Map<String, Matcher<StructuredDataSource>> sourceMatchers;

    MetaStoreMatcher(final MetaStore metaStore) {
      this.sourceMatchers = metaStore.getAllStructuredDataSources().entrySet().stream()
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
          other.getAllStructuredDataSourceNames(),
          description,
          "source set mismatch: ")) {
        return false;
      }
      for (final String name : sourceMatchers.keySet()) {
        if (!test(
            sourceMatchers.get(name),
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
    private final Matcher<Set<String>> sinkNamesMatcher;
    private final Matcher<Schema> resultSchemaMatcher;
    private final Matcher<String> sqlMatcher;
    private final Matcher<KafkaStreams.State> stateMatcher;

    PersistentQueryMetadataMatcher(final PersistentQueryMetadata metadata) {
      this.sourcesNamesMatcher = equalTo(metadata.getSourceNames());
      this.sinkNamesMatcher = equalTo(metadata.getSinkNames());
      this.resultSchemaMatcher = equalTo(metadata.getResultSchema());
      this.sqlMatcher = equalTo(metadata.getStatementString());
      this.stateMatcher = equalTo(metadata.getKafkaStreams().state());
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
          metadata.getSinkNames(),
          description,
          "sink names mismatch: "
      )) {
        return false;
      }
      if (!test(
          resultSchemaMatcher,
          metadata.getResultSchema(),
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
          metadata.getKafkaStreams().state(),
          description,
          "state mismatch: ");
    }
  }

  private static Matcher<PersistentQueryMetadata> sameQuery(
      final PersistentQueryMetadata metadata) {
    return new PersistentQueryMetadataMatcher(metadata);
  }

  private Map<QueryId, PersistentQueryMetadata> queriesById(
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
        server.ksqlEngine.getMetaStore().getAllStructuredDataSourceNames(),
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
        recovered.ksqlEngine.getMetaStore().getAllStructuredDataSourceNames(),
        contains("A"));
  }
}
