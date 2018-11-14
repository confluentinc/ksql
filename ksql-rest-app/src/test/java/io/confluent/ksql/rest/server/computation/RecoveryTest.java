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
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.FakeKafkaClientSupplier;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.easymock.EasyMock;
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

  private void assertTopicsEqual(final KsqlTopic topic, final KsqlTopic otherTopic) {
    assertThat(topic.getTopicName(), equalTo(otherTopic.getTopicName()));
    assertThat(topic.getKafkaTopicName(), equalTo(otherTopic.getKafkaTopicName()));
    assertThat(
        topic.getKsqlTopicSerDe().getClass(),
        equalTo(otherTopic.getKsqlTopicSerDe().getClass()));
  }

  private void assertTablesEqual(final KsqlTable table, final StructuredDataSource other) {
    assertThat(other, instanceOf(KsqlTable.class));
    final KsqlTable otherTable = (KsqlTable) other;
    assertThat(table.isWindowed(), equalTo(otherTable.isWindowed()));
  }

  private void assertSourcesEqual(
      final StructuredDataSource source,
      final StructuredDataSource other) {
    assertThat(source.getClass(), equalTo(other.getClass()));
    assertThat(source.getName(), equalTo(other.getName()));
    assertThat(source.getDataSourceType(), equalTo(other.getDataSourceType()));
    assertThat(source.getSchema(), equalTo(other.getSchema()));
    assertThat(source.getSqlExpression(), equalTo(other.getSqlExpression()));
    assertThat(
        source.getTimestampExtractionPolicy(), equalTo(other.getTimestampExtractionPolicy()));
    assertThat(source, anyOf(instanceOf(KsqlStream.class), instanceOf(KsqlTable.class)));
    assertTopicsEqual(source.getKsqlTopic(), other.getKsqlTopic());
    if (source instanceof KsqlTable) {
      assertTablesEqual((KsqlTable) source, other);
    }
  }

  final Map<QueryId, PersistentQueryMetadata> queriesById(
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
    assertThat(
        engine.getMetaStore().getAllStructuredDataSourceNames(),
        equalTo(recovered.getMetaStore().getAllStructuredDataSourceNames()));
    for (final String sourceName : engine.getMetaStore().getAllStructuredDataSourceNames()) {
      assertSourcesEqual(
          engine.getMetaStore().getSource(sourceName),
          recovered.getMetaStore().getSource(sourceName)
      );
    }
    final Map<QueryId, PersistentQueryMetadata> queries
        = queriesById(engine.getPersistentQueries());
    final Map<QueryId, PersistentQueryMetadata> recoveredQueries
        = queriesById(recovered.getPersistentQueries());
    assertThat(queries.keySet(), equalTo(recoveredQueries.keySet()));
    queries.forEach(
        (queryId, query) -> {
          final PersistentQueryMetadata recoveredQuery = recoveredQueries.get(query.getQueryId());
          assertThat(query.getSourceNames(), equalTo(recoveredQuery.getSourceNames()));
          assertThat(query.getSinkNames(), equalTo(recoveredQuery.getSinkNames()));
          assertThat(query.getResultSchema(), equalTo(recoveredQuery.getResultSchema()));
          assertThat(query.getStatementString(), equalTo(recoveredQuery.getStatementString()));
          assertThat(
              query.getKafkaStreams().state(),
              equalTo(recoveredQuery.getKafkaStreams().state()));
        }
    );
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
