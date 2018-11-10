package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
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
    return TestUtils.createKsqlEngine(
        ksqlConfig,
        new MockKafkaTopicClient(),
        new MockSchemaRegistryClientFactory()::get);
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
              c.getCommand(), c.getCommandId(), Optional.empty())
      );
    }

    void submitCommands(final String ...statements) {
      for (final String statement : statements) {
        ksqlResource.handleKsqlStatements(new KsqlRequest(statement, Collections.emptyMap()));
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

  private void shouldRecoverCorrectly(final List<QueuedCommand> commands) {
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
    final Collection<PersistentQueryMetadata> queries = engine.getPersistentQueries();
    final Collection<PersistentQueryMetadata> recoveredQueries = recovered.getPersistentQueries();
    assertThat(queries.size(), equalTo(recoveredQueries.size()));
    final Map<QueryId, PersistentQueryMetadata> recoveredQueriesById
        = recoveredQueries.stream().collect(
            Collectors.toMap(PersistentQueryMetadata::getQueryId, pqm -> pqm));
    for (final PersistentQueryMetadata query : queries) {
      assertThat(recoveredQueriesById.keySet(), contains(query.getQueryId()));
      final PersistentQueryMetadata recoveredQuery = recoveredQueriesById.get(query.getQueryId());
      assertThat(query.getSourceNames(), equalTo(recoveredQuery.getSourceNames()));
      assertThat(query.getSinkNames(), equalTo(recoveredQuery.getSinkNames()));
    }
  }

  @Test
  public void shouldRecoverCreates() {
    server1.submitCommands(
        "CREATE STREAM A COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;"
    );
    shouldRecoverCorrectly(commands);
  }

  @Test
  public void shouldRecoverTerminates() {
    server1.submitCommands(
        "CREATE STREAM A COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;"
    );
  }

  @Test
  public void shouldRecoverLogWithRepeatedTerminatesCorrectly() {
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
    shouldRecoverCorrectly(commands);
  }

  @Test
  public void shouldRecoverDropCorrectly() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B AS SELECT * FROM A;",
        "TERMINATE CSAS_B_0;"
    );
    server2.executeCommands();
    server1.submitCommands("INSERT INTO B SELECT * FROM A;");
    server2.submitCommands("DROP STREAM B;");
    shouldRecoverCorrectly(commands);
  }

  @Test
  public void shouldRecoverLogWithTerminatesCorrectly() {
    server1.submitCommands(
        "CREATE STREAM A (COLUMN STRING) WITH (KAFKA_TOPIC='A', VALUE_FORMAT='JSON');",
        "CREATE STREAM B (COLUMN STRING) WITH (KAFKA_TOPIC='B', VALUE_FORMAT='JSON');"
    );
    server2.executeCommands();
    server1.submitCommands("INSERT INTO B SELECT * FROM A;");
    server2.submitCommands("DROP STREAM B;");
    server1.submitCommands("TERMINATE InsertQuery_0;");
    shouldRecoverCorrectly(commands);
  }
}
