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
 **/

package io.confluent.ksql.rest.server;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import java.io.Console;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class StandaloneExecutor implements Executable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final String queriesFile;
  private final UdfLoader udfLoader;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Map<String, Object> configProperties = new HashMap<>();
  private final boolean failOnNoQueries;

  StandaloneExecutor(final KsqlConfig ksqlConfig,
                     final KsqlEngine ksqlEngine,
                     final String queriesFile,
                     final UdfLoader udfLoader,
                     final boolean failOnNoQueries) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig can't be null");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine can't be null");
    this.queriesFile = Objects.requireNonNull(queriesFile, "queriesFile can't be null");
    this.udfLoader = Objects.requireNonNull(udfLoader, "udfLoader can't be null");
    this.failOnNoQueries = failOnNoQueries;
  }

  private interface Handler<T extends Statement> {
    void handle(StandaloneExecutor executor, String queryString, T statement);
  }

  private static final Map<Class<? extends Statement>, Handler<Statement>> HANDLERS =
      ImmutableMap.<Class<? extends Statement>, Handler<Statement>>builder()
          .put(SetProperty.class,
              castHandler(StandaloneExecutor::handleSetProperty, SetProperty.class))
          .put(UnsetProperty.class,
              castHandler(StandaloneExecutor::handleUnsetProperty, UnsetProperty.class))
          .put(CreateStream.class,
              castHandler(StandaloneExecutor::handleCreateStream, CreateStream.class))
          .put(CreateTable.class,
              castHandler(StandaloneExecutor::handleCreateTable, CreateTable.class))
          .put(CreateStreamAsSelect.class,
              castHandler(StandaloneExecutor::handleCreateStreamAsSelect,
                  CreateStreamAsSelect.class))
          .put(CreateTableAsSelect.class, castHandler(StandaloneExecutor::handleCreateTableAsSelect,
              CreateTableAsSelect.class))
          .put(InsertInto.class,
              castHandler(StandaloneExecutor::handleInsertInto, InsertInto.class))
          .build();

  // Each member function is passed its required arguments, (pre-cast to the right type).
  @SuppressWarnings("unused")
  private void handleSetProperty(final String ignored, final SetProperty statement) {
    configProperties.put(statement.getPropertyName(), statement.getPropertyValue());
  }

  @SuppressWarnings("unused")
  private void handleUnsetProperty(final String ignored, final UnsetProperty statement) {
    configProperties.remove(statement.getPropertyName());
  }

  @SuppressWarnings("unused")
  private void handleCreateStream(final String queryString, final CreateStream ignored) {
    ksqlEngine.buildMultipleQueries(queryString, ksqlConfig, configProperties);
  }

  @SuppressWarnings("unused")
  private void handleCreateTable(final String queryString, final CreateTable ignored) {
    ksqlEngine.buildMultipleQueries(queryString, ksqlConfig, configProperties);
  }

  @SuppressWarnings("unused")
  private void handleCreateStreamAsSelect(
      final String queryString,
      final CreateStreamAsSelect statement) {
    final Query query = statement.getQuery();
    final QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(query, ksqlConfig);
    if (queryMetadata.getDataSourceType() != DataSourceType.KSTREAM) {
      throw new KsqlException("Invalid result type. Your SELECT query produces a STREAM. Please "
          + "use CREATE STREAM AS SELECT statement instead. Query: " + queryString);
    }

    handlePersistentQuery(queryString, configProperties);
  }

  @SuppressWarnings("unused")
  private void handleCreateTableAsSelect(
      final String queryString,
      final CreateTableAsSelect statement) {
    final Query query = statement.getQuery();
    final QueryMetadata queryMetadata = ksqlEngine.getQueryExecutionPlan(query, ksqlConfig);
    if (queryMetadata.getDataSourceType() != DataSourceType.KTABLE) {
      throw new KsqlException("Invalid result type. Your SELECT query produces a TABLE. Please "
          + "use CREATE TABLE AS SELECT statement instead. Query: " + queryString);
    }

    handlePersistentQuery(queryString, configProperties);
  }

  @SuppressWarnings("unused")
  private void handleInsertInto(final String queryString, final InsertInto statement) {
    final Query query = statement.getQuery();
    ksqlEngine.getQueryExecutionPlan(query, ksqlConfig);

    handlePersistentQuery(queryString, configProperties);
  }

  // Define a default handler to call when no other handler registered.
  @SuppressWarnings("unused")
  private void defaultHandler(final String queryString, final Statement ignored) {
    final String message = String.format(
            "Ignoring statements: %s"
                + "%nOnly DDL (CREATE STREAM/TABLE, DROP STREAM/TABLE, SET, UNSET) "
                + "and DML(CSAS, CTAS and INSERT INTO) statements can run in standalone mode.",
        queryString
        );
    System.err.println(message);
    log.warn(message);
    throw new KsqlException(message);
  }

  private static <T extends Statement> Handler<Statement> castHandler(
      final Handler<? super T> handler,
      final Class<T> type) {
    return (executor, queryString, statement) ->
        handler.handle(executor, queryString, type.cast(statement));
  }


  public void start() {
    try {
      udfLoader.load();
      executeStatements(readQueriesFile(queriesFile));
      showWelcomeMessage();
    } catch (final Exception e) {
      log.error("Failed to start KSQL Server with query file: " + queriesFile, e);
      stop();
      throw e;
    }
  }

  public void stop() {
    try {
      ksqlEngine.close();
    } catch (final Exception e) {
      log.warn("Failed to cleanly shutdown the KSQL Engine", e);
    }
    shutdownLatch.countDown();
  }

  @Override
  public void join() throws InterruptedException {
    shutdownLatch.await();
  }

  public static StandaloneExecutor create(final Properties properties,
                                          final String queriesFile,
                                          final String installDir) {
    final KsqlConfig ksqlConfig = new KsqlConfig(properties);
    final KsqlEngine ksqlEngine = KsqlEngine.create(ksqlConfig);
    final UdfLoader udfLoader = UdfLoader.newInstance(ksqlConfig,
        ksqlEngine.getMetaStore(),
        installDir);
    return new StandaloneExecutor(ksqlConfig, ksqlEngine, queriesFile, udfLoader, true);
  }

  private void showWelcomeMessage() {
    final Console console = System.console();
    if (console == null) {
      return;
    }
    final PrintWriter writer =
        new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    WelcomeMsgUtils.displayWelcomeMessage(80, writer);
    writer.printf("Server %s started with query file %s. Interactive mode is disabled.%n",
                  Version.getVersion(),
                  queriesFile);

    writer.flush();
  }

  private void executeStatements(final String queries) {
    final List<PreparedStatement> preparedStatements = ksqlEngine.parseStatements(queries);

    if (failOnNoQueries) {
      final boolean noQueries = preparedStatements.stream()
          .map(PreparedStatement::getStatement)
          .noneMatch(stmt -> stmt instanceof QueryContainer);
      if (noQueries) {
        throw new KsqlException("The SQL file did not contain any queries");
      }
    }

    for (final PreparedStatement preparedStatement: preparedStatements) {
      final Statement statement = preparedStatement.getStatement();
      HANDLERS
          .getOrDefault(statement.getClass(), StandaloneExecutor::defaultHandler)
          .handle(this, preparedStatement.getStatementText(), statement);
    }
  }

  private void handlePersistentQuery(
      final String statementString,
      final Map<String, Object> configProperties) {
    if (ksqlEngine.hasReachedMaxNumberOfPersistentQueries(ksqlConfig)) {
      throw new KsqlException(
          String.format(
              "Not executing query '%s' since the limit on number of active, persistent queries "
                  + "has been reached (%d persistent queries currently running. Limit is %d). "
                  + "This limit can be reconfigured via the 'ksql-server.properties' file.",
              statementString,
              ksqlEngine.numberOfPersistentQueries(),
              ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
          )
      );
    }
    final List<QueryMetadata> queryMetadataList =
        ksqlEngine.buildMultipleQueries(statementString, ksqlConfig, configProperties);
    if (queryMetadataList.size() != 1
        || !(queryMetadataList.get(0) instanceof PersistentQueryMetadata)) {
      throw new KsqlException("Could not build the query: " + statementString);
    }
    queryMetadataList.get(0).start();
  }

  private static String readQueriesFile(final String queryFilePath) {
    try {
      return new String(java.nio.file.Files.readAllBytes(
          Paths.get(queryFilePath)), StandardCharsets.UTF_8);

    } catch (IOException e) {
      throw new KsqlException(
          String.format("Could not read the query file: %s. Details: %s",
              queryFilePath, e.getMessage()),
          e);
    }
  }

  public Map<String, ?> getConfigProperties() {
    return configProperties;
  }
}
