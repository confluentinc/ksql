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
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
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

public class StandaloneExecutor implements Executable {

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  private static final Map<Class<? extends Statement>, Handler<Statement>> HANDLERS =
      ImmutableMap.<Class<? extends Statement>, Handler<Statement>>builder()
          .put(SetProperty.class,
              castHandler(StandaloneExecutor::handleSetProperty, SetProperty.class))
          .put(UnsetProperty.class,
              castHandler(StandaloneExecutor::handleUnsetProperty, UnsetProperty.class))
          .put(CreateStream.class,
              castHandler(StandaloneExecutor::handleExecutableDdl, CreateStream.class))
          .put(CreateTable.class,
              castHandler(StandaloneExecutor::handleExecutableDdl, CreateTable.class))
          .put(CreateStreamAsSelect.class,
              castHandler(StandaloneExecutor::handlePersistentQuery, CreateStreamAsSelect.class))
          .put(CreateTableAsSelect.class,
              castHandler(StandaloneExecutor::handlePersistentQuery, CreateTableAsSelect.class))
          .put(InsertInto.class,
              castHandler(StandaloneExecutor::handlePersistentQuery, InsertInto.class))
          .build();

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final String queriesFile;
  private final UdfLoader udfLoader;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Map<String, Object> configProperties = new HashMap<>();
  private final boolean failOnNoQueries;

  public static StandaloneExecutor create(
      final Properties properties,
      final String queriesFile,
      final String installDir
  ) {
    final KsqlConfig ksqlConfig = new KsqlConfig(properties);

    final KsqlEngine ksqlEngine = KsqlEngine.create(ksqlConfig);

    final UdfLoader udfLoader =
        UdfLoader.newInstance(ksqlConfig, ksqlEngine.getMetaStore(), installDir);

    return new StandaloneExecutor(ksqlConfig, ksqlEngine, queriesFile, udfLoader, true);
  }

  StandaloneExecutor(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final String queriesFile,
      final UdfLoader udfLoader,
      final boolean failOnNoQueries
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig can't be null");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine can't be null");
    this.queriesFile = Objects.requireNonNull(queriesFile, "queriesFile can't be null");
    this.udfLoader = Objects.requireNonNull(udfLoader, "udfLoader can't be null");
    this.failOnNoQueries = failOnNoQueries;
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
    final List<PreparedStatement<?>> preparedStatements =
        ksqlEngine.parseStatements(queries);

    if (failOnNoQueries) {
      final boolean noQueries = preparedStatements.stream()
          .map(PreparedStatement::getStatement)
          .noneMatch(stmt -> stmt instanceof QueryContainer);
      if (noQueries) {
        throw new KsqlException("The SQL file did not contain any queries");
      }
    }

    preparedStatements.forEach(this::executeStatement);
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> void executeStatement(
      final PreparedStatement<T> statement
  ) {
    final Class<? extends Statement> type = statement.getStatement().getClass();
    HANDLERS
        .getOrDefault(type, StandaloneExecutor::defaultHandler)
        .handle(this, (PreparedStatement)statement);
  }

  private void handleSetProperty(final PreparedStatement<SetProperty> statement) {
    final SetProperty setProperty = statement.getStatement();
    configProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
  }

  private void handleUnsetProperty(final PreparedStatement<UnsetProperty> statement) {
    configProperties.remove(statement.getStatement().getPropertyName());
  }

  private void handleExecutableDdl(final PreparedStatement<?> statement) {
    ksqlEngine.execute(statement.getStatementText(), ksqlConfig, configProperties);
  }

  private void handlePersistentQuery(final PreparedStatement<?> statement) {
    final List<QueryMetadata> queries =
        ksqlEngine.execute(statement.getStatementText(), ksqlConfig, configProperties);

    if (queries.size() != 1
        || !(queries.get(0) instanceof PersistentQueryMetadata)) {
      throw new KsqlException("Could not build the query: " + statement.getStatementText());
    }

    queries.get(0).start();
  }

  private void defaultHandler(final PreparedStatement<?> statement) {
    throw new KsqlException(String.format(
        "Ignoring statements: %s"
            + "%nOnly DDL (CREATE STREAM/TABLE, DROP STREAM/TABLE, SET, UNSET) "
            + "and DML(CSAS, CTAS and INSERT INTO) statements can run in standalone mode.",
        statement.getStatementText()));
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

  @SuppressWarnings({"unchecked", "unused"})
  private static <T extends Statement> Handler<Statement> castHandler(
      final Handler<? super T> handler,
      final Class<T> type) {
    return ((Handler<Statement>) handler);
  }

  private interface Handler<T extends Statement> {

    void handle(StandaloneExecutor executor, PreparedStatement<T> statement);
  }
}
