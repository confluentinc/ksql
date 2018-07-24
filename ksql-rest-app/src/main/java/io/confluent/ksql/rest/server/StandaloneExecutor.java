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
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
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
import java.util.function.Consumer;
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
  private final ImmutableMap<Class<? extends Statement>, Consumer<Pair<String, Statement>>>
      functionMap;
  private final Map<String, Object> configProperties = new HashMap<>();

  StandaloneExecutor(final KsqlConfig ksqlConfig,
                     final KsqlEngine ksqlEngine,
                     final String queriesFile,
                     final UdfLoader udfLoader) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig can't be null");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine can't be null");
    this.queriesFile = Objects.requireNonNull(queriesFile, "queriesFile can't be null");
    this.udfLoader = Objects.requireNonNull(udfLoader, "udfLoader can't be null");
    functionMap =
        ImmutableMap.<Class<? extends Statement>, Consumer<Pair<String, Statement>>>builder()
        .put(SetProperty.class, (statementPair) -> {
          final SetProperty setProperty = (SetProperty) statementPair.getRight();
          configProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
        })
        .put(UnsetProperty.class, (Pair<String, Statement> statementPair) -> {
          final UnsetProperty unsetProperty = (UnsetProperty) statementPair.getRight();
          configProperties.remove(unsetProperty.getPropertyName());
        })
        .put(CreateStream.class, (Pair<String, Statement> statementPair) -> {
          ksqlEngine.buildMultipleQueries(statementPair.getLeft(),
              ksqlConfig,
              configProperties);
        })
        .put(CreateTable.class, (Pair<String, Statement> statementPair) -> {
          ksqlEngine.buildMultipleQueries(statementPair.getLeft(),
              ksqlConfig,
              configProperties);
        })
        .put(CreateStreamAsSelect.class, (Pair<String, Statement> statementPair) -> {
          handlePersistentQuery(statementPair.getRight(),
              statementPair.getLeft(),
              configProperties);
        })
        .put(CreateTableAsSelect.class, (Pair<String, Statement> statementPair) -> {
          handlePersistentQuery(statementPair.getRight(),
              statementPair.getLeft(),
              configProperties);
        })
        .put(InsertInto.class, (Pair<String, Statement> statementPair) -> {
          handlePersistentQuery(statementPair.getRight(),
              statementPair.getLeft(),
              configProperties);
        })
        .build();
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
    return new StandaloneExecutor(ksqlConfig, ksqlEngine, queriesFile, udfLoader);
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
    final List<Pair<String, Statement>> statementPairs =
        ksqlEngine.parseStatements(queries, ksqlEngine.getMetaStore().clone());
    for (final Pair<String, Statement> statementPair: statementPairs) {
      final String statementString = statementPair.getLeft();
      final Statement statement = statementPair.getRight();
      if (functionMap.containsKey(statement.getClass())) {
        functionMap.get(statement.getClass()).accept(statementPair);
      } else {
        final String message = String.format(
            "Ignoring statements: %s"
                + "%nOnly DDL (CREATE STREAM/TABLE, DROP STREAM/TABLE, SET, UNSET) "
                + "and DML(CSAS, CTAS and INSERT INTO) statements can run in standalone mode.",
            statementPair.getLeft()
        );
        System.err.println(message);
        log.warn(message);
      }
    }

  }


  private void handlePersistentQuery(
      final Statement statement,
      final String statementString,
      final Map<String, Object> configProperties) {
    final Query query;
    if (statement instanceof CreateAsSelect) {
      query = ((CreateAsSelect) statement).getQuery();
    } else if (statement instanceof InsertInto) {
      query = ((InsertInto) statement).getQuery();
    } else {
      throw new KsqlException("Only CSAS/CTAS and INSERT INTO are persistent queries: "
          + statementString);
    }

    final QueryMetadata queryMetadata =
        ksqlEngine.getQueryExecutionPlan(query, ksqlConfig);
    if (statement instanceof CreateAsSelect) {
      validateCsasCtas(queryMetadata, (CreateAsSelect) statement);
    }
    final List<QueryMetadata> queryMetadataList =
        ksqlEngine.buildMultipleQueries(statementString, ksqlConfig, configProperties);
    if (queryMetadataList.isEmpty()
        || !(queryMetadataList.get(0) instanceof PersistentQueryMetadata)) {
      throw new KsqlException("Could not build the query: " + statementString);
    }
    queryMetadataList.get(0).start();
  }

  private void validateCsasCtas(final QueryMetadata queryMetadata,
      final CreateAsSelect createAsSelect) {
    if (createAsSelect instanceof CreateStreamAsSelect
        && queryMetadata.getDataSourceType() == DataSource.DataSourceType.KTABLE) {
      throw new KsqlException("Invalid result type. Your SELECT query produces a TABLE. "
          + "Please use CREATE TABLE AS SELECT statement instead.");
    } else if (createAsSelect instanceof CreateTableAsSelect
        && queryMetadata.getDataSourceType() == DataSourceType.KSTREAM) {
      throw new KsqlException("Invalid result type. Your SELECT query produces a STREAM. "
          + "Please use CREATE STREAM AS SELECT statement instead.");
    }
  }

  private static String readQueriesFile(final String queryFilePath) {
    try {
      return new String(java.nio.file.Files.readAllBytes(
          Paths.get("src/test/resources/SampleMultilineStatements.sql")), "UTF-8");

    } catch (IOException e) {
      throw new KsqlException(
          String.format("Could not read the query file: %s. Details: %s",
              queryFilePath, e.getMessage()),
          e);
    }
  }

  public Map<String, Object> getConfigProperties() {
    return configProperties;
  }
}
