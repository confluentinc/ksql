/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QueryContainer;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.processing.log.ProcessingLogConfig;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneExecutor implements Executable {

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  private final ServiceContext serviceContext;
  private final ProcessingLogConfig processingLogConfig;
  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final String queriesFile;
  private final UdfLoader udfLoader;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Map<String, Object> configProperties = new HashMap<>();
  private final boolean failOnNoQueries;
  private final VersionCheckerAgent versionChecker;
  private final Function<ServiceContext, SchemaInjector> schemaInjectorFactory;

  StandaloneExecutor(
      final ServiceContext serviceContext,
      final ProcessingLogConfig processingLogConfig,
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final String queriesFile,
      final UdfLoader udfLoader,
      final boolean failOnNoQueries,
      final VersionCheckerAgent versionChecker,
      final Function<ServiceContext, SchemaInjector> schemaInjectorFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogConfig = Objects.requireNonNull(processingLogConfig, "processingLogConfig");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.queriesFile = Objects.requireNonNull(queriesFile, "queriesFile");
    this.udfLoader = Objects.requireNonNull(udfLoader, "udfLoader");
    this.failOnNoQueries = failOnNoQueries;
    this.versionChecker = Objects.requireNonNull(versionChecker, "versionChecker");
    this.schemaInjectorFactory = Objects
        .requireNonNull(schemaInjectorFactory, "schemaInjectorFactory");
  }

  public void start() {
    try {
      udfLoader.load();
      ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
          serviceContext.getTopicClient(),
          processingLogConfig,
          ksqlConfig);
      if (processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE)) {
        log.warn("processing log auto-create is enabled, but this is not supported "
            + "for headless mode.");
      }
      processesQueryFile(readQueriesFile(queriesFile));
      showWelcomeMessage();
      final Properties properties = new Properties();
      properties.putAll(configProperties);
      versionChecker.start(KsqlModuleType.SERVER, properties);
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
    try {
      serviceContext.close();
    } catch (final Exception e) {
      log.warn("Failed to cleanly shutdown services", e);
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

  private void processesQueryFile(final String queries) {
    final List<ParsedStatement> preparedStatements = ksqlEngine.parse(queries);

    validateStatements(preparedStatements);

    final SchemaInjector schemaInjector = schemaInjectorFactory.apply(serviceContext);

    executeStatements(
        preparedStatements,
        new StatementExecutor(ksqlEngine, schemaInjector, configProperties, ksqlConfig)
    );

    ksqlEngine.getPersistentQueries().forEach(QueryMetadata::start);
  }

  private void validateStatements(final List<ParsedStatement> statements) {
    final SchemaInjector schemaInjector = schemaInjectorFactory
        .apply(SandboxedServiceContext.create(serviceContext));

    final StatementExecutor sandboxExecutor = new StatementExecutor(
        ksqlEngine.createSandbox(),
        schemaInjector,
        new HashMap<>(configProperties),
        ksqlConfig
    );

    final boolean hasQueries = executeStatements(statements, sandboxExecutor);

    if (failOnNoQueries && !hasQueries) {
      throw new KsqlException("The SQL file did not contain any queries");
    }
  }

  private static boolean executeStatements(
      final List<ParsedStatement> statements,
      final StatementExecutor executor
  ) {
    boolean hasQueries = false;

    for (final ParsedStatement parsed : statements) {
      hasQueries |= executor.execute(parsed);
    }

    return hasQueries;
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

  private static final class StatementExecutor {

    private static final Map<Class<? extends Statement>, Handler<Statement>> HANDLERS =
        ImmutableMap.<Class<? extends Statement>, Handler<Statement>>builder()
            .put(SetProperty.class, createHandler(
                StatementExecutor::handleSetProperty,
                SetProperty.class,
                "SET"))
            .put(UnsetProperty.class, createHandler(
                StatementExecutor::handleUnsetProperty,
                UnsetProperty.class,
                "UNSET"))
            .put(CreateStream.class, createHandler(
                StatementExecutor::handleExecutableDdl,
                CreateStream.class,
                "CREATE STREAM"))
            .put(CreateTable.class, createHandler(
                StatementExecutor::handleExecutableDdl,
                CreateTable.class,
                "CREATE TABLE"))
            .put(CreateStreamAsSelect.class, createHandler(
                StatementExecutor::handlePersistentQuery,
                CreateStreamAsSelect.class,
                "CREAETE STREAM AS SELECT"))
            .put(CreateTableAsSelect.class, createHandler(
                StatementExecutor::handlePersistentQuery,
                CreateTableAsSelect.class,
                "CREATE TABLE AS SELECT"))
            .put(InsertInto.class, createHandler(
                StatementExecutor::handlePersistentQuery,
                InsertInto.class,
                "INSERT INTO"))
            .build();

    private static final String SUPPORTED_STATEMENTS = generateSupportedMessage();

    private final KsqlExecutionContext executionContext;
    private final SchemaInjector schemaInjector;
    private final Map<String, Object> configProperties;
    private final KsqlConfig ksqlConfig;

    private StatementExecutor(
        final KsqlExecutionContext executionContext,
        final SchemaInjector schemaInjector,
        final Map<String, Object> configProperties,
        final KsqlConfig ksqlConfig
    ) {
      this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
      this.schemaInjector = Objects.requireNonNull(schemaInjector, "schemaInjector");
      this.configProperties = Objects.requireNonNull(configProperties, "configProperties");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    }

    /**
     * @return true if the statement contained a query, false otherwise
     */
    @SuppressWarnings("unchecked")
    boolean execute(final ParsedStatement statement) {
      final PreparedStatement<?> prepared = prepare(statement);

      throwOnMissingSchema(prepared);

      final Handler<Statement> handler = HANDLERS.get(prepared.getStatement().getClass());
      if (handler == null) {
        throw new KsqlStatementException("Unsupported statement. "
            + "Only the following statements are supporting in standalone mode:"
            + System.lineSeparator()
            + SUPPORTED_STATEMENTS,
            statement.getStatementText());
      }

      handler.handle(this, (PreparedStatement) prepared);
      return prepared.getStatement() instanceof QueryContainer;
    }

    private PreparedStatement<?> prepare(final ParsedStatement statement) {
      final PreparedStatement<?> prepared = executionContext.prepare(statement);
      return schemaInjector.forStatement(prepared);
    }

    private static void throwOnMissingSchema(final PreparedStatement<?> statement) {
      if (!(statement.getStatement() instanceof AbstractStreamCreateStatement)) {
        return;
      }

      if (!((AbstractStreamCreateStatement) statement.getStatement()).getElements().isEmpty()) {
        return;
      }

      throw new KsqlStatementException("statement does not define the schema "
          + "and the supplied format does not support schema inference",
          statement.getStatementText());
    }

    private void handleSetProperty(final PreparedStatement<SetProperty> statement) {
      final SetProperty setProperty = statement.getStatement();
      configProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
    }

    private void handleUnsetProperty(final PreparedStatement<UnsetProperty> statement) {
      configProperties.remove(statement.getStatement().getPropertyName());
    }

    private void handleExecutableDdl(final PreparedStatement<?> statement) {
      executionContext.execute(statement, ksqlConfig, configProperties);
    }

    private void handlePersistentQuery(final PreparedStatement<?> statement) {
      executionContext.execute(statement, ksqlConfig, configProperties)
          .getQuery()
          .filter(q -> q instanceof PersistentQueryMetadata)
          .orElseThrow((() -> new KsqlStatementException(
              "Could not build the query",
              statement.getStatementText())));
    }

    private static String generateSupportedMessage() {
      return HANDLERS.values().stream()
          .map(Handler::getName)
          .sorted()
          .collect(Collectors.joining(System.lineSeparator()));
    }

    @SuppressWarnings({"unchecked", "unused"})
    private static <T extends Statement> Handler<Statement> createHandler(
        final BiConsumer<StatementExecutor, PreparedStatement<T>> handler,
        final Class<T> type,
        final String name
    ) {

      return new StatementExecutor.Handler<Statement>() {
        @Override
        public void handle(
            final StatementExecutor executor,
            final PreparedStatement<Statement> statement
        ) {
          handler.accept(executor, (PreparedStatement) statement);
        }

        @Override
        public String getName() {
          return name;
        }
      };
    }

    private interface Handler<T extends Statement> {

      void handle(StatementExecutor executor, PreparedStatement<T> statement);

      String getName();
    }
  }
}
