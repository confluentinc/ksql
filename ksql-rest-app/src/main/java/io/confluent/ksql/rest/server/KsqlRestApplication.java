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

package io.confluent.ksql.rest.server;

import static io.confluent.ksql.rest.server.KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG;

import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.TopicAccessValidator;
import io.confluent.ksql.engine.TopicAccessValidatorFactory;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.rest.server.context.KsqlRestServiceContextBinder;
import io.confluent.ksql.rest.server.filters.KsqlAuthorizationFilter;
import io.confluent.ksql.rest.server.resources.KsqlExceptionMapper;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.server.security.KsqlDefaultSecurityExtension;
import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.server.state.ServerStateDynamicBinding;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.LazyServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.Application;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.io.Console;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;
import javax.ws.rs.core.Configurable;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class KsqlRestApplication extends Application<KsqlRestConfig> implements Executable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  public static final String COMMANDS_STREAM_NAME = "KSQL_COMMANDS";

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final CommandStore commandStore;
  private final RootDocument rootDocument;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final VersionCheckerAgent versionCheckerAgent;
  private final ServiceContext serviceContext;
  private final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder>  serviceContextBinderFactory;
  private final KsqlSecurityExtension securityExtension;
  private final ServerState serverState;
  private final ProcessingLogContext processingLogContext;
  private final List<KsqlServerPrecondition> preconditions;

  public static String getCommandsStreamName() {
    return COMMANDS_STREAM_NAME;
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  KsqlRestApplication(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlRestConfig config,
      final CommandRunner commandRunner,
      final CommandStore commandStore,
      final RootDocument rootDocument,
      final StatusResource statusResource,
      final StreamedQueryResource streamedQueryResource,
      final KsqlResource ksqlResource,
      final VersionCheckerAgent versionCheckerAgent,
      final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory,
      final KsqlSecurityExtension securityExtension,
      final ServerState serverState,
      final ProcessingLogContext processingLogContext,
      final List<KsqlServerPrecondition> preconditions
  ) {
    super(config);
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");
    this.rootDocument = Objects.requireNonNull(rootDocument, "rootDocument");
    this.statusResource = Objects.requireNonNull(statusResource, "statusResource");
    this.streamedQueryResource =
        Objects.requireNonNull(streamedQueryResource, "streamedQueryResource");
    this.ksqlResource = Objects.requireNonNull(ksqlResource, "ksqlResource");
    this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
    this.serverState = Objects.requireNonNull(serverState, "serverState");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.preconditions = Objects.requireNonNull(preconditions, "preconditions");
    this.versionCheckerAgent =
        Objects.requireNonNull(versionCheckerAgent, "versionCheckerAgent");
    this.serviceContextBinderFactory = Objects.requireNonNull(
        serviceContextBinderFactory, "serviceContextBinderFactory");
    this.securityExtension = Objects.requireNonNull(
        securityExtension, "securityExtension"
    );
  }

  @Override
  public void setupResources(final Configurable<?> config, final KsqlRestConfig appConfig) {
    config.register(rootDocument);
    config.register(new ServerInfoResource(serviceContext, ksqlConfig));
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(new KsqlExceptionMapper());
    config.register(new ServerStateDynamicBinding(serverState));
  }

  @Override
  public void start() throws Exception {
    super.start();
    startKsql();
    commandRunner.start();
    final Properties metricsProperties = new Properties();
    metricsProperties.putAll(getConfiguration().getOriginals());
    if (versionCheckerAgent != null) {
      versionCheckerAgent.start(KsqlModuleType.SERVER, metricsProperties);
    }
    displayWelcomeMessage();
  }

  @VisibleForTesting
  void startKsql() {
    waitForPreconditions();
    initialize();
  }

  private static final class KsqlFailedPrecondition extends RuntimeException {

    private KsqlFailedPrecondition(final String message) {
      super(message);
    }
  }

  private void checkPreconditions() {
    for (final KsqlServerPrecondition precondition : preconditions) {
      final Optional<String> error = precondition.checkPrecondition(
          config,
          serviceContext
      );
      if (error.isPresent()) {
        serverState.setInitializingReason(error.get());
        throw new KsqlFailedPrecondition(error.get());
      }
    }
  }

  private void waitForPreconditions() {
    final List<Predicate<Exception>> predicates = ImmutableList.of(
        e -> !(e instanceof KsqlFailedPrecondition)
    );
    RetryUtil.retryWithBackoff(
        Integer.MAX_VALUE,
        1000,
        30000,
        this::checkPreconditions,
        predicates
    );
  }

  private void initialize() {
    registerCommandTopic();

    commandStore.start();

    ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        serviceContext.getTopicClient(),
        processingLogContext.getConfig(),
        ksqlConfig
    );
    maybeCreateProcessingLogStream(
        processingLogContext.getConfig(),
        ksqlConfig,
        ksqlEngine,
        commandStore
    );

    commandRunner.processPriorCommands();

    serverState.setReady();
  }

  @Override
  public void stop() {
    try {
      ksqlEngine.close();
    } catch (final Exception e) {
      log.error("Exception while waiting for Ksql Engine to close", e);
    }

    try {
      commandRunner.close();
    } catch (final Exception e) {
      log.error("Exception while waiting for CommandRunner thread to complete", e);
    }

    try {
      serviceContext.close();
    } catch (final Exception e) {
      log.error("Exception while closing services", e);
    }

    try {
      securityExtension.close();
    } catch (final Exception e) {
      log.error("Exception while closing security extension", e);
    }

    try {
      super.stop();
    } catch (final Exception e) {
      log.error("Exception while stopping rest server", e);
    }
  }

  public List<URL> getListeners() {
    return Arrays.stream(server.getConnectors())
        .filter(connector -> connector instanceof ServerConnector)
        .map(ServerConnector.class::cast)
        .map(connector -> {
          try {
            final String protocol = new HashSet<>(connector.getProtocols())
                .stream()
                .map(String::toLowerCase)
                .anyMatch(s -> s.equals("ssl")) ? "https" : "http";

            final int localPort = connector.getLocalPort();

            return new URL(protocol, "localhost", localPort, "");
          } catch (final Exception e) {
            throw new RuntimeException("Malformed listener", e);
          }
        })
        .collect(Collectors.toList());
  }

  @Override
  public void configureBaseApplication(
      final Configurable<?> config,
      final Map<String, String> metricTags) {
    // Would call this but it registers additional, unwanted exception mappers
    // super.configureBaseApplication(config, metricTags);
    // Instead, just copy+paste the desired parts from Application.configureBaseApplication() here:
    final JacksonMessageBodyProvider jsonProvider =
        new JacksonMessageBodyProvider(JsonMapper.INSTANCE.mapper);
    config.register(jsonProvider);
    config.register(JsonParseExceptionMapper.class);
    config.register(serviceContextBinderFactory.apply(ksqlConfig, securityExtension));

    // Don't want to buffer rows when streaming JSON in a request to the query resource
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    config.property(ServerProperties.WADL_FEATURE_DISABLE, true);

    // Controls the access to all REST endpoints
    securityExtension.getAuthorizationProvider().ifPresent(
        ac -> config.register(new KsqlAuthorizationFilter(ac))
    );
  }

  @Override
  protected void registerWebSocketEndpoints(final ServerContainer container) {
    try {
      final ListeningScheduledExecutorService exec = MoreExecutors.listeningDecorator(
          Executors.newScheduledThreadPool(
              config.getInt(KsqlRestConfig.KSQL_WEBSOCKETS_NUM_THREADS),
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("websockets-query-thread-%d")
                  .build()
          )
      );

      final StatementParser statementParser = new StatementParser(ksqlEngine);
      final TopicAccessValidator topicAccessValidator =
          TopicAccessValidatorFactory.create(ksqlConfig, serviceContext);

      container.addEndpoint(
          ServerEndpointConfig.Builder
              .create(
                  WSQueryEndpoint.class,
                  WSQueryEndpoint.class.getAnnotation(ServerEndpoint.class).value()
              )
              .configurator(new Configurator() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> T getEndpointInstance(final Class<T> endpointClass) {
                  return (T) new WSQueryEndpoint(
                      ksqlConfig,
                      JsonMapper.INSTANCE.mapper,
                      statementParser,
                      ksqlEngine,
                      commandStore,
                      exec,
                      versionCheckerAgent::updateLastRequestTime,
                      Duration.ofMillis(config.getLong(
                          KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
                      topicAccessValidator,
                      securityExtension,
                      serverState
                  );
                }
              })
              .build()
      );
    } catch (final DeploymentException e) {
      log.error("Unable to create websockets endpoint", e);
    }
  }

  public static KsqlRestApplication buildApplication(
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final int maxStatementRetries
  ) {
    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());
    final ServiceContext serviceContext
        = new LazyServiceContext(() -> DefaultServiceContext.create(ksqlConfig));

    return buildApplication(
        restConfig,
        versionCheckerFactory,
        maxStatementRetries,
        serviceContext,
        KsqlRestServiceContextBinder::new);
  }

  static KsqlRestApplication buildApplication(
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final int maxStatementRetries,
      final ServiceContext serviceContext,
      final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory
  ) {
    final String ksqlInstallDir = restConfig.getString(KsqlRestConfig.INSTALL_DIR_CONFIG);

    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());

    final ProcessingLogConfig processingLogConfig
        = new ProcessingLogConfig(restConfig.getOriginals());
    final ProcessingLogContext processingLogContext
        = ProcessingLogContext.create(processingLogConfig);

    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();

    final KsqlEngine ksqlEngine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        ServiceInfo.create(ksqlConfig));

    UdfLoader.newInstance(ksqlConfig, functionRegistry, ksqlInstallDir).load();

    final String commandTopic = KsqlInternalTopicUtils.getTopicName(
        ksqlConfig, KsqlRestConfig.COMMAND_TOPIC_SUFFIX);

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    final CommandStore commandStore = CommandStore.Factory.create(
        commandTopic,
        restConfig.getCommandConsumerProperties(),
        restConfig.getCommandProducerProperties());

    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser
    );

    final RootDocument rootDocument = new RootDocument();

    final StatusResource statusResource = new StatusResource(statementExecutor);
    final VersionCheckerAgent versionChecker
        = versionCheckerFactory.apply(ksqlEngine::hasActiveQueries);

    final ServerState serverState = new ServerState();

    final KsqlSecurityExtension securityExtension = loadSecurityExtension(ksqlConfig);

    final TopicAccessValidator topicAccessValidator =
        TopicAccessValidatorFactory.create(ksqlConfig, serviceContext);

    final StreamedQueryResource streamedQueryResource = new StreamedQueryResource(
        ksqlConfig,
        ksqlEngine,
        statementParser,
        commandStore,
        Duration.ofMillis(
            restConfig.getLong(KsqlRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)),
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime,
        topicAccessValidator
    );

    final KsqlResource ksqlResource = new KsqlResource(
        ksqlConfig,
        ksqlEngine,
        commandStore,
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime,
        Injectors.DEFAULT,
        topicAccessValidator);

    final List<String> managedTopics = new LinkedList<>();
    managedTopics.add(commandTopic);
    if (processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)) {
      managedTopics.add(ProcessingLogServerUtils.getTopicName(processingLogConfig, ksqlConfig));
    }
    final CommandRunner commandRunner = new CommandRunner(
        statementExecutor,
        commandStore,
        maxStatementRetries,
        new ClusterTerminator(ksqlConfig, ksqlEngine, serviceContext, managedTopics),
        serverState
    );

    final List<KsqlServerPrecondition> preconditions = restConfig.getConfiguredInstances(
        KsqlRestConfig.KSQL_SERVER_PRECONDITIONS,
        KsqlServerPrecondition.class
    );

    return new KsqlRestApplication(
        serviceContext,
        ksqlEngine,
        ksqlConfig,
        restConfig,
        commandRunner,
        commandStore,
        rootDocument,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        versionChecker,
        serviceContextBinderFactory,
        securityExtension,
        serverState,
        processingLogContext,
        preconditions
    );
  }

  private void registerCommandTopic() {

    final String commandTopic = commandStore.getCommandTopicName();

    KsqlInternalTopicUtils.ensureTopic(commandTopic, ksqlConfig, serviceContext.getTopicClient());

    final String createCmd = "CREATE STREAM " + COMMANDS_STREAM_NAME
        + " (STATEMENT STRING)"
        + " WITH(VALUE_FORMAT='JSON', KAFKA_TOPIC='" + commandTopic + "');";

    final ParsedStatement parsed = ksqlEngine.parse(createCmd).get(0);
    final PreparedStatement<?> prepared = ksqlEngine.prepare(parsed);
    ksqlEngine.execute(ConfiguredStatement.of(prepared, ImmutableMap.of(), ksqlConfig));
  }

  private static KsqlSecurityExtension loadSecurityExtension(final KsqlConfig ksqlConfig) {
    final KsqlSecurityExtension securityExtension = Optional.ofNullable(
        ksqlConfig.getConfiguredInstance(
            KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS,
            KsqlSecurityExtension.class
        )).orElse(new KsqlDefaultSecurityExtension());

    securityExtension.initialize(ksqlConfig);
    return securityExtension;
  }

  private void displayWelcomeMessage() {
    final Console console = System.console();
    if (console == null) {
      return;
    }

    final PrintWriter writer =
        new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    WelcomeMsgUtils.displayWelcomeMessage(80, writer);

    final String version = Version.getVersion();
    final List<URL> listeners = getListeners();
    final String allListeners = listeners.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", "));

    writer.printf("Server %s listening on %s%n", version, allListeners);
    writer.println();
    writer.println("To access the KSQL CLI, run:");
    writer.println("ksql " + listeners.get(0));
    writer.println();

    writer.flush();
  }

  static void maybeCreateProcessingLogStream(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final CommandQueue commandQueue
  ) {
    if (!config.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE)
        || !commandQueue.isEmpty()) {
      return;
    }

    final PreparedStatement<?> statement = ProcessingLogServerUtils
        .processingLogStreamCreateStatement(config, ksqlConfig);
    final Supplier<ConfiguredStatement<?>> configured = () -> ConfiguredStatement.of(
        statement, Collections.emptyMap(), ksqlConfig);

    try {
      ksqlEngine.createSandbox(ksqlEngine.getServiceContext()).execute(configured.get());
    } catch (final KsqlException e) {
      log.warn("Failed to create processing log stream", e);
      return;
    }

    commandQueue.enqueueCommand(configured.get());
  }
}
