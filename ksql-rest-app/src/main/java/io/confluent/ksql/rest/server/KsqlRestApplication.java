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
import static io.confluent.rest.RestConfig.LISTENERS_CONFIG;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.api.plugin.KsqlServerEndpoints;
import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.query.id.SpecificQueryIdGenerator;
import io.confluent.ksql.rest.ErrorMessages;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.HeartbeatAgent.Builder;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.InteractiveStatementExecutor;
import io.confluent.ksql.rest.server.context.KsqlSecurityContextBinder;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.execution.PullQueryExecutorDelegate;
import io.confluent.ksql.rest.server.filters.KsqlAuthorizationFilter;
import io.confluent.ksql.rest.server.resources.ClusterStatusResource;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.HeartbeatResource;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.rest.server.resources.KsqlExceptionMapper;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.LagReportingResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.ServerMetadataResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClient;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.server.state.ServerStateDynamicBinding;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.rest.util.KsqlUncaughtExceptionHandler;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.rest.util.RocksDBConfigSetterHandler;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlAuthorizationValidatorFactory;
import io.confluent.ksql.security.KsqlDefaultSecurityExtension;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.LazyServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.RetryUtil;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.RestConfig;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import io.vertx.core.Vertx;
import java.io.Console;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;
import javax.ws.rs.core.Configurable;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.LogManager;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class KsqlRestApplication extends ExecutableApplication<KsqlRestConfig> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  private static final SourceName COMMANDS_STREAM_NAME = SourceName.of("KSQL_COMMANDS");

  private final KsqlConfig ksqlConfigNoPort;
  private final KsqlRestConfig restConfig;
  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final CommandStore commandStore;
  private final RootDocument rootDocument;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final VersionCheckerAgent versionCheckerAgent;
  private final ServiceContext serviceContext;
  private final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory;
  private final KsqlSecurityExtension securityExtension;
  private final ServerState serverState;
  private final ProcessingLogContext processingLogContext;
  private final List<KsqlServerPrecondition> preconditions;
  private final List<KsqlConfigurable> configurables;
  private final Consumer<KsqlConfig> rocksDBConfigSetterHandler;
  private final Optional<HeartbeatAgent> heartbeatAgent;
  private final Optional<LagReportingAgent> lagReportingAgent;
  private final RoutingFilterFactory routingFilterFactory;
  private final PullQueryExecutor pullQueryExecutor;

  // We embed this in here for now
  private Vertx vertx = null;
  private Server apiServer = null;

  public static SourceName getCommandsStreamName() {
    return COMMANDS_STREAM_NAME;
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @VisibleForTesting
  KsqlRestApplication(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig,
      final KsqlRestConfig restConfig,
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
      final List<KsqlServerPrecondition> preconditions,
      final List<KsqlConfigurable> configurables,
      final Consumer<KsqlConfig> rocksDBConfigSetterHandler,
      final Optional<HeartbeatAgent> heartbeatAgent,
      final Optional<LagReportingAgent> lagReportingAgent
  ) {
    super(restConfig);

    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfigNoPort = requireNonNull(ksqlConfig, "ksqlConfig");
    this.restConfig = requireNonNull(restConfig, "restConfig");
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandRunner = requireNonNull(commandRunner, "commandRunner");
    this.rootDocument = requireNonNull(rootDocument, "rootDocument");
    this.statusResource = requireNonNull(statusResource, "statusResource");
    this.streamedQueryResource = requireNonNull(streamedQueryResource, "streamedQueryResource");
    this.ksqlResource = requireNonNull(ksqlResource, "ksqlResource");
    this.commandStore = requireNonNull(commandStore, "commandStore");
    this.serverState = requireNonNull(serverState, "serverState");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.preconditions = requireNonNull(preconditions, "preconditions");
    this.versionCheckerAgent = requireNonNull(versionCheckerAgent, "versionCheckerAgent");
    this.serviceContextBinderFactory =
        requireNonNull(serviceContextBinderFactory, "serviceContextBinderFactory");
    this.securityExtension = requireNonNull(securityExtension, "securityExtension");
    this.configurables = requireNonNull(configurables, "configurables");
    this.rocksDBConfigSetterHandler =
        requireNonNull(rocksDBConfigSetterHandler, "rocksDBConfigSetterHandler");
    this.heartbeatAgent = requireNonNull(heartbeatAgent, "heartbeatAgent");
    this.lagReportingAgent = requireNonNull(lagReportingAgent, "lagReportingAgent");

    this.routingFilterFactory = initializeRoutingFilterFactory(
        ksqlConfigNoPort, heartbeatAgent, lagReportingAgent);
    this.pullQueryExecutor = new PullQueryExecutor(
        ksqlEngine, heartbeatAgent, routingFilterFactory);
  }

  @Override
  public void setupResources(final Configurable<?> config, final KsqlRestConfig appConfig) {
    config.register(rootDocument);
    config.register(new ServerInfoResource(serviceContext, ksqlConfigNoPort));
    config.register(ServerMetadataResource.create(serviceContext, ksqlConfigNoPort));
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(HealthCheckResource.create(ksqlResource, serviceContext, this.config));
    if (heartbeatAgent.isPresent()) {
      config.register(new HeartbeatResource(heartbeatAgent.get()));
      config.register(new ClusterStatusResource(
          ksqlEngine, heartbeatAgent.get(), lagReportingAgent));
    }
    if (lagReportingAgent.isPresent()) {
      config.register(new LagReportingResource(lagReportingAgent.get()));
    }
    config.register(new KsqlExceptionMapper());
    config.register(new ServerStateDynamicBinding(serverState));
  }

  @Override
  public void startAsync() {
    log.info("KSQL RESTful API listening on {}", StringUtils.join(getListeners(), ", "));
    final KsqlConfig ksqlConfigWithPort = buildConfigWithPort();
    configurables.forEach(c -> c.configure(ksqlConfigWithPort));
    startKsql(ksqlConfigWithPort);
    final Properties metricsProperties = new Properties();
    metricsProperties.putAll(getConfiguration().getOriginals());
    if (versionCheckerAgent != null) {
      versionCheckerAgent.start(KsqlModuleType.SERVER, metricsProperties);
    }
    if (ksqlConfigWithPort.getBoolean(KsqlConfig.KSQL_NEW_API_ENABLED)) {
      startApiServer(ksqlConfigWithPort);
    }
    displayWelcomeMessage();
  }

  @VisibleForTesting
  void startKsql(final KsqlConfig ksqlConfigWithPort) {
    waitForPreconditions();
    initialize(ksqlConfigWithPort);
  }

  void startApiServer(final KsqlConfig ksqlConfigWithPort) {
    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));
    final Endpoints endpoints = new KsqlServerEndpoints(
        ksqlEngine,
        ksqlConfigWithPort,
        securityExtension,
        RestServiceContextFactory::create,
        new PullQueryExecutorDelegate(pullQueryExecutor)
    );
    final ApiServerConfig config = new ApiServerConfig(ksqlConfigWithPort.originals());
    apiServer = new Server(vertx, config, endpoints);
    apiServer.start();
    log.info("KSQL New API Server started");
  }

  @VisibleForTesting
  KsqlEngine getEngine() {
    return ksqlEngine;
  }

  private static final class KsqlFailedPrecondition extends RuntimeException {

    private KsqlFailedPrecondition(final String message) {
      super(message);
    }
  }

  private void checkPreconditions() {
    for (final KsqlServerPrecondition precondition : preconditions) {
      final Optional<KsqlErrorMessage> error = precondition.checkPrecondition(
          config,
          serviceContext
      );
      if (error.isPresent()) {
        serverState.setInitializingReason(error.get());
        throw new KsqlFailedPrecondition(error.get().toString());
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

  private void initialize(final KsqlConfig configWithApplicationServer) {
    rocksDBConfigSetterHandler.accept(ksqlConfigNoPort);

    registerCommandTopic();

    commandStore.start();

    ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        serviceContext.getTopicClient(),
        processingLogContext.getConfig(),
        ksqlConfigNoPort
    );
    commandRunner.processPriorCommands();
    commandRunner.start();
    maybeCreateProcessingLogStream(
        processingLogContext.getConfig(),
        ksqlConfigNoPort,
        restConfig,
        ksqlResource,
        serviceContext
    );

    if (heartbeatAgent.isPresent()) {
      heartbeatAgent.get().setLocalAddress((String)configWithApplicationServer
          .getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG));
      heartbeatAgent.get().startAgent();
    }
    if (lagReportingAgent.isPresent()) {
      lagReportingAgent.get().setLocalAddress((String)configWithApplicationServer
          .getKsqlStreamConfigProps().get(StreamsConfig.APPLICATION_SERVER_CONFIG));
      lagReportingAgent.get().startAgent();
    }

    serverState.setReady();
  }

  @Override
  public void triggerShutdown() {
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

    if (apiServer != null) {
      apiServer.stop();
      apiServer = null;
    }
    if (vertx != null) {
      vertx.close();
      vertx = null;
    }

    shutdownAdditionalAgents();
  }

  private void shutdownAdditionalAgents() {
    if (heartbeatAgent.isPresent()) {
      try {
        heartbeatAgent.get().stopAgent();
      } catch (final Exception e) {
        log.error("Exception while shutting down HeartbeatAgent", e);
      }
    }
    if (lagReportingAgent.isPresent()) {
      try {
        lagReportingAgent.get().stopAgent();
      } catch (final Exception e) {
        log.error("Exception while shutting down LagReportingAgent", e);
      }
    }
  }

  @Override
  public void onShutdown() {
    triggerShutdown();
  }

  List<URL> getListeners() {
    final Function<URL, Set<Integer>> resolvePort = url ->
        Arrays.stream(server.getConnectors())
            .filter(connector -> connector instanceof ServerConnector)
            .map(ServerConnector.class::cast)
            .filter(connector -> {
              final String connectorProtocol = connector.getProtocols().stream()
                  .map(String::toLowerCase)
                  .anyMatch(p -> p.equals("ssl")) ? "https" : "http";

              return connectorProtocol.equalsIgnoreCase(url.getProtocol());
            })
            .map(ServerConnector::getLocalPort)
            .collect(Collectors.toSet());

    final Function<String, Stream<URL>> resolveUrl = listener -> {
      try {
        final URL url = new URL(listener);
        if (url.getPort() != 0) {
          return Stream.of(url);
        }

        // Need to resolve port using actual listeners:
        return resolvePort.apply(url).stream()
            .map(port -> {
              try {
                return new URL(url.getProtocol(), url.getHost(), port, url.getFile());
              } catch (final MalformedURLException e) {
                throw new KsqlServerException("Malformed URL specified in '"
                    + LISTENERS_CONFIG + "' config: " + listener, e);
              }
            });
      } catch (final MalformedURLException e) {
        throw new KsqlServerException("Malformed URL specified in '"
            + LISTENERS_CONFIG + "' config: " + listener, e);
      }
    };

    return restConfig.getList(LISTENERS_CONFIG).stream()
        .flatMap(resolveUrl)
        .distinct()
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
    config.register(serviceContextBinderFactory.apply(ksqlConfigNoPort, securityExtension));

    // Don't want to buffer rows when streaming JSON in a request to the query resource
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    config.property(ServerProperties.WADL_FEATURE_DISABLE, true);

    // Controls the access to all REST endpoints
    securityExtension.getAuthorizationProvider().ifPresent(
        ac -> config.register(new KsqlAuthorizationFilter(ac))
    );
  }

  @SuppressWarnings("UnstableApiUsage")
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
      final Optional<KsqlAuthorizationValidator> authorizationValidator =
          KsqlAuthorizationValidatorFactory.create(ksqlConfigNoPort, serviceContext);
      final Errors errorHandler = new Errors(restConfig.getConfiguredInstance(
          KsqlRestConfig.KSQL_SERVER_ERROR_MESSAGES,
          ErrorMessages.class
      ));

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
                      buildConfigWithPort(),
                      JsonMapper.INSTANCE.mapper,
                      statementParser,
                      ksqlEngine,
                      commandStore,
                      exec,
                      versionCheckerAgent::updateLastRequestTime,
                      Duration.ofMillis(config.getLong(
                          KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
                      authorizationValidator,
                      errorHandler,
                      securityExtension,
                      serverState,
                      serviceContext.getSchemaRegistryClientFactory(),
                      pullQueryExecutor
                  );
                }
              })
              .build()
      );
    } catch (final DeploymentException e) {
      log.error("Unable to create websockets endpoint", e);
    }
  }

  static KsqlRestApplication buildApplication(
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory
  ) {
    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());
    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
        new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get;
    final ServiceContext serviceContext = new LazyServiceContext(() ->
        RestServiceContextFactory.create(ksqlConfig, Optional.empty(),
            schemaRegistryClientFactory));

    return buildApplication(
        "",
        restConfig,
        versionCheckerFactory,
        Integer.MAX_VALUE,
        serviceContext,
        (config, securityExtension) ->
            new KsqlSecurityContextBinder(config, securityExtension, schemaRegistryClientFactory));
  }

  static KsqlRestApplication buildApplication(
      final String metricsPrefix,
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final int maxStatementRetries,
      final ServiceContext serviceContext,
      final BiFunction<KsqlConfig, KsqlSecurityExtension, Binder> serviceContextBinderFactory) {
    final String ksqlInstallDir = restConfig.getString(KsqlRestConfig.INSTALL_DIR_CONFIG);

    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());

    MetricCollectors.addConfigurableReporter(ksqlConfig);

    final ProcessingLogConfig processingLogConfig
        = new ProcessingLogConfig(restConfig.getOriginals());
    final ProcessingLogContext processingLogContext
        = ProcessingLogContext.create(processingLogConfig);

    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();

    if (restConfig.getBoolean(KsqlRestConfig.KSQL_SERVER_ENABLE_UNCAUGHT_EXCEPTION_HANDLER)) {
      Thread.setDefaultUncaughtExceptionHandler(
          new KsqlUncaughtExceptionHandler(LogManager::shutdown));
    }

    final SpecificQueryIdGenerator specificQueryIdGenerator =
        new SpecificQueryIdGenerator();

    final KsqlEngine ksqlEngine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        ServiceInfo.create(ksqlConfig, metricsPrefix),
        specificQueryIdGenerator
    );

    UserFunctionLoader.newInstance(ksqlConfig, functionRegistry, ksqlInstallDir).load();

    final String commandTopicName = ReservedInternalTopics.commandTopic(ksqlConfig);

    final CommandStore commandStore = CommandStore.Factory.create(
        commandTopicName,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        restConfig.getCommandConsumerProperties(),
        restConfig.getCommandProducerProperties()
    );

    final InteractiveStatementExecutor statementExecutor =
        new InteractiveStatementExecutor(serviceContext, ksqlEngine, specificQueryIdGenerator);

    final RootDocument rootDocument = new RootDocument();

    final StatusResource statusResource = new StatusResource(statementExecutor);
    final VersionCheckerAgent versionChecker
        = versionCheckerFactory.apply(ksqlEngine::hasActiveQueries);

    final ServerState serverState = new ServerState();

    final KsqlSecurityExtension securityExtension = loadSecurityExtension(ksqlConfig);

    final Optional<KsqlAuthorizationValidator> authorizationValidator =
        KsqlAuthorizationValidatorFactory.create(ksqlConfig, serviceContext);

    final Errors errorHandler = new Errors(restConfig.getConfiguredInstance(
        KsqlRestConfig.KSQL_SERVER_ERROR_MESSAGES,
        ErrorMessages.class
    ));

    final Optional<LagReportingAgent> lagReportingAgent =
        initializeLagReportingAgent(restConfig, ksqlEngine, serviceContext);
    final Optional<HeartbeatAgent> heartbeatAgent =
        initializeHeartbeatAgent(restConfig, ksqlEngine, serviceContext, lagReportingAgent);
    final RoutingFilterFactory routingFilterFactory = initializeRoutingFilterFactory(ksqlConfig,
        heartbeatAgent, lagReportingAgent);

    final PullQueryExecutor pullQueryExecutor = new PullQueryExecutor(
        ksqlEngine, heartbeatAgent, routingFilterFactory);
    final StreamedQueryResource streamedQueryResource = new StreamedQueryResource(
        ksqlEngine,
        commandStore,
        Duration.ofMillis(
            restConfig.getLong(KsqlRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)),
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime,
        authorizationValidator,
        errorHandler,
        pullQueryExecutor
    );

    final KsqlResource ksqlResource = new KsqlResource(
        ksqlEngine,
        commandStore,
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime,
        authorizationValidator,
        errorHandler
    );

    final List<String> managedTopics = new LinkedList<>();
    managedTopics.add(commandTopicName);
    if (processingLogConfig.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)) {
      managedTopics.add(ProcessingLogServerUtils.getTopicName(processingLogConfig, ksqlConfig));
    }

    final CommandRunner commandRunner = new CommandRunner(
        statementExecutor,
        commandStore,
        maxStatementRetries,
        new ClusterTerminator(ksqlEngine, serviceContext, managedTopics),
        serverState,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        Duration.ofMillis(restConfig.getLong(
            KsqlRestConfig.KSQL_COMMAND_RUNNER_BLOCKED_THRESHHOLD_ERROR_MS)),
        metricsPrefix
    );

    final List<KsqlServerPrecondition> preconditions = restConfig.getConfiguredInstances(
        KsqlRestConfig.KSQL_SERVER_PRECONDITIONS,
        KsqlServerPrecondition.class
    );

    final List<KsqlConfigurable> configurables = ImmutableList.of(
        ksqlResource,
        streamedQueryResource,
        statementExecutor
    );

    final Consumer<KsqlConfig> rocksDBConfigSetterHandler =
        RocksDBConfigSetterHandler::maybeConfigureRocksDBConfigSetter;

    return new KsqlRestApplication(
        serviceContext,
        ksqlEngine,
        ksqlConfig,
        injectPathsWithoutAuthentication(restConfig),
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
        preconditions,
        configurables,
        rocksDBConfigSetterHandler,
        heartbeatAgent,
        lagReportingAgent
    );
  }

  private static Optional<HeartbeatAgent> initializeHeartbeatAgent(
      final KsqlRestConfig restConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final Optional<LagReportingAgent> lagReportingAgent
  ) {
    if (restConfig.getBoolean(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG)) {
      final Builder builder = HeartbeatAgent.builder();
      builder
          .heartbeatSendInterval(restConfig.getLong(
              KsqlRestConfig.KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG))
          .heartbeatCheckInterval(restConfig.getLong(
              KsqlRestConfig.KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG))
          .heartbeatMissedThreshold(restConfig.getLong(
              KsqlRestConfig.KSQL_HEARTBEAT_MISSED_THRESHOLD_CONFIG))
          .heartbeatWindow(restConfig.getLong(
              KsqlRestConfig.KSQL_HEARTBEAT_WINDOW_MS_CONFIG))
          .discoverClusterInterval(restConfig.getLong(
              KsqlRestConfig.KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG))
          .threadPoolSize(restConfig.getInt(
              KsqlRestConfig.KSQL_HEARTBEAT_THREAD_POOL_SIZE_CONFIG));

      if (lagReportingAgent.isPresent()) {
        builder.addHostStatusListener(lagReportingAgent.get());
      }

      return Optional.of(builder.build(ksqlEngine, serviceContext));
    }
    return Optional.empty();
  }

  private static Optional<LagReportingAgent> initializeLagReportingAgent(
      final KsqlRestConfig restConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext
  ) {
    if (restConfig.getBoolean(KsqlRestConfig.KSQL_LAG_REPORTING_ENABLE_CONFIG)
        && restConfig.getBoolean(KsqlRestConfig.KSQL_HEARTBEAT_ENABLE_CONFIG)) {
      final LagReportingAgent.Builder builder = LagReportingAgent.builder();
      return Optional.of(
          builder
              .lagSendIntervalMs(restConfig.getLong(
                  KsqlRestConfig.KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG))
              .build(ksqlEngine, serviceContext));
    }
    return Optional.empty();
  }

  private static RoutingFilterFactory initializeRoutingFilterFactory(
      final KsqlConfig ksqlConfig,
      final Optional<HeartbeatAgent> heartbeatAgent,
      final Optional<LagReportingAgent> lagReportingAgent) {
    return (routingOptions, hosts, active, applicationQueryId, storeName, partition) -> {
      final ImmutableList.Builder<RoutingFilter> filterBuilder = ImmutableList.builder();
      if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS)) {
        filterBuilder.add(new ActiveHostFilter(active));
      }
      filterBuilder.add(new LivenessFilter(heartbeatAgent));
      MaximumLagFilter.create(lagReportingAgent, routingOptions, hosts, applicationQueryId,
          storeName, partition)
          .map(filterBuilder::add);
      return new RoutingFilters(filterBuilder.build());
    };
  }

  private void registerCommandTopic() {

    final String commandTopic = commandStore.getCommandTopicName();

    KsqlInternalTopicUtils.ensureTopic(
        commandTopic,
        ksqlConfigNoPort,
        serviceContext.getTopicClient()
    );

    final String createCmd = "CREATE STREAM " + COMMANDS_STREAM_NAME
        + " (STATEMENT STRING)"
        + " WITH(VALUE_FORMAT='JSON', KAFKA_TOPIC='" + commandTopic + "');";

    final ParsedStatement parsed = ksqlEngine.parse(createCmd).get(0);
    final PreparedStatement<?> prepared = ksqlEngine.prepare(parsed);
    ksqlEngine.execute(
        serviceContext,
        ConfiguredStatement.of(prepared, ImmutableMap.of(), ksqlConfigNoPort)
    );
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

  private static void maybeCreateProcessingLogStream(
      final ProcessingLogConfig processingLogConfig,
      final KsqlConfig ksqlConfig,
      final KsqlRestConfig restConfig,
      final KsqlResource ksqlResource,
      final ServiceContext serviceContext
  ) {
    if (!processingLogConfig.getBoolean(ProcessingLogConfig.STREAM_AUTO_CREATE)) {
      return;
    }

    try {
      final SimpleKsqlClient internalClient =
          new ServerInternalKsqlClient(ksqlResource, new KsqlSecurityContext(
              Optional.empty(), serviceContext));
      final URI serverEndpoint = ServerUtil.getServerAddress(restConfig);

      final RestResponse<KsqlEntityList> response = internalClient.makeKsqlRequest(
          serverEndpoint,
          ProcessingLogServerUtils.processingLogStreamCreateStatement(
              processingLogConfig,
              ksqlConfig
          )
      );

      if (response.isSuccessful()) {
        log.info("Successfully created processing log stream.");
      }
    } catch (final Exception e) {
      log.error(
          "Error while sending processing log CreateStream request to KsqlResource: ", e);
    }
  }

  /**
   * Build a complete config with the KS IQ application.server set.
   *
   * @return true server config.
   */
  @VisibleForTesting
  KsqlConfig buildConfigWithPort() {
    final Map<String, Object> props = ksqlConfigNoPort.originals();

    // Wire up KS IQ so that pull queries work across KSQL nodes:
    props.put(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_SERVER_CONFIG,
        restConfig.getInterNodeListener(this::resolvePort).toString()
    );

    return new KsqlConfig(props);
  }

  private int resolvePort(final URL listener) {
    return getListeners().stream()
        .filter(l ->
            l.getProtocol().equals(listener.getProtocol())
                && l.getHost().equals(listener.getHost())
        )
        .map(URL::getPort)
        .findFirst()
        .orElseThrow(() ->
            new IllegalStateException("Failed resolve port for listener: " + listener));
  }

  private static KsqlRestConfig injectPathsWithoutAuthentication(final KsqlRestConfig restConfig) {
    final Set<String> authenticationSkipPaths = new HashSet<>(
        restConfig.getList(RestConfig.AUTHENTICATION_SKIP_PATHS)
    );

    authenticationSkipPaths.addAll(KsqlAuthorizationFilter.getPathsWithoutAuthorization());

    final Map<String, Object> restConfigs = restConfig.getOriginals();

    // REST paths that are public and do not require authentication
    restConfigs.put(RestConfig.AUTHENTICATION_SKIP_PATHS,
        Joiner.on(",").join(authenticationSkipPaths));

    return new KsqlRestConfig(restConfigs);
  }
}
