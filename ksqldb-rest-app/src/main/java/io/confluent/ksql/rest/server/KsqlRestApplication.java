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
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.auth.KsqlAuthorizationProviderHandler;
import io.confluent.ksql.api.impl.DefaultKsqlSecurityContextProvider;
import io.confluent.ksql.api.impl.KsqlSecurityContextProvider;
import io.confluent.ksql.api.impl.MonitoredEndpoints;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.RoutingFilter;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogServerUtils;
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
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.server.HeartbeatAgent.Builder;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.InteractiveStatementExecutor;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.ClusterStatusResource;
import io.confluent.ksql.rest.server.resources.HealthCheckResource;
import io.confluent.ksql.rest.server.resources.HeartbeatResource;
import io.confluent.ksql.rest.server.resources.KsqlConfigurable;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.LagReportingResource;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.ServerMetadataResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.services.ServerInternalKsqlClient;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.rest.util.KsqlUncaughtExceptionHandler;
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
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.util.RetryUtil;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import java.io.Console;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class KsqlRestApplication implements Executable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  private static final SourceName COMMANDS_STREAM_NAME = SourceName.of("KSQL_COMMANDS");

  private final KsqlConfig ksqlConfigNoPort;
  private final KsqlRestConfig restConfig;
  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final CommandStore commandStore;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final VersionCheckerAgent versionCheckerAgent;
  private final ServiceContext serviceContext;
  private final KsqlSecurityContextProvider ksqlSecurityContextProvider;
  private final KsqlSecurityExtension securityExtension;
  private final Optional<AuthenticationPlugin> authenticationPlugin;
  private final ServerState serverState;
  private final ProcessingLogContext processingLogContext;
  private final List<KsqlServerPrecondition> preconditions;
  private final List<KsqlConfigurable> configurables;
  private final Consumer<KsqlConfig> rocksDBConfigSetterHandler;
  private final Optional<HeartbeatAgent> heartbeatAgent;
  private final Optional<LagReportingAgent> lagReportingAgent;
  private final PullQueryExecutor pullQueryExecutor;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final ServerInfoResource serverInfoResource;
  private final Optional<HeartbeatResource> heartbeatResource;
  private final Optional<ClusterStatusResource> clusterStatusResource;
  private final Optional<LagReportingResource> lagReportingResource;
  private final HealthCheckResource healthCheckResource;
  private volatile ServerMetadataResource serverMetadataResource;
  private volatile WSQueryEndpoint wsQueryEndpoint;
  @SuppressWarnings("UnstableApiUsage")
  private volatile ListeningScheduledExecutorService oldApiWebsocketExecutor;
  private final Vertx vertx;
  private Server apiServer = null;
  private final CompletableFuture<Void> terminatedFuture = new CompletableFuture<>();

  // The startup thread that can be interrupted if necessary during shutdown.  This should only
  // happen if startup hangs.
  private volatile Thread startAsyncThread;

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
      final StatusResource statusResource,
      final StreamedQueryResource streamedQueryResource,
      final KsqlResource ksqlResource,
      final VersionCheckerAgent versionCheckerAgent,
      final KsqlSecurityContextProvider ksqlSecurityContextProvider,
      final KsqlSecurityExtension securityExtension,
      final Optional<AuthenticationPlugin> authenticationPlugin,
      final ServerState serverState,
      final ProcessingLogContext processingLogContext,
      final List<KsqlServerPrecondition> preconditions,
      final List<KsqlConfigurable> configurables,
      final Consumer<KsqlConfig> rocksDBConfigSetterHandler,
      final PullQueryExecutor pullQueryExecutor,
      final Optional<HeartbeatAgent> heartbeatAgent,
      final Optional<LagReportingAgent> lagReportingAgent
  ) {
    log.debug("Creating instance of ksqlDB API server");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.ksqlConfigNoPort = requireNonNull(ksqlConfig, "ksqlConfig");
    this.restConfig = requireNonNull(restConfig, "restConfig");
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.commandRunner = requireNonNull(commandRunner, "commandRunner");
    this.statusResource = requireNonNull(statusResource, "statusResource");
    this.streamedQueryResource = requireNonNull(streamedQueryResource, "streamedQueryResource");
    this.ksqlResource = requireNonNull(ksqlResource, "ksqlResource");
    this.commandStore = requireNonNull(commandStore, "commandStore");
    this.serverState = requireNonNull(serverState, "serverState");
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.preconditions = requireNonNull(preconditions, "preconditions");
    this.versionCheckerAgent = requireNonNull(versionCheckerAgent, "versionCheckerAgent");
    this.ksqlSecurityContextProvider = requireNonNull(ksqlSecurityContextProvider,
        "ksqlSecurityContextProvider");
    this.securityExtension = requireNonNull(securityExtension, "securityExtension");
    this.authenticationPlugin = requireNonNull(authenticationPlugin, "authenticationPlugin");
    this.configurables = requireNonNull(configurables, "configurables");
    this.rocksDBConfigSetterHandler =
        requireNonNull(rocksDBConfigSetterHandler, "rocksDBConfigSetterHandler");
    this.pullQueryExecutor = requireNonNull(pullQueryExecutor, "pullQueryExecutor");
    this.heartbeatAgent = requireNonNull(heartbeatAgent, "heartbeatAgent");
    this.lagReportingAgent = requireNonNull(lagReportingAgent, "lagReportingAgent");
    this.vertx = Vertx.vertx(
        new VertxOptions()
            .setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS)
            .setMaxWorkerExecuteTime(Long.MAX_VALUE)
            .setMetricsOptions(setUpHttpMetrics(ksqlConfig)));
    this.vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    this.serverInfoResource = new ServerInfoResource(serviceContext, ksqlConfigNoPort);
    if (heartbeatAgent.isPresent()) {
      this.heartbeatResource = Optional.of(new HeartbeatResource(heartbeatAgent.get()));
      this.clusterStatusResource = Optional.of(new ClusterStatusResource(
          ksqlEngine, heartbeatAgent.get(), lagReportingAgent));
    } else {
      this.heartbeatResource = Optional.empty();
      this.clusterStatusResource = Optional.empty();
    }
    if (lagReportingAgent.isPresent()) {
      this.lagReportingResource = Optional.of(new LagReportingResource(lagReportingAgent.get()));
    } else {
      this.lagReportingResource = Optional.empty();
    }
    this.healthCheckResource = HealthCheckResource.create(
        ksqlResource,
        serviceContext,
        this.restConfig,
        this.ksqlConfigNoPort);
    MetricCollectors.addConfigurableReporter(ksqlConfigNoPort);
    log.debug("ksqlDB API server instance created");
  }

  @Override
  public void startAsync() {
    log.debug("Starting the ksqlDB API server");
    this.serverMetadataResource = ServerMetadataResource.create(serviceContext, ksqlConfigNoPort);
    final StatementParser statementParser = new StatementParser(ksqlEngine);
    final Optional<KsqlAuthorizationValidator> authorizationValidator =
        KsqlAuthorizationValidatorFactory.create(ksqlConfigNoPort, serviceContext);
    final Errors errorHandler = new Errors(restConfig.getConfiguredInstance(
        KsqlRestConfig.KSQL_SERVER_ERROR_MESSAGES,
        ErrorMessages.class
    ));

    final KsqlRestConfig ksqlRestConfig = new KsqlRestConfig(ksqlConfigNoPort.originals());

    oldApiWebsocketExecutor = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(
            ksqlRestConfig.getInt(KsqlRestConfig.KSQL_WEBSOCKETS_NUM_THREADS),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("websockets-query-thread-%d")
                .build()
        )
    );

    this.wsQueryEndpoint = new WSQueryEndpoint(
        ksqlConfigNoPort,
        statementParser,
        ksqlEngine,
        commandStore,
        oldApiWebsocketExecutor,
        versionCheckerAgent::updateLastRequestTime,
        Duration.ofMillis(ksqlRestConfig.getLong(
            KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        authorizationValidator,
        errorHandler,
        pullQueryExecutor
    );

    startAsyncThread = Thread.currentThread();
    try {
      final Endpoints endpoints = new KsqlServerEndpoints(
          ksqlEngine,
          ksqlConfigNoPort,
          pullQueryExecutor,
          ksqlSecurityContextProvider,
          ksqlResource,
          streamedQueryResource,
          serverInfoResource,
          heartbeatResource,
          clusterStatusResource,
          statusResource,
          lagReportingResource,
          healthCheckResource,
          serverMetadataResource,
          wsQueryEndpoint
      );
      apiServer = new Server(vertx, ksqlRestConfig, endpoints, securityExtension,
          authenticationPlugin, serverState);
      apiServer.start();

      final KsqlConfig ksqlConfigWithPort = buildConfigWithPort();
      configurables.forEach(c -> c.configure(ksqlConfigWithPort));

      startKsql(ksqlConfigWithPort);
      final Properties metricsProperties = new Properties();
      metricsProperties.putAll(restConfig.getOriginals());
      versionCheckerAgent.start(KsqlModuleType.SERVER, metricsProperties);

      log.info("ksqlDB API server listening on {}", StringUtils.join(getListeners(), ", "));
      displayWelcomeMessage();
    } catch (AbortApplicationStartException e) {
      log.error("Aborting application start", e);
    } finally {
      startAsyncThread = null;
    }
  }

  @VisibleForTesting
  void startKsql(final KsqlConfig ksqlConfigWithPort) {
    waitForPreconditions();
    initialize(ksqlConfigWithPort);
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

  static final class AbortApplicationStartException extends KsqlServerException {

    private AbortApplicationStartException(final String message) {
      super(message);
    }
  }

  private void checkPreconditions() {
    for (final KsqlServerPrecondition precondition : preconditions) {
      final Optional<KsqlErrorMessage> error = precondition.checkPrecondition(
          restConfig,
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
    try {
      RetryUtil.retryWithBackoff(
          Integer.MAX_VALUE,
          1000,
          30000,
          this::checkPreconditions,
          shuttingDown::get,
          predicates
      );
    } catch (KsqlFailedPrecondition e) {
      log.error("Failed to meet preconditions. Exiting...", e);
    }

    if (shuttingDown.get()) {
      throw new AbortApplicationStartException(
          "Shutting down application during waitForPreconditions");
    }
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

  @SuppressWarnings("checkstyle:NPathComplexity")
  @Override
  public void triggerShutdown() {
    log.debug("ksqlDB triggerShutdown called");
    // First, make sure the server wasn't stuck in startup.  Set the shutdown flag and interrupt the
    // startup thread if it's been hanging.
    shuttingDown.set(true);
    if (startAsyncThread != null) {
      startAsyncThread.interrupt();
    }

    try {
      streamedQueryResource.closeMetrics();
    } catch (final Exception e) {
      log.error("Exception while waiting for pull query metrics to close", e);
    }
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
    }

    if (oldApiWebsocketExecutor != null) {
      oldApiWebsocketExecutor.shutdown();
    }

    shutdownAdditionalAgents();

    log.debug("ksqlDB triggerShutdown complete");

    terminatedFuture.complete(null);
  }

  @Override
  public void awaitTerminated() throws InterruptedException {
    try {
      terminatedFuture.get();
    } catch (ExecutionException e) {
      log.error("Exception in awaitTerminated", e);
      throw new KsqlException(e.getCause());
    }
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

  // Current tests require URIs as URLs, even though they're not URLs
  List<URL> getListeners() {
    return apiServer.getListeners().stream().map(uri -> {
      try {
        return uri.toURL();
      } catch (MalformedURLException e) {
        throw new KsqlException(e);
      }
    }).collect(Collectors.toList());
  }

  Optional<URL> getInternalListener() {
    return apiServer.getInternalListener().map(uri -> {
      try {
        return uri.toURL();
      } catch (MalformedURLException e) {
        throw new KsqlException(e);
      }
    });
  }

  public static KsqlRestApplication buildApplication(final KsqlRestConfig restConfig) {
    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());
    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
        new KsqlSchemaRegistryClientFactory(ksqlConfig, Collections.emptyMap())::get;
    final ServiceContext serviceContext = new LazyServiceContext(() ->
        RestServiceContextFactory.create(ksqlConfig, Optional.empty(),
            schemaRegistryClientFactory));

    return buildApplication(
        "",
        restConfig,
        KsqlVersionCheckerAgent::new,
        Integer.MAX_VALUE,
        serviceContext,
        schemaRegistryClientFactory
    );
  }

  @SuppressWarnings("checkstyle:MethodLength")
  static KsqlRestApplication buildApplication(
      final String metricsPrefix,
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final int maxStatementRetries,
      final ServiceContext serviceContext,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory) {
    final String ksqlInstallDir = restConfig.getString(KsqlRestConfig.INSTALL_DIR_CONFIG);

    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());

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

    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final CommandStore commandStore = CommandStore.Factory.create(
        commandTopicName,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        ksqlConfig.addConfluentMetricsContextConfigsKafka(
            restConfig.getCommandConsumerProperties(),
            serviceId),
        ksqlConfig.addConfluentMetricsContextConfigsKafka(
            restConfig.getCommandProducerProperties(),
            serviceId)
    );

    final InteractiveStatementExecutor statementExecutor =
        new InteractiveStatementExecutor(serviceContext, ksqlEngine, specificQueryIdGenerator);

    final StatusResource statusResource = new StatusResource(statementExecutor);
    final VersionCheckerAgent versionChecker
        = versionCheckerFactory.apply(ksqlEngine::hasActiveQueries);

    final ServerState serverState = new ServerState();

    final KsqlSecurityExtension securityExtension = loadSecurityExtension(ksqlConfig);

    final KsqlSecurityContextProvider ksqlSecurityContextProvider =
        new DefaultKsqlSecurityContextProvider(
            securityExtension,
            RestServiceContextFactory::create,
            RestServiceContextFactory::create, ksqlConfig, schemaRegistryClientFactory);

    final Optional<AuthenticationPlugin> securityHandlerPlugin = loadAuthenticationPlugin(
        restConfig);

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
        ksqlEngine, routingFilterFactory, ksqlConfig);

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
        statusResource,
        streamedQueryResource,
        ksqlResource,
        versionChecker,
        ksqlSecurityContextProvider,
        securityExtension,
        securityHandlerPlugin,
        serverState,
        processingLogContext,
        preconditions,
        configurables,
        rocksDBConfigSetterHandler,
        pullQueryExecutor,
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
      final KsqlConfig configWithApplicationServer,
      final Optional<HeartbeatAgent> heartbeatAgent,
      final Optional<LagReportingAgent> lagReportingAgent) {
    return (routingOptions, hosts, active, applicationQueryId, storeName, partition) -> {
      final ImmutableList.Builder<RoutingFilter> filterBuilder = ImmutableList.builder();

      // If the lookup is for a forwarded request, apply only MaxLagFilter for localhost
      if (routingOptions.skipForwardRequest()) {
        MaximumLagFilter.create(lagReportingAgent, routingOptions, hosts, applicationQueryId,
                                storeName, partition)
            .map(filterBuilder::add);
      } else {
        if (!configWithApplicationServer.getBoolean(
            KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS)) {
          filterBuilder.add(new ActiveHostFilter(active));
        }
        filterBuilder.add(new LivenessFilter(heartbeatAgent));
        MaximumLagFilter.create(lagReportingAgent, routingOptions, hosts, applicationQueryId,
                                storeName, partition)
            .map(filterBuilder::add);
      }
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

  private static Optional<AuthenticationPlugin> loadAuthenticationPlugin(
      final KsqlRestConfig ksqlRestConfig) {
    final Optional<AuthenticationPlugin> authenticationPlugin = Optional.ofNullable(
        ksqlRestConfig.getConfiguredInstance(
            KsqlRestConfig.KSQL_AUTHENTICATION_PLUGIN_CLASS,
            AuthenticationPlugin.class
        ));
    authenticationPlugin.ifPresent(securityHandlerPlugin ->
        securityHandlerPlugin.configure(ksqlRestConfig.originals())
    );
    return authenticationPlugin;
  }

  private void displayWelcomeMessage() {
    
    final Console console = System.console();
    if (console == null) {
      return;
    }

    final PrintWriter writer =
        new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    WelcomeMsgUtils.displayWelcomeMessage(80, writer);

    final String version = AppInfo.getVersion();
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
      
      final String processingLogStreamName =
          processingLogConfig.getString(ProcessingLogConfig.STREAM_NAME);
      if (!processingLogStreamExists(
          internalClient,
          serverEndpoint,
          processingLogStreamName
      )) {
        final RestResponse<KsqlEntityList> response = internalClient.makeKsqlRequest(
            serverEndpoint,
            ProcessingLogServerUtils.processingLogStreamCreateStatement(
                processingLogConfig,
                ksqlConfig
            ),
            ImmutableMap.of());

        if (response.isSuccessful()) {
          log.info("Successfully created processing log stream.");
        }
      }
    } catch (final Exception e) {
      log.error(
          "Error while sending processing log CreateStream request to KsqlResource: ", e);
    }
  }

  private static boolean processingLogStreamExists(
      final SimpleKsqlClient internalClient,
      final URI serverEndpoint,
      final String processingLogStreamName
  ) {
    final RestResponse<KsqlEntityList> listStreamsResponse = internalClient.makeKsqlRequest(
        serverEndpoint,
        "list streams;",
            ImmutableMap.of());

    final List<SourceInfo.Stream> streams =
        ((StreamsList) listStreamsResponse.getResponse().get(0)).getStreams();

    return streams
        .stream()
        .anyMatch(stream -> stream.getName().equals(processingLogStreamName));
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

  private static DropwizardMetricsOptions setUpHttpMetrics(final KsqlConfig ksqlConfig) {
    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final DropwizardMetricsOptions metricsOptions = new DropwizardMetricsOptions()
        .setJmxEnabled(true).setBaseName("_confluent-ksql-" + serviceId)
        .setJmxDomain("io.confluent.ksql.metrics");
    final List<Match> matches = MonitoredEndpoints.getMonitoredEndpoints();
    for (Match match : matches) {
      metricsOptions.addMonitoredHttpServerUri(match);
    }
    return metricsOptions;
  }

  private static KsqlRestConfig injectPathsWithoutAuthentication(final KsqlRestConfig restConfig) {
    final Set<String> authenticationSkipPaths = new HashSet<>(
        restConfig.getList(KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG)
    );

    authenticationSkipPaths.addAll(KsqlAuthorizationProviderHandler.PATHS_WITHOUT_AUTHORIZATION);

    final Map<String, Object> restConfigs = restConfig.getOriginals();

    // REST paths that are public and do not require authentication
    restConfigs.put(KsqlRestConfig.AUTHENTICATION_SKIP_PATHS_CONFIG,
        Joiner.on(",").join(authenticationSkipPaths));

    return new KsqlRestConfig(restConfigs);
  }

}
