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
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.rest.server.computation.CommandIdAssigner;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.rest.server.resources.KsqlExceptionMapper;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.RootDocument;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.server.resources.streaming.WSQueryEndpoint;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.rest.server.state.ServerStateDynamicBinding;
import io.confluent.ksql.rest.util.ClusterTerminator;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.LazyServiceContext;
import io.confluent.ksql.services.ServiceContext;
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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;
import javax.ws.rs.core.Configurable;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class KsqlRestApplication extends Application<KsqlRestConfig> implements Executable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  public static final String COMMANDS_KSQL_TOPIC_NAME = "__KSQL_COMMANDS_TOPIC";
  private static final String COMMANDS_STREAM_NAME = "KSQL_COMMANDS";

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final CommandStore commandStore;
  private final RootDocument rootDocument;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final Thread commandRunnerThread;
  private final VersionCheckerAgent versionCheckerAgent;
  private final ServiceContext serviceContext;
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

    this.commandRunnerThread = new Thread(commandRunner, "CommandRunner");
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

    // WARN: SUPER HACKY CODE BELOW
    //
    // as of jetty 9.4.21.v20190926 the behavior of the error handler
    // changed in a "backwards incompatible" way.
    //
    // it used to have the behavior below where the response type
    // is TEXT_HTML regardless of the input type and there was no
    // special content (it was just handled as an error page) returned
    // so the contents of the message were just an empty string
    //
    // in 9.4.21+ it is a bit more intelligent about responding in
    // the same format as the input. If the request content type
    // accepts JSON, it will respond with more detail error message
    // including the url, status code and message that caused the error
    //
    // while this is in theory a wonderful improvement, it proves to
    // be backwards incompatible with our client :( Specifically, we
    // expect there to be no error contents UNLESS it's properly formatted
    // JSON representation of KsqlErrorMessage.
    //
    // this code below forces an error handler that has the old behavior to
    // maintain backwards compatibility while still addressing the CVEs fixed
    // in more recent jetty versions
    server.setErrorHandler(new ErrorHandler() {
      @SuppressWarnings("deprecation")
      @Override
      protected void generateAcceptableResponse(
          final Request baseRequest,
          final HttpServletRequest request,
          final HttpServletResponse response,
          final int code,
          final String message,
          final String contentType
      ) throws IOException {
        switch (contentType) {
          case "text/html":
          case "text/*":
          case "*/*":
            baseRequest.setHandled(true);
            final Writer writer = getAcceptableWriter(baseRequest, request, response);
            if (writer != null) {
              response.setContentType(MimeTypes.Type.TEXT_HTML.asString());
              handleErrorPage(request, writer, code, message);
            }
            break;
          default:
            break;
        }
      }
    });

    startKsql();
    commandRunnerThread.start();
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
    final String commandTopic = commandStore.getCommandTopicName();
    KsqlInternalTopicUtils.ensureTopic(
        commandTopic,
        ksqlConfig,
        serviceContext.getTopicClient()
    );
    commandStore.start();

    ksqlEngine.getDdlCommandExec().execute(new RegisterTopicCommand(new RegisterTopic(
        QualifiedName.of(COMMANDS_KSQL_TOPIC_NAME),
        false,
        ImmutableMap.of(
            DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("json"),
            DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(commandTopic)
        )
    )));
    ksqlEngine.getDdlCommandExec().execute(new CreateStreamCommand(
        "statementText",
        new CreateStream(
            QualifiedName.of(COMMANDS_STREAM_NAME),
            Collections.singletonList(new TableElement(
                "STATEMENT",
                new PrimitiveType(Type.KsqlType.STRING)
            )),
            false,
            Collections.singletonMap(
                DdlConfig.TOPIC_NAME_PROPERTY,
                new StringLiteral(COMMANDS_KSQL_TOPIC_NAME)
            )
        ),
        serviceContext.getTopicClient()
    ));

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
      commandRunnerThread.join();
    } catch (final Exception e) {
      log.error("Exception while waiting for CommandRunner thread to complete", e);
    }

    try {
      serviceContext.close();
    } catch (final Exception e) {
      log.error("Exception while closing services", e);
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

    // Don't want to buffer rows when streaming JSON in a request to the query resource
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    config.property(ServerProperties.WADL_FEATURE_DISABLE, true);
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
                      serviceContext,
                      commandStore,
                      exec,
                      versionCheckerAgent::updateLastRequestTime,
                      Duration.ofMillis(config.getLong(
                          KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
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
    return buildApplication(restConfig, versionCheckerFactory, maxStatementRetries, serviceContext);
  }

  static KsqlRestApplication buildApplication(
      final KsqlRestConfig restConfig,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final int maxStatementRetries,
      final ServiceContext serviceContext
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
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));

    UdfLoader.newInstance(ksqlConfig, functionRegistry, ksqlInstallDir).load();

    final String commandTopic = KsqlInternalTopicUtils.getTopicName(
        ksqlConfig, KsqlRestConfig.COMMAND_TOPIC_SUFFIX);

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    final CommandStore commandStore = new CommandStore(
        commandTopic,
        restConfig.getCommandConsumerProperties(),
        restConfig.getCommandProducerProperties(),
        new CommandIdAssigner(ksqlEngine.getMetaStore())
    );

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

    final StreamedQueryResource streamedQueryResource = new StreamedQueryResource(
        ksqlConfig,
        ksqlEngine,
        serviceContext,
        statementParser,
        commandStore,
        Duration.ofMillis(
            restConfig.getLong(KsqlRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)),
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime
    );

    final KsqlResource ksqlResource = new KsqlResource(
        ksqlConfig,
        ksqlEngine,
        serviceContext,
        commandStore,
        Duration.ofMillis(restConfig.getLong(DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)),
        versionChecker::updateLastRequestTime
    );

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
        serverState,
        processingLogContext,
        preconditions
    );
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

    try {
      ksqlEngine.createSandbox().execute(statement, ksqlConfig, Collections.emptyMap());
    } catch (final KsqlException e) {
      log.warn("Failed to create processing log stream", e);
      return;
    }

    commandQueue.enqueueCommand(statement, ksqlConfig, Collections.emptyMap());
  }
}
