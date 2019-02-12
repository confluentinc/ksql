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

import static io.confluent.ksql.rest.server.KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG;

import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.rest.entity.ServerInfo;
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
import io.confluent.ksql.rest.util.ProcessingLogConfig;
import io.confluent.ksql.rest.util.ProcessingLogServerUtils;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;
import javax.ws.rs.core.Configurable;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.eclipse.jetty.server.ServerConnector;
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
  private final CommandQueue commandQueue;
  private final RootDocument rootDocument;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final ServerInfo serverInfo;
  private final Thread commandRunnerThread;
  private final VersionCheckerAgent versionCheckerAgent;
  private final ServiceContext serviceContext;

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
      final CommandQueue commandQueue,
      final RootDocument rootDocument,
      final StatusResource statusResource,
      final StreamedQueryResource streamedQueryResource,
      final KsqlResource ksqlResource,
      final VersionCheckerAgent versionCheckerAgent
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
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");

    this.versionCheckerAgent =
        Objects.requireNonNull(versionCheckerAgent, "versionCheckerAgent");
    this.serverInfo = new ServerInfo(
        Version.getVersion(),
        getKafkaClusterId(serviceContext),
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));

    this.commandRunnerThread = new Thread(commandRunner, "CommandRunner");
  }

  @Override
  public void setupResources(final Configurable<?> config, final KsqlRestConfig appConfig) {
    config.register(rootDocument);
    config.register(new ServerInfoResource(serverInfo));
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(new KsqlExceptionMapper());
  }

  @Override
  public void start() throws Exception {
    commandRunnerThread.start();
    super.start();
    final Properties metricsProperties = new Properties();
    metricsProperties.putAll(getConfiguration().getOriginals());
    if (versionCheckerAgent != null) {
      versionCheckerAgent.start(KsqlModuleType.SERVER, metricsProperties);
    }

    displayWelcomeMessage();
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
                      commandQueue,
                      exec,
                      versionCheckerAgent::updateLastRequestTime,
                      Duration.ofMillis(config.getLong(
                          KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG))
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

    final String ksqlInstallDir = restConfig.getString(KsqlRestConfig.INSTALL_DIR_CONFIG);

    final KsqlConfig ksqlConfig = new KsqlConfig(restConfig.getKsqlConfigProperties());

    final ServiceContext serviceContext = DefaultServiceContext.create(ksqlConfig);

    final KsqlEngine ksqlEngine = new KsqlEngine(
        serviceContext, ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));

    UdfLoader.newInstance(ksqlConfig, ksqlEngine.getFunctionRegistry(), ksqlInstallDir).load();

    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    ensureCommandTopic(restConfig, serviceContext.getTopicClient(), commandTopic);

    final Map<String, Expression> commandTopicProperties = new HashMap<>();
    commandTopicProperties.put(
        DdlConfig.VALUE_FORMAT_PROPERTY,
        new StringLiteral("json")
    );
    commandTopicProperties.put(
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY,
        new StringLiteral(commandTopic)
    );

    ksqlEngine.getDdlCommandExec().execute(new RegisterTopicCommand(new RegisterTopic(
        QualifiedName.of(COMMANDS_KSQL_TOPIC_NAME),
        false,
        commandTopicProperties
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
        serviceContext.getTopicClient(),
        true
    ));

    final StatementParser statementParser = new StatementParser(ksqlEngine);

    final CommandStore commandStore = new CommandStore(
        commandTopic,
        restConfig.getCommandConsumerProperties(),
        restConfig.getCommandProducerProperties(),
        new CommandIdAssigner(ksqlEngine.getMetaStore()));

    final StatementExecutor statementExecutor = new StatementExecutor(
        ksqlConfig,
        ksqlEngine,
        statementParser
    );

    final CommandRunner commandRunner = new CommandRunner(
        statementExecutor,
        commandStore,
        ksqlConfig,
        ksqlEngine,
        maxStatementRetries,
        serviceContext
    );

    final RootDocument rootDocument = new RootDocument();

    final StatusResource statusResource = new StatusResource(statementExecutor);
    final VersionCheckerAgent versionChecker = versionCheckerFactory
        .apply(ksqlEngine::hasActiveQueries);

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

    final ProcessingLogConfig processingLogConfig =
        new ProcessingLogConfig(restConfig.getOriginals());
    ProcessingLogServerUtils.maybeCreateProcessingLogTopic(
        serviceContext.getTopicClient(),
        processingLogConfig,
        ksqlConfig);
    maybeCreateProcessingLogStream(
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        commandStore
    );

    commandRunner.processPriorCommands();

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
        versionChecker
    );
  }

  private static String getKafkaClusterId(final ServiceContext serviceContext) {
    try {
      return serviceContext.getAdminClient().describeCluster().clusterId().get();

    } catch (final UnsupportedVersionException e) {
      throw new KsqlException(
          "The kafka brokers are incompatible with. "
          + "KSQL requires broker versions >= 0.10.1.x"
      );
    } catch (final Exception e) {
      throw new KsqlException("Failed to get Kafka cluster information", e);
    }
  }

  static void ensureCommandTopic(final KsqlRestConfig restConfig,
                                 final KafkaTopicClient topicClient,
                                 final String commandTopic) {
    final long requiredTopicRetention = Long.MAX_VALUE;
    if (topicClient.isTopicExists(commandTopic)) {
      final ImmutableMap<String, Object> requiredConfig =
          ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention);

      if (topicClient.addTopicConfig(commandTopic, requiredConfig)) {
        log.info("Corrected retention.ms on command topic. topic:{}, retention.ms:{}",
                 commandTopic, requiredTopicRetention);
      }

      return;
    }

    try {
      short replicationFactor = 1;
      if (restConfig.getOriginals().containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)) {
        replicationFactor = Short.parseShort(
            restConfig
                .getOriginals()
                .get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY)
                .toString()
        );
      }
      if (replicationFactor < 2) {
        log.warn("Creating topic {} with replication factor of {} which is less than 2. "
                 + "This is not advisable in a production environment. ",
                commandTopic, replicationFactor);
      }

      // for now we create the command topic with infinite retention so that we
      // don't lose any data in case of fail over etc.
      topicClient.createTopic(
          commandTopic,
          1,
          replicationFactor,
          Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention)
      );
    } catch (final KafkaTopicExistsException e) {
      log.info("Command Topic Exists: {}", e.getMessage());
    }
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
