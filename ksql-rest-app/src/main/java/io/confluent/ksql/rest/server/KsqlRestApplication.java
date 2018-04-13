/*
 * Copyright 2017 Confluent Inc.
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
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.eclipse.jetty.websocket.jsr356.server.ServerContainer;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.websocket.DeploymentException;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;
import javax.ws.rs.core.Configurable;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.exception.KafkaTopicException;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandIdAssigner;
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
import io.confluent.ksql.rest.util.ZipUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.validation.JacksonMessageBodyProvider;

public class KsqlRestApplication extends Application<KsqlRestConfig> implements Executable {

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  public static final String COMMANDS_KSQL_TOPIC_NAME = "__KSQL_COMMANDS_TOPIC";
  private static final String COMMANDS_STREAM_NAME = "KSQL_COMMANDS";
  private static final String EXPANDED_FOLDER = "/expanded";
  private static AdminClient adminClient;

  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final RootDocument rootDocument;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final boolean isUiEnabled;
  private final String uiFolder;

  private final ServerInfo serverInfo;

  private final Thread commandRunnerThread;
  private final VersionCheckerAgent versionCheckerAgent;

  public static String getCommandsStreamName() {
    return COMMANDS_STREAM_NAME;
  }

  private KsqlRestApplication(
      KsqlEngine ksqlEngine,
      KsqlRestConfig config,
      CommandRunner commandRunner,
      RootDocument rootDocument,
      StatusResource statusResource,
      StreamedQueryResource streamedQueryResource,
      KsqlResource ksqlResource,
      boolean isUiEnabled,
      VersionCheckerAgent versionCheckerAgent,
      ServerInfo serverInfo
  ) {
    super(config);
    this.ksqlEngine = ksqlEngine;
    this.commandRunner = commandRunner;
    this.rootDocument = rootDocument;
    this.statusResource = statusResource;
    this.streamedQueryResource = streamedQueryResource;
    this.ksqlResource = ksqlResource;

    this.versionCheckerAgent = versionCheckerAgent;
    this.serverInfo = serverInfo;

    this.commandRunnerThread = new Thread(commandRunner);
    final String ksqlInstallDir = config.getString(KsqlRestConfig.INSTALL_DIR_CONFIG);

    if (ksqlInstallDir == null || ksqlInstallDir.trim().isEmpty() && isUiEnabled) {
      log.warn("System property {} is not set. User interface will be disabled",
          KsqlRestConfig.INSTALL_DIR_CONFIG);
      this.uiFolder = null;
    } else if (isUiEnabled) {
      this.uiFolder = ksqlInstallDir + "/ui";
    } else {
      this.uiFolder = null;
    }
    this.isUiEnabled = isUiEnabled && uiFolder != null;
  }

  @Override
  public void setupResources(Configurable<?> config, KsqlRestConfig appConfig) {
    config.register(rootDocument);
    config.register(new ServerInfoResource(serverInfo));
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(new KsqlExceptionMapper());
  }

  @Override
  public ResourceCollection getStaticResources() {
    log.info("User interface enabled: {}", isUiEnabled);
    if (isUiEnabled) {
      try {
        return new ResourceCollection(
            Resource.newResource(new File(this.uiFolder, EXPANDED_FOLDER).getCanonicalFile()));
      } catch (Exception e) {
        log.error("Unable to load ui from {}. You can disable the ui by setting {} to false",
            this.uiFolder + EXPANDED_FOLDER,
            KsqlRestConfig.UI_ENABLED_CONFIG,
            e);
      }
    }

    return super.getStaticResources();
  }

  @Override
  public void start() throws Exception {
    super.start();
    commandRunnerThread.start();
    Properties metricsProperties = new Properties();
    metricsProperties.putAll(getConfiguration().getOriginals());
    if (versionCheckerAgent != null) {
      versionCheckerAgent.start(KsqlModuleType.SERVER, metricsProperties);
    }

    displayWelcomeMessage();
  }

  @Override
  public void stop() throws Exception {
    ksqlEngine.close();
    commandRunner.close();
    try {
      commandRunnerThread.join();
    } catch (InterruptedException exception) {
      log.error("Interrupted while waiting for CommandRunner thread to complete", exception);
    }
    super.stop();
  }

  @Override
  public void configureBaseApplication(Configurable<?> config, Map<String, String> metricTags) {
    // Would call this but it registers additional, unwanted exception mappers
    // super.configureBaseApplication(config, metricTags);
    // Instead, just copy+paste the desired parts from Application.configureBaseApplication() here:
    JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(getJsonMapper());
    config.register(jsonProvider);
    config.register(JsonParseExceptionMapper.class);

    // Don't want to buffer rows when streaming JSON in a request to the query resource
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    if (isUiEnabled) {
      loadUiWar();
      config.property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "/(static/.*|.*html)");
    }
  }

  @Override
  protected ObjectMapper getJsonMapper() {
    // superclass creates a new json mapper on every call
    // we should probably ony create one per application
    ObjectMapper jsonMapper = super.getJsonMapper();
    new SchemaMapper().registerToObjectMapper(jsonMapper);
    return jsonMapper;
  }

  @Override
  protected void registerWebSocketEndpoints(ServerContainer container) {
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
      final ObjectMapper mapper = getJsonMapper();
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
                public <T> T getEndpointInstance(Class<T> endpointClass) {
                  return (T) new WSQueryEndpoint(
                      mapper,
                      statementParser,
                      ksqlEngine,
                      exec
                  );
                }
              })
              .build()
      );
    } catch (DeploymentException e) {
      log.error("Unable to create websockets endpoint", e);
    }
  }

  public static KsqlRestApplication buildApplication(
      KsqlRestConfig restConfig,
      boolean isUiEnabled,
      VersionCheckerAgent versionCheckerAgent
  )
      throws Exception {

    Map<String, Object> ksqlConfProperties = new HashMap<>();
    ksqlConfProperties.putAll(restConfig.getKsqlConfigProperties());

    KsqlConfig ksqlConfig = new KsqlConfig(ksqlConfProperties);

    adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    KsqlEngine ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(adminClient));
    KafkaTopicClient topicClient = ksqlEngine.getTopicClient();

    final String kafkaClusterId;
    try {
      kafkaClusterId = adminClient.describeCluster().clusterId().get();
    } catch (final UnsupportedVersionException e) {
      throw new KsqlException(
          "The kafka brokers are incompatible with. "
          + "KSQL requires broker versions >= 0.10.1.x"
      );
    }

    String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    String commandTopic =
        restConfig.getCommandTopic(ksqlServiceId);
    ensureCommandTopic(restConfig, topicClient, commandTopic);

    Map<String, Expression> commandTopicProperties = new HashMap<>();
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
    )), false);

    ksqlEngine.getDdlCommandExec().execute(new CreateStreamCommand(
        "statementText",
        new CreateStream(
            QualifiedName.of(COMMANDS_STREAM_NAME),
            Collections.singletonList(new TableElement(
                "STATEMENT",
                "STRING"
            )),
            false,
            Collections.singletonMap(
                DdlConfig.TOPIC_NAME_PROPERTY,
                new StringLiteral(COMMANDS_KSQL_TOPIC_NAME)
            )
        ),
        ksqlEngine.getTopicClient(),
        true
    ), false);

    Map<String, Object> commandConsumerProperties = restConfig.getCommandConsumerProperties();
    KafkaConsumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        getJsonDeserializer(CommandId.class, true),
        getJsonDeserializer(Command.class, false)
    );

    KafkaProducer<CommandId, Command> commandProducer = new KafkaProducer<>(
        restConfig.getCommandProducerProperties(),
        getJsonSerializer(true),
        getJsonSerializer(false)
    );

    CommandStore commandStore = new CommandStore(
        commandTopic,
        commandConsumer,
        commandProducer,
        new CommandIdAssigner(ksqlEngine.getMetaStore())
    );

    StatementParser statementParser = new StatementParser(ksqlEngine);

    StatementExecutor statementExecutor = new StatementExecutor(
        ksqlEngine,
        statementParser
    );

    CommandRunner commandRunner = new CommandRunner(
        statementExecutor,
        commandStore
    );

    RootDocument rootDocument = new RootDocument(isUiEnabled,
                                                 restConfig.getList(RestConfig.LISTENERS_CONFIG)
                                                     .get(0));

    StatusResource statusResource = new StatusResource(statementExecutor);
    StreamedQueryResource streamedQueryResource = new StreamedQueryResource(
        ksqlEngine,
        statementParser,
        restConfig.getLong(KsqlRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)
    );
    KsqlResource ksqlResource = new KsqlResource(
        ksqlEngine,
        commandStore,
        statementExecutor,
        restConfig.getLong(KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)
    );

    commandRunner.processPriorCommands();

    return new KsqlRestApplication(
        ksqlEngine,
        restConfig,
        commandRunner,
        rootDocument,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        isUiEnabled,
        versionCheckerAgent,
        new ServerInfo(Version.getVersion(), kafkaClusterId, ksqlServiceId)
    );
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
        log.warn("Creating topic {} with replication factor of %d which is less than 2. "
                 + "This is not advisable in a production environment. ", replicationFactor);
      }

      // for now we create the command topic with infinite retention so that we
      // don't lose any data in case of fail over etc.
      topicClient.createTopic(
          commandTopic,
          1,
          replicationFactor,
          Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, requiredTopicRetention)
      );
    } catch (KafkaTopicException e) {
      log.info("Command Topic Exists: {}", e.getMessage());
    }
  }

  private void loadUiWar() {
    final File uiFolder = new File(this.uiFolder);
    log.info("Loading UI-WAR from {}", uiFolder.getAbsolutePath());

    final File[] files = uiFolder
        .listFiles((dir, name) -> name.endsWith(".war"));

    if (files != null) {
      Arrays.stream(files).forEach(war -> KsqlRestApplication.unzipWar(war, uiFolder));
    }
  }

  private static void unzipWar(final File warFile, final File uiFolder) {
    try {
      ZipUtil.unzip(warFile, new File(uiFolder, EXPANDED_FOLDER));
      log.info("Expand WAR file '{}'", warFile.getPath());
    } catch (final Exception e) {
      log.warn("Failed to unzip WAR file: " + warFile.getPath(), e);
    }
  }

  private static <T> Serializer<T> getJsonSerializer(boolean isKey) {
    Serializer<T> result = new KafkaJsonSerializer<>();
    result.configure(Collections.emptyMap(), isKey);
    return result;
  }

  private static <T> Deserializer<T> getJsonDeserializer(Class<T> classs, boolean isKey) {
    Deserializer<T> result = new KafkaJsonDeserializer<>();
    String typeConfigProperty = isKey
                                ? KafkaJsonDeserializerConfig.JSON_KEY_TYPE
                                : KafkaJsonDeserializerConfig.JSON_VALUE_TYPE;

    Map<String, ?> props = Collections.singletonMap(
        typeConfigProperty,
        classs
    );
    result.configure(props, isKey);
    return result;
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
    final String listener = config.getList(RestConfig.LISTENERS_CONFIG)
        .get(0)
        .replace("0.0.0.0", "localhost");

    writer.printf("Server %s listening on %s%n", version, listener);
    writer.println();
    writer.println("To access the KSQL CLI, run:");
    writer.println("ksql " + listener);
    writer.println();

    if (isUiEnabled) {
      writer.println("To access the UI, point your browser at:");
      writer.printf(listener + "/index.html");
      writer.println();
    }

    writer.flush();
  }
}
