/**
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.ddl.commands.RegisterTopicCommand;
import io.confluent.ksql.exception.KafkaTopicException;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
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
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.ServerInfoResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Version;
import io.confluent.rest.Application;
import io.confluent.rest.validation.JacksonMessageBodyProvider;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Configurable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KsqlRestApplication extends Application<KsqlRestConfig> {

  private static final Logger log = LoggerFactory.getLogger(KsqlRestApplication.class);

  public static final String COMMANDS_KSQL_TOPIC_NAME = "__KSQL_COMMANDS_TOPIC";
  public static final String COMMANDS_STREAM_NAME = "KSQL_COMMANDS";
  private static AdminClient adminClient;

  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final ServerInfoResource serverInfoResource;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final boolean enableQuickstartPage;

  private final Thread commandRunnerThread;
  private final VersionCheckerAgent versionChckerAgent;

  public static String getCommandsKsqlTopicName() {
    return COMMANDS_KSQL_TOPIC_NAME;
  }

  public static String getCommandsStreamName() {
    return COMMANDS_STREAM_NAME;
  }

  public KsqlRestApplication(
      KsqlEngine ksqlEngine,
      KsqlRestConfig config,
      CommandRunner commandRunner,
      ServerInfoResource serverInfoResource,
      StatusResource statusResource,
      StreamedQueryResource streamedQueryResource,
      KsqlResource ksqlResource,
      boolean enableQuickstartPage,
      VersionCheckerAgent versionCheckerAgent
  ) {
    super(config);
    this.ksqlEngine = ksqlEngine;
    this.commandRunner = commandRunner;
    this.serverInfoResource = serverInfoResource;
    this.statusResource = statusResource;
    this.streamedQueryResource = streamedQueryResource;
    this.ksqlResource = ksqlResource;
    this.enableQuickstartPage = enableQuickstartPage;
    this.versionChckerAgent = versionCheckerAgent;

    this.commandRunnerThread = new Thread(commandRunner);
  }

  @Override
  public void setupResources(Configurable<?> config, KsqlRestConfig appConfig) {
    config.register(serverInfoResource);
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(new KsqlExceptionMapper());
  }

  @Override
  public ResourceCollection getStaticResources() {
    if (enableQuickstartPage) {
      return new ResourceCollection(Resource.newClassPathResource("/io/confluent/ksql/rest/"));
    } else {
      return super.getStaticResources();
    }
  }

  private static Properties getProps(String propsFile) throws IOException {
    Properties result = new Properties();
    result.put("application.id", "KSQL_REST_SERVER_DEFAULT_APP_ID");
    try(final FileInputStream inputStream = new FileInputStream(propsFile)) {
      result.load(inputStream);
    }
    return result;
  }

  @Override
  public void start() throws Exception {
    super.start();
    commandRunnerThread.start();
    Properties metricsProperties = new Properties();
    metricsProperties.putAll(getConfiguration().getOriginals());
    if (versionChckerAgent != null) {
      versionChckerAgent.start(KsqlModuleType.SERVER, metricsProperties);
    }
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
    ObjectMapper jsonMapper = getJsonMapper();
    new SchemaMapper().registerToObjectMapper(jsonMapper);

    JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(jsonMapper);
    config.register(jsonProvider);
    config.register(JsonParseExceptionMapper.class);

    // Don't want to buffer rows when streaming JSON in a request to the query resource
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    if (enableQuickstartPage) {
      config.property(ServletProperties.FILTER_STATIC_CONTENT_REGEX, "^/quickstart\\.html$");
    }
  }

  public static void main(String[] args) throws Exception {
    CliOptions cliOptions = CliOptions.parse(args);
    if (cliOptions == null) {
      return;
    }

    KsqlRestConfig restConfig = new KsqlRestConfig(getProps(cliOptions.getPropertiesFile()));
    KsqlRestApplication app = buildApplication(restConfig, cliOptions.getQuickstart(), new KsqlVersionCheckerAgent());

    log.info("Starting server");
    app.start();
    log.info("Server up and running");
    app.join();
    log.info("Server shutting down");
  }

  public static KsqlRestApplication buildApplication(
      KsqlRestConfig restConfig,
      boolean quickstart,
      VersionCheckerAgent versionCheckerAgent
  )
      throws Exception {

    Map<String, Object> ksqlConfProperties = new HashMap<>();
    ksqlConfProperties.putAll(restConfig.getCommandConsumerProperties());
    ksqlConfProperties.putAll(restConfig.getCommandProducerProperties());
    ksqlConfProperties.putAll(restConfig.getKsqlStreamsProperties());
    ksqlConfProperties.putAll(restConfig.getOriginals());

    KsqlConfig ksqlConfig = new KsqlConfig(ksqlConfProperties);
    adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    KsqlEngine ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(adminClient));
    KafkaTopicClient client = ksqlEngine.getTopicClient();

    String commandTopic = restConfig.getCommandTopic();

    try {
      short replicationFactor = 1;
      if(restConfig.getOriginals().containsKey(KsqlConstants.SINK_NUMBER_OF_REPLICAS)) {
        replicationFactor = Short.parseShort(restConfig.getOriginals()
                                                     .get(KsqlConstants.SINK_NUMBER_OF_REPLICAS).toString());
      }
      client.createTopic(commandTopic, 1, replicationFactor);
    } catch (KafkaTopicException e) {
      log.info("Command Topic Exists: " + e.getMessage());
    }

    Map<String, Expression> commandTopicProperties = new HashMap<>();
    commandTopicProperties.put(
        DdlConfig.VALUE_FORMAT_PROPERTY,
        new StringLiteral("json")
    );
    commandTopicProperties.put(
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY,
        new StringLiteral(commandTopic)
    );

    ksqlEngine.getDDLCommandExec().execute(new RegisterTopicCommand(new RegisterTopic(
            QualifiedName.of(COMMANDS_KSQL_TOPIC_NAME),
            false,
            commandTopicProperties)));

    ksqlEngine.getDDLCommandExec().execute(new CreateStreamCommand(new CreateStream(
            QualifiedName.of(COMMANDS_STREAM_NAME),
            Collections.singletonList(new TableElement("STATEMENT", "STRING")),
            false,
            Collections.singletonMap(
                    DdlConfig.TOPIC_NAME_PROPERTY,
                    new StringLiteral(COMMANDS_KSQL_TOPIC_NAME)
            )), Collections.emptyMap(), ksqlEngine.getTopicClient()));

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

    ServerInfoResource serverInfoResource =
        new ServerInfoResource(new ServerInfo(Version.getVersion()));
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
        serverInfoResource,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        quickstart,
        versionCheckerAgent
    );
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

  public KsqlEngine getKsqlEngine() {
    return ksqlEngine;
  }
}

/*
        TODO: Find a good, forwards-compatible use for the root resource
 */
