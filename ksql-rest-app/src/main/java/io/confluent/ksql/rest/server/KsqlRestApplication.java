/**
 * Copyright 2017 Confluent Inc.
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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
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
import io.confluent.ksql.util.TopicUtil;
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

  private static final String COMMANDS_KSQL_TOPIC_NAME = "__COMMANDS_TOPIC";
  private static final String COMMANDS_STREAM_NAME = "COMMANDS";

  private final KsqlEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final ServerInfoResource serverInfoResource;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KsqlResource ksqlResource;
  private final TopicUtil topicUtil;
  private final boolean enableQuickstartPage;

  private final Thread commandRunnerThread;

  public KsqlRestApplication(
      KsqlEngine ksqlEngine,
      KsqlRestConfig config,
      CommandRunner commandRunner,
      ServerInfoResource serverInfoResource,
      StatusResource statusResource,
      StreamedQueryResource streamedQueryResource,
      KsqlResource ksqlResource,
      TopicUtil topicUtil,
      boolean enableQuickstartPage
  ) {
    super(config);
    this.ksqlEngine = ksqlEngine;
    this.commandRunner = commandRunner;
    this.serverInfoResource = serverInfoResource;
    this.statusResource = statusResource;
    this.streamedQueryResource = streamedQueryResource;
    this.ksqlResource = ksqlResource;
    this.topicUtil = topicUtil;
    this.enableQuickstartPage = enableQuickstartPage;

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
    result.load(new FileInputStream(propsFile));
    return result;
  }

  @Override
  public void start() throws Exception {
    super.start();
    commandRunnerThread.start();
  }

  @Override
  public void stop() throws Exception {
    ksqlEngine.close();
    commandRunner.close();
    topicUtil.close();
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

    Properties props = getProps(cliOptions.getPropertiesFile());
    KsqlRestApplication app = buildApplication(props, cliOptions.getQuickstart());
    log.info("Starting server");
    app.start();
    log.info("Server up and running");
    app.join();
    log.info("Server shutting down");
  }

  public static KsqlRestApplication buildApplication(Properties props, boolean quickstart)
      throws Exception {
    KsqlRestConfig config = new KsqlRestConfig(props);

    AdminClient client = AdminClient.create((Map) props);
    TopicUtil topicUtil = new TopicUtil(client);

    String commandTopic = config.getCommandTopic();
    if (!topicUtil.ensureTopicExists(commandTopic, 1, (short) 1)) {
      throw new Exception(
          String.format("Failed to guarantee existence of command topic '%s'", commandTopic)
      );
    }

    // TODO: Make MetaStore class configurable, consider renaming MetaStoreImpl to MetaStoreCache
    MetaStore metaStore = new MetaStoreImpl();

    KsqlEngine ksqlEngine = new KsqlEngine(metaStore, config.getKsqlStreamsProperties());

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
            ))));

    Map<String, Object> commandConsumerProperties = config.getCommandConsumerProperties();
    KafkaConsumer<CommandId, Command> commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        getJsonDeserializer(CommandId.class, true),
        getJsonDeserializer(Command.class, false)
    );

    KafkaProducer<CommandId, Command> commandProducer = new KafkaProducer<>(
        config.getCommandProducerProperties(),
        getJsonSerializer(true),
        getJsonSerializer(false)
    );

    CommandStore commandStore = new CommandStore(
        commandTopic,
        commandConsumer,
        commandProducer,
        new CommandIdAssigner(metaStore)
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
        config.getLong(KsqlRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)
    );
    KsqlResource ksqlResource = new KsqlResource(
        ksqlEngine,
        commandStore,
        statementExecutor,
        config.getLong(KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG)
    );

    commandRunner.processPriorCommands();

    return new KsqlRestApplication(
        ksqlEngine,
        config,
        commandRunner,
        serverInfoResource,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        topicUtil,
        quickstart
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
}

/*
        TODO: Find a good, forwards-compatible use for the root resource
 */
