/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import io.confluent.adminclient.KafkaAdminClient;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandIdAssigner;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.StatementExecutor;
import io.confluent.ksql.rest.server.resources.KSQLExceptionMapper;
import io.confluent.ksql.rest.server.resources.KSQLResource;
import io.confluent.ksql.rest.server.resources.StatusResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.rest.Application;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Map;
import java.util.Properties;

public class KSQLRestApplication extends Application<KSQLRestConfig> {

  private static final Logger log = LoggerFactory.getLogger(KSQLRestApplication.class);

  private final KSQLEngine ksqlEngine;
  private final CommandRunner commandRunner;
  private final StatusResource statusResource;
  private final StreamedQueryResource streamedQueryResource;
  private final KSQLResource ksqlResource;
  private final boolean enableQuickstartPage;

  private final Thread commandRunnerThread;

  public KSQLRestApplication(
      KSQLEngine ksqlEngine,
      KSQLRestConfig config,
      CommandRunner commandRunner,
      StatusResource statusResource,
      StreamedQueryResource streamedQueryResource,
      KSQLResource ksqlResource,
      boolean enableQuickstartPage
  ) {
    super(config);
    this.ksqlEngine = ksqlEngine;
    this.commandRunner = commandRunner;
    this.statusResource = statusResource;
    this.streamedQueryResource = streamedQueryResource;
    this.ksqlResource = ksqlResource;
    this.enableQuickstartPage = enableQuickstartPage;

    this.commandRunnerThread = new Thread(commandRunner);
  }

  @Override
  public void setupResources(Configurable<?> config, KSQLRestConfig appConfig) {
    config.register(statusResource);
    config.register(ksqlResource);
    config.register(streamedQueryResource);
    config.register(new KSQLExceptionMapper());
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
    CLIOptions cliOptions = CLIOptions.parse(args);
    if (cliOptions == null) {
      return;
    }

    Properties props = getProps(cliOptions.getPropertiesFile());
    KSQLRestApplication app = buildApplication(props, cliOptions.getQuickstart());
    log.info("Starting server");
    app.start();
    log.info("Server up and running");
    app.join();
    log.info("Server shutting down");
  }

  public static KSQLRestApplication buildApplication(Properties props, boolean quickstart) throws Exception {
    KSQLRestConfig config = new KSQLRestConfig(props);

    @SuppressWarnings("unchecked")
    KafkaAdminClient client = new KafkaAdminClient((Map) props);
    TopicUtil topicUtil = new TopicUtil(client);

    // TODO: Make MetaStore class configurable, consider renaming MetaStoreImpl to MetaStoreCache
    MetaStore metaStore = new MetaStoreImpl();

    KSQLEngine ksqlEngine = new KSQLEngine(metaStore, config.getKsqlStreamsProperties());
    StatementParser statementParser = new StatementParser(ksqlEngine);

    String commandTopic = config.getCommandTopic();
    topicUtil.ensureTopicExists(commandTopic);

    Map<String, Object> commandConsumerProperties = config.getCommandConsumerProperties();
    KafkaConsumer<CommandId, String> commandConsumer = new KafkaConsumer<>(
        commandConsumerProperties,
        getCommandIdDeserializer(),
        new StringDeserializer()
    );
    KafkaProducer<CommandId, String> commandProducer = new KafkaProducer<>(
        config.getCommandProducerProperties(),
        getCommandIdSerializer(),
        new StringSerializer()
    );

    CommandStore commandStore = new CommandStore(
        commandTopic,
        commandConsumer,
        commandProducer,
        new CommandIdAssigner(metaStore)
    );

    StatementExecutor statementExecutor = new StatementExecutor(
        topicUtil,
        ksqlEngine,
        statementParser
    );

    CommandRunner commandRunner = new CommandRunner(
        statementExecutor,
        commandStore
    );

    StatusResource statusResource = new StatusResource(statementExecutor);
    StreamedQueryResource streamedQueryResource = new StreamedQueryResource(
        ksqlEngine,
        statementParser,
        config.getLong(KSQLRestConfig.STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG)
    );
    KSQLResource ksqlResource = new KSQLResource(
        ksqlEngine,
        commandStore,
        statementExecutor
    );

    commandRunner.processPriorCommands();

    return new KSQLRestApplication(
        ksqlEngine,
        config,
        commandRunner,
        statusResource,
        streamedQueryResource,
        ksqlResource,
        quickstart
    );
  }

  private static Serializer<CommandId> getCommandIdSerializer() {
    Serializer<CommandId> result = new KafkaJsonSerializer<>();
    result.configure(Collections.emptyMap(), true);
    return result;
  }

  private static Deserializer<CommandId> getCommandIdDeserializer() {
    Deserializer<CommandId> result = new KafkaJsonDeserializer<>();
    Map<String, ?> props = Collections.singletonMap(
        KafkaJsonDeserializerConfig.JSON_KEY_TYPE,
        CommandId.class
    );
    result.configure(props, true);
    return result;
  }
}

/*
        TODO: Find a good, forwards-compatible use for the root resource
 */
