/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli.commands;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import io.confluent.ksql.cli.LocalCli;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.JLineTerminal;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Command(name = "local", description = "Run a local (standalone) Cli session")
public class Local extends AbstractCliCommands {

  private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";

  private static final String PORT_NUMBER_OPTION_NAME = "--port-number";
  private static final int PORT_NUMBER_OPTION_DEFAULT = 9098;

  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_NAME = "--bootstrap-server";
  private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

  private static final String APPLICATION_ID_OPTION_NAME = "--application-id";
  private static final String APPLICATION_ID_OPTION_DEFAULT = "ksql_standalone_cli";

  private static final String COMMAND_TOPIC_SUFFIX_OPTION_NAME = "--command-topic-suffix";
  private static final String COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT = "commands";

  @Port(acceptablePorts = PortType.ANY)
  @Option(
      name = PORT_NUMBER_OPTION_NAME,
      description = "The portNumber to use for the connection (defaults to "
          + PORT_NUMBER_OPTION_DEFAULT
          + ")"
  )
  int portNumber = PORT_NUMBER_OPTION_DEFAULT;

  @Option(
      name = KAFKA_BOOTSTRAP_SERVER_OPTION_NAME,
      description = "The Kafka server to connect to (defaults to "
          + KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT
          + ")"
  )
  String bootstrapServer;

  @Option(
      name = APPLICATION_ID_OPTION_NAME,
      description = "The application ID to use for the created Kafka Streams instance(s) "
          + "(defaults to '"
          + APPLICATION_ID_OPTION_DEFAULT
          + "')"
  )
  String applicationId;

  @Option(
      name = COMMAND_TOPIC_SUFFIX_OPTION_NAME,
      description = "The suffix to append to the end of the name of the command topic "
          + "(defaults to '"
          + COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT
          + "')"
  )
  String commandTopicSuffix;

  @Option(
      name = PROPERTIES_FILE_OPTION_NAME,
      description = "A file specifying properties for Ksql and its underlying Kafka Streams "
          + "instance(s) (can specify port number, bootstrap server, etc. but these options will "
          + "be overridden if also given via  flags)"
  )
  String propertiesFile;

  @Override
  public LocalCli getCli() throws Exception {
    Properties serverProperties;
    try {
      serverProperties = getStandaloneProperties();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }

    KsqlRestClient restClient = new KsqlRestClient(CliUtils.getServerAddress(portNumber));
    Console terminal = new JLineTerminal(parseOutputFormat(), restClient);

    return new LocalCli(
        serverProperties,
        portNumber,
        streamedQueryRowLimit,
        streamedQueryTimeoutMs,
        restClient,
        terminal
    );
  }

  private Properties getStandaloneProperties() throws IOException {
    Properties properties = new Properties();
    addDefaultProperties(properties);
    addFileProperties(properties);
    addFlagProperties(properties);
    return properties;
  }

  private void addDefaultProperties(Properties properties) {
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
    properties.put(
        KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG,
        COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT
    );
  }

  private void addFileProperties(Properties properties) throws IOException {
    if (propertiesFile != null) {
      properties.load(new FileInputStream(propertiesFile));
    }
  }

  private void addFlagProperties(Properties properties) {
    if (bootstrapServer != null) {
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    }
    if (applicationId != null) {
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    }
    if (commandTopicSuffix != null) {
      properties.put(KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, commandTopicSuffix);
    }
  }
}
