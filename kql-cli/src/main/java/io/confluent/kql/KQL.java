/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.kql.rest.client.KQLRestClient;
import io.confluent.kql.rest.server.KQLRestApplication;
import io.confluent.kql.rest.server.KQLRestConfig;
import io.confluent.rest.RestConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KQL {

  @Command(name = "standalone", description = "Run a standalone Cli session")
  public static class Standalone implements Runnable {

    private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";

    private static final String PORT_NUMBER_OPTION_NAME = "--portNumber";
    private static final int PORT_NUMBER_OPTION_DEFAULT = 6969;

    private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_NAME = "--bootstrap-server";
    private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

    private static final String APPLICATION_ID_OPTION_NAME = "--application-id";
    private static final String APPLICATION_ID_OPTION_DEFAULT = "kql_standalone_cli";

    private static final String NODE_ID_OPTION_NAME = "--node-id";
    private static final String NODE_ID_OPTION_DEFAULT = "node";

    private static final String COMMAND_TOPIC_SUFFIX_OPTION_NAME = "--command-topic-suffix";
    private static final String COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT = "commands";

    @Port(acceptablePorts = PortType.ANY)
    @Option(
        name = PORT_NUMBER_OPTION_NAME,
        description = "The portNumber to use for the connection (defaults to " + PORT_NUMBER_OPTION_DEFAULT + ")"
    )
    private Integer portNumber;

    @Option(
        name = KAFKA_BOOTSTRAP_SERVER_OPTION_NAME,
        description = "The Kafka server to connect to (defaults to " + KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT + ")"
    )
    private String bootstrapServer;

    @Option(
        name = APPLICATION_ID_OPTION_NAME,
        description = "The application ID to use for the created Kafka Streams instance(s) (defaults to "
            + APPLICATION_ID_OPTION_DEFAULT + ")"
    )
    private String applicationId;

    @Option(
        name = NODE_ID_OPTION_NAME,
        description = "The node ID to assign to the standalone KQL server instance (defaults to "
            + NODE_ID_OPTION_DEFAULT + ")"
    )
    private String nodeId;

    @Option(
        name = COMMAND_TOPIC_SUFFIX_OPTION_NAME,
        description = "The suffix to append to the end of the name of the command topic (defaults to "
            + COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT + ")"
    )
    private String commandTopicSuffix;

    @Option(
        name = PROPERTIES_FILE_OPTION_NAME,
        description = "A file specifying properties for KQL and its underlying Kafka Streams instance(s) "
            + "(can specify port number, bootstrap server, etc. but these options will be overridden if also given via "
            + "flags)"
    )
    private String propertiesFile;

    @Override
    public void run() {
      Properties restServerProperties;
      try {
        restServerProperties = getStandaloneProperties();
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      }

      KQLRestApplication restServer;
      try {
        restServer = KQLRestApplication.buildApplication(restServerProperties, false);
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }

      List<String> listeners = restServer.getConfiguration().getList(RestConfig.LISTENERS_CONFIG);
      if (listeners.isEmpty()) {
        throw new RuntimeException("Could not deduce location of standalone server");
      }
      String server = listeners.get(0);

      try {
        restServer.start();
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }

      try {
        new io.confluent.kql.Cli(new KQLRestClient(server)).repl();
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      } finally {
        try {
          restServer.stop();
          restServer.join();
        } catch (Exception exception) {
          throw new RuntimeException(exception);
        }
      }
    }

    private Properties getStandaloneProperties() throws IOException {
      Properties properties = new Properties();
      addDefaultProperties(properties);
      addFileProperties(properties);
      addFlagProperties(properties);
      return properties;
    }

    private void addDefaultProperties(Properties properties) {
      properties.put(RestConfig.LISTENERS_CONFIG, String.format("http://localhost:%d", PORT_NUMBER_OPTION_DEFAULT));
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT);
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_OPTION_DEFAULT);
      properties.put(KQLRestConfig.NODE_ID_CONFIG, NODE_ID_OPTION_DEFAULT);
      properties.put(KQLRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT);
    }

    private void addFileProperties(Properties properties) throws IOException {
      if (propertiesFile != null) {
        properties.load(new FileInputStream(propertiesFile));
      }
    }

    private void addFlagProperties(Properties properties) {
      if (portNumber != null) {
        properties.put(RestConfig.LISTENERS_CONFIG, String.format("http://localhost:%d", portNumber));
      }
      if (bootstrapServer != null) {
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
      }
      if (applicationId != null) {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      }
      if (nodeId != null) {
        properties.put(KQLRestConfig.NODE_ID_CONFIG, nodeId);
      }
      if (commandTopicSuffix != null) {
        properties.put(KQLRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, commandTopicSuffix);
      }
    }
  }

  @Command(name = "distributed", description = "Connect to a distributed KQL session")
  public static class Distributed implements Runnable {

    @Once
    @Required
    @Arguments(
        title = "server",
        description = "The address of the KQL server to connect to (ex: http://confluent.io:6969)"
    )
    private String server;

    @Override
    public void run() {
      try {
        new io.confluent.kql.Cli(new KQLRestClient(server)).repl();
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      }
    }
  }

  public static void main(String[] args) throws IOException {

    Cli<Runnable> cli = Cli.<Runnable>builder("Cli")
        .withDescription("Kafka Query Language")
        .withDefaultCommand(Help.class)
        .withCommand(Standalone.class)
        .withCommand(Distributed.class)
        .build();

    try {
      cli.parse(args).run();
    } catch (ParseException exception) {
      if (exception.getMessage() != null) {
        System.err.println(exception.getMessage());
      } else {
        System.err.println("Options parsing failed for an unknown reason");
      }
      System.err.println("See the help command for usage information");
    }
  }
}
