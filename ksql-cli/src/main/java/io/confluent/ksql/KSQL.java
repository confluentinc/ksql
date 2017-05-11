/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.LocalCli;
import io.confluent.ksql.cli.RemoteCli;
import io.confluent.ksql.rest.server.KSQLRestConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KSQL {

  public static abstract class KSQLCommand implements Runnable {
    protected abstract Cli getCli() throws Exception;

    @Override
    public void run() {
      try (Cli cli = getCli()) {
        cli.repl();
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    }
  }

  @Command(name = "local", description = "Run a local (standalone) Cli session")
  public static class Local extends KSQLCommand {

    private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";

    private static final String PORT_NUMBER_OPTION_NAME = "--port-number";
    private static final int PORT_NUMBER_OPTION_DEFAULT = 6969;

    private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_NAME = "--bootstrap-server";
    private static final String KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT = "localhost:9092";

    private static final String APPLICATION_ID_OPTION_NAME = "--application-id";
    private static final String APPLICATION_ID_OPTION_DEFAULT = "ksql_standalone_cli";

    private static final String COMMAND_TOPIC_SUFFIX_OPTION_NAME = "--command-topic-suffix";
    private static final String COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT = "commands";

    @Port(acceptablePorts = PortType.ANY)
    @Option(
        name = PORT_NUMBER_OPTION_NAME,
        description = "The portNumber to use for the connection (defaults to " + PORT_NUMBER_OPTION_DEFAULT + ")"
    )
    private int portNumber = PORT_NUMBER_OPTION_DEFAULT;

    @Option(
        name = KAFKA_BOOTSTRAP_SERVER_OPTION_NAME,
        description = "The Kafka server to connect to (defaults to " + KAFKA_BOOTSTRAP_SERVER_OPTION_DEFAULT + ")"
    )
    private String bootstrapServer;

    @Option(
        name = APPLICATION_ID_OPTION_NAME,
        description = "The application ID to use for the created Kafka Streams instance(s) (defaults to '"
            + APPLICATION_ID_OPTION_DEFAULT + "')"
    )
    private String applicationId;

    @Option(
        name = COMMAND_TOPIC_SUFFIX_OPTION_NAME,
        description = "The suffix to append to the end of the name of the command topic (defaults to '"
            + COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT + "')"
    )
    private String commandTopicSuffix;

    @Option(
        name = PROPERTIES_FILE_OPTION_NAME,
        description = "A file specifying properties for KSQL and its underlying Kafka Streams instance(s) "
            + "(can specify port number, bootstrap server, etc. but these options will be overridden if also given via "
            + "flags)"
    )
    private String propertiesFile;

    @Override
    protected Cli getCli() throws Exception {
      Properties serverProperties;
      try {
        serverProperties = getStandaloneProperties();
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      }

      return new LocalCli(serverProperties, portNumber);
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
      properties.put(KSQLRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, COMMAND_TOPIC_SUFFIX_OPTION_DEFAULT);
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
        properties.put(KSQLRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, commandTopicSuffix);
      }
    }
  }

  @Command(name = "remote", description = "Connect to a remote (possibly distributed) KSQL session")
  public static class Remote extends KSQLCommand {

    @Once
    @Required
    @Arguments(
        title = "server",
        description = "The address of the KSQL server to connect to (ex: http://confluent.io:6969)"
    )
    private String server;

    @Override
    public Cli getCli() throws Exception {
      return new RemoteCli(server);
    }
  }

  public static void main(String[] args) throws IOException {

    com.github.rvesse.airline.Cli<Runnable> cli = com.github.rvesse.airline.Cli.<Runnable>builder("Cli")
        .withDescription("Kafka Query Language")
        .withDefaultCommand(Help.class)
        .withCommand(Local.class)
        .withCommand(Remote.class)
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
