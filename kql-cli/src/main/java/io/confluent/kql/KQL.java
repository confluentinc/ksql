/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.kql.rest.client.KQLRestClient;
import io.confluent.kql.rest.server.KQLRestApplication;
import io.confluent.rest.RestConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KQL {

  @Command(name = "standalone", description = "Run a standalone Cli session")
  public static class Standalone implements Runnable {

//    private static final String PORT_NUMBER_OPTION_NAME = "--port";
    private static final String PROPERTIES_FILE_OPTION_NAME = "--properties-file";
    private static final String CATALOG_FILE_OPTION_NAME = "--catalog-file";
    private static final String QUERY_FILE_OPTION_NAME = "--query-file";
    private static final String QUERIES_OPTION_NAME = "--queries";
    private static final String QUERY_TIME_OPTION_NAME = "--query-time";

    private static final String NON_INTERACTIVE_QUERIES_TAG = "non-interactive queries";

//    @IntegerRange(min = 0, max = 65535)
//    @Option(
//        name = PORT_NUMBER_OPTION_NAME,
//        description = "The port to use for the connection"
//    )
//    private int port = 6969;

    @Required
    @Option(
        name = PROPERTIES_FILE_OPTION_NAME,
        description = "A file specifying properties for Cli and its underlying Kafka Streams instance(s)"
    )
    private String propertiesFile;

    @Option(
        name = CATALOG_FILE_OPTION_NAME,
        description = "A file to import metastore data from before execution"
    )
    private String catalogFile;

    @MutuallyExclusiveWith(tag = NON_INTERACTIVE_QUERIES_TAG)
    @Option(
        name = QUERY_FILE_OPTION_NAME,
        description = "A file to run non-interactive queries from"
    )
    private String queryFile;

    @MutuallyExclusiveWith(tag = NON_INTERACTIVE_QUERIES_TAG)
    @Option(
        name = QUERIES_OPTION_NAME,
        description = "One or more non-interactive queries to run"
    )
    private String queries;

    @Option(
        name = QUERY_TIME_OPTION_NAME,
        description = "How long to run non-interactive queries for (ms)"
    )
    private Long queryTime;

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

    // TODO: If the properties passed to the KQLRestApplication need to be mucked with, do it here; otherwise, get rid
    // of this method
    private Properties getStandaloneProperties() throws IOException {
      Properties properties = new Properties();
      properties.load(new FileInputStream(propertiesFile));
      return properties;
    }
  }

  @Command(name = "distributed", description = "Connect to a distributed Cli session")
  public static class Distributed implements Runnable {

    @Once
    @Required
    @Arguments(
        title = "server",
        description = "The address of the Cli server to connect to (ex: http://confluent.io:6969)"
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
