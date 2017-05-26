/**
 * Copyright 2017 Confluent Inc.
 */
package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

public class DataGen {

  public static void main(String[] args) {
    Arguments arguments;

    try {
      arguments = new Arguments.Builder().parseArgs(args).build();
    } catch (Arguments.ArgumentParseException exception) {
      System.err.println(exception.getMessage());
      usage(1);
      return;
    } catch (IOException exception) {
      System.err.printf("IOException encountered: %s%n", exception.getMessage());
      return;
    }

    if (arguments.help) {
      usage(0);
    }


    Generator generator;
    try {
      generator = new Generator(arguments.schemaFile, new Random());
    } catch (IOException exception) {
      System.err.printf("IOException encountered: %s%n", exception.getMessage());
      return;
    }
    DataGenProducer dataProducer;

    switch (arguments.format) {
      case AVRO:
        dataProducer = new AvroProducer();
        break;
      case JSON:
        dataProducer = new JsonProducer();
        break;
      case CSV:
        dataProducer = new CsvProducer();
        break;
      default:
        System.err.printf("Invalid format in '%s'; was expecting one of AVRO, JSON, or CSV%n", arguments.format);
        usage(1);
        return;
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", arguments.bootstrapServer);
    props.put("client.id", "KSQLDataGenProducer");

    dataProducer.populateTopic(props, generator, arguments.topicName, arguments.keyName, arguments.iterations);
  }

  private static void usage() {
    System.err.println(
        "usage: DataGen "
            + "[help] "
            + "[bootstrap-server=<kafka bootstrap server(s)> (defaults to localhost:9092)] "
            + "[quickstart=<quickstart preset> (case-insensitive; one of 'orders', 'users', or 'pageview')] "
            + "schema=<avro schema file> "
            + "format=<message format> (case-insensitive; one of 'avro', 'json', or 'csv') "
            + "topic=<kafka topic name> "
            + "key=<name of key column> "
            + "[iterations=<number of rows> (defaults to 1000)]"
    );
  }

  private static void usage(int exitValue) {
    usage();
    System.exit(exitValue);
  }

  private static class Arguments {
    public enum Format { AVRO, JSON, CSV }

    public final boolean help;
    public final String bootstrapServer;
    public final InputStream schemaFile;
    public final Format format;
    public final String topicName;
    public final String keyName;
    public final int iterations;

    public Arguments(
        boolean help,
        String bootstrapServer,
        InputStream schemaFile,
        Format format,
        String topicName,
        String keyName,
        int iterations
    ) {
      this.help = help;
      this.bootstrapServer = bootstrapServer;
      this.schemaFile = schemaFile;
      this.format = format;
      this.topicName = topicName;
      this.keyName = keyName;
      this.iterations = iterations;
    }

    public static class ArgumentParseException extends RuntimeException {
      public ArgumentParseException(String message) {
        super(message);
      }
    }

    public static class Builder {
      private Quickstart quickstart;

      private boolean help;
      private String bootstrapServer;
      private InputStream schemaFile;
      private Format format;
      private String topicName;
      private String keyName;
      private int iterations;


      public Builder() {
        quickstart = null;
        help = false;
        bootstrapServer = "localhost:9092";
        schemaFile = null;
        format = null;
        topicName = null;
        keyName = null;
        iterations = 1000;
      }

      private enum Quickstart {
        ORDERS("orders_schema.avro", "orders", "orderid"),
        USERS("users_schema.avro", "users", "userid"),
        PAGEVIEW("pageview_schema.avro", "pageview", "viewtime");

        private final String schemaFileName;
        private final String rootTopicName;
        private final String keyName;

        Quickstart(String schemaFileName, String rootTopicName, String keyName) {
          this.schemaFileName = schemaFileName;
          this.rootTopicName = rootTopicName;
          this.keyName = keyName;
        }

        public InputStream getSchemaFile() {
          return getClass().getClassLoader().getResourceAsStream(schemaFileName);
        }

        public String getTopicName(Format format) {
          return String.format("%s_kafka_topic_%s", rootTopicName, format.name().toLowerCase());
        }

        public String getKeyName() {
          return keyName;
        }

        public Format getFormat() {
          return Format.JSON;
        }

      }

      public Arguments build() {
        if (help) {
          return new Arguments(true, null, null, null, null, null, 0);
        }

        if (quickstart != null) {
          schemaFile = Optional.ofNullable(schemaFile).orElse(quickstart.getSchemaFile());
          format = Optional.ofNullable(format).orElse(quickstart.getFormat());
          topicName = Optional.ofNullable(topicName).orElse(quickstart.getTopicName(format));
          keyName = Optional.ofNullable(keyName).orElse(quickstart.getKeyName());
        }

        try {
          Objects.requireNonNull(schemaFile, "Schema file not provided");
          Objects.requireNonNull(format, "Message format not provided");
          Objects.requireNonNull(topicName, "Kafka topic name not provided");
          Objects.requireNonNull(keyName, "Name of key column not provided");
        } catch (NullPointerException exception) {
          throw new ArgumentParseException(exception.getMessage());
        }
        return new Arguments(help, bootstrapServer, schemaFile, format, topicName, keyName, iterations);
      }

      public Builder parseArgs(String[] args) throws IOException {
        for (String arg : args) {
          parseArg(arg);
        }
        return this;
      }

      public Builder parseArg(String arg) throws IOException {

        if ("help".equals(arg)) {
          help = true;
          return this;
        }

        String[] splitOnEquals = arg.split("=");
        if (splitOnEquals.length != 2) {
          throw new ArgumentParseException(String.format(
              "Invalid argument format in '%s'; expected <name>=<value>",
              arg
          ));
        }

        String argName = splitOnEquals[0].trim();
        String argValue = splitOnEquals[1].trim();

        if (argName.isEmpty()) {
          throw new ArgumentParseException(String.format(
              "Empty argument name in %s",
              arg
          ));
        }

        if (argValue.isEmpty()) {
          throw new ArgumentParseException(String.format(
              "Empty argument value in '%s'",
              arg
          ));
        }

        switch (argName) {
          case "quickstart":
            try {
              quickstart = Quickstart.valueOf(argValue.toUpperCase());
            } catch (IllegalArgumentException iae) {
              throw new ArgumentParseException(String.format(
                  "Invalid quickstart in '%s'; was expecting one of ORDERS, USERS, or PAGEVIEW (case-insensitive)",
                  argValue
              ));
            }
            break;
          case "bootstrap-server":
            bootstrapServer = argValue;
            break;
          case "schema":
            schemaFile = new FileInputStream(argValue);
            break;
          case "format":
            format = parseFormat(argValue);
            break;
          case "topic":
            topicName = argValue;
            break;
          case "key":
            keyName = argValue;
            break;
          case "iterations":
            iterations = parseIterations(argValue);
            break;
          default:
            throw new ArgumentParseException(String.format(
                "Unknown argument name in '%s'",
                argName
            ));
        }
        return this;
      }

      private Format parseFormat(String formatString) {
        try {
          return Format.valueOf(formatString.toUpperCase());
        } catch (IllegalArgumentException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid format in '%s'; was expecting one of AVRO, JSON, or CSV (case-insensitive)",
              formatString
          ));
        }
      }

      private int parseIterations(String iterationsString) {
        try {
          int result = Integer.valueOf(iterationsString, 10);
          if (result <= 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of iterations in '%d'; must be a positive number",
                result
            ));
          }
          return Integer.valueOf(iterationsString, 10);
        } catch (NumberFormatException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid number of iterations in '%s'; must be a valid base 10 integer",
              iterationsString
          ));
        }
      }
    }
  }
}
