/**
 * Copyright 2017 Confluent Inc.
 */
package io.confluent.kql.datagen;

import io.confluent.avro.random.generator.Generator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

public class DataGen {

  public static void main(String[] args) throws IOException {
    Arguments arguments;

    try {
      arguments = new Arguments.Builder().parseArgs(args).build();
    } catch (Exception exception) {
      exception.printStackTrace();
      usage();
      return;
    }

    Generator generator = new Generator(arguments.schemaFile, new Random());
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
        usage();
        return;
    }

    dataProducer.populateAvroTopic(generator, arguments.topicName, arguments.keyName, arguments.iterations);
  }

  private static void usage() {
    System.err.println(
        "usage: DataGen "
            + "[quickstart=<quickstart preset> (one of 'orders', 'users', or 'pageview')] "
            + "schema=<avro schema file> "
            + "topic=<kafka topic name> "
            + "key=<name of key column> "
            + "format=<message format> "
            + "[iterations=<number of rows> (defaults to 1000)]"
    );
  }

  private static class Arguments {
    public enum Format { AVRO, JSON, CSV }

    public final InputStream schemaFile;
    public final String topicName;
    public final String keyName;
    public final Format format;
    public final int iterations;

    public Arguments(InputStream schemaFile, String topicName, String keyName, Format format, int iterations) {
      this.schemaFile = schemaFile;
      this.topicName = topicName;
      this.keyName = keyName;
      this.format = format;
      this.iterations = iterations;
    }

    public static class Builder {
      private String quickstart;
      private InputStream schemaFile;
      private String topicName;
      private String keyName;
      private Format format;
      private int iterations;


      public Builder() {
        quickstart = null;
        schemaFile = null;
        topicName = null;
        keyName = null;
        format = null;
        iterations = 1000;
      }

      public Arguments build() {
        switch (quickstart) {
          case "orders":
            schemaFile = Optional
                .ofNullable(schemaFile)
                .orElse(getClass().getClassLoader().getResourceAsStream("orders_schema.avro"));
            format = Optional
                .ofNullable(format)
                .orElse(Format.JSON);
            topicName = Optional
                .ofNullable(topicName)
                .orElse(String.format("orders_kafka_topic_%s", format.name().toLowerCase()));
            keyName = Optional
                .ofNullable(keyName)
                .orElse("ORDERID");
            break;

          case "users":
            schemaFile = Optional
                .ofNullable(schemaFile)
                .orElse(getClass().getClassLoader().getResourceAsStream("users_schema.avro"));
            format = Optional
                .ofNullable(format)
                .orElse(Format.JSON);
            topicName = Optional
                .ofNullable(topicName)
                .orElse(String.format("users_kafka_topic_%s", format.name().toLowerCase()));
            keyName = Optional
                .ofNullable(keyName)
                .orElse("userid");
            break;

          case "pageview":
            schemaFile = Optional
                .ofNullable(schemaFile)
                .orElse(getClass().getClassLoader().getResourceAsStream("pageview_schema.avro"));
            format = Optional
                .ofNullable(format)
                .orElse(Format.JSON);
            topicName = Optional
                .ofNullable(topicName)
                .orElse(String.format("pageview_kafka_topic_%s", format.name().toLowerCase()));
            keyName = Optional
                .ofNullable(keyName)
                .orElse("viewtime");
            break;
        }

        Objects.requireNonNull(schemaFile, "Schema file not provided");
        Objects.requireNonNull(topicName, "Kafka topic name not provided");
        Objects.requireNonNull(keyName, "Name of key column not provided");
        Objects.requireNonNull(format, "Message format not provided");
        return new Arguments(schemaFile, topicName, keyName, format, iterations);
      }

      public Builder parseArgs(String[] args) throws IOException {
        for (String arg : args) {
          parseArg(arg);
        }
        return this;
      }

      public Builder parseArg(String arg) throws IOException {

        String[] splitOnEquals = arg.split("=");
        if (splitOnEquals.length != 2) {
          throw new RuntimeException(String.format(
              "Invalid argument format in '%s'; expected <name>=<value>",
              arg
          ));
        }

        String argName = splitOnEquals[0].trim();
        String argValue = splitOnEquals[1].trim();

        if (argName.isEmpty()) {
          throw new RuntimeException(String.format(
              "Empty argument name in %s",
              arg
          ));
        }

        if (argValue.isEmpty()) {
          throw new RuntimeException(String.format(
              "Empty argument value in '%s'",
              arg
          ));
        }

        switch (argName) {
          case "quickstart":
            quickstart = argValue;
            break;
          case "schema":
            schemaFile = new FileInputStream(argValue);
            break;
          case "topic":
            topicName = argValue;
            break;
          case "key":
            keyName = argValue;
            break;
          case "format":
            format = parseFormat(argValue);
            break;
          case "iterations":
            iterations = parseIterations(argValue);
            break;
          default:
            throw new RuntimeException(String.format(
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
          throw new RuntimeException(String.format(
              "Invalid format in '%s'; was expecting one of AVRO, JSON, or CSV",
              formatString
          ));
        }
      }

      private int parseIterations(String iterationsString) {
        try {
          int result = Integer.valueOf(iterationsString, 10);
          if (result <= 0) {
            throw new RuntimeException(String.format(
                "Invalid number of iterations in '%d'; must be a positive number",
                result
            ));
          }
          return Integer.valueOf(iterationsString, 10);
        } catch (NumberFormatException exception) {
          throw new RuntimeException(String.format(
              "Invalid number of iterations in '%s'; must be a valid base 10 integer",
              iterationsString
          ));
        }
      }
    }
  }
}
