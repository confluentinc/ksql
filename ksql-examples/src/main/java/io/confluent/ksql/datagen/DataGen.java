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

package io.confluent.ksql.datagen;

import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.util.KsqlConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

public final class DataGen {

  private DataGen() {
  }

  public static void main(final String[] args) {
    try {
      run(args);
    } catch (final Arguments.ArgumentParseException exception) {
      System.err.println(exception.getMessage());
      usage();
      System.exit(1);
    } catch (final Exception e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  static void run(final String... args) throws IOException {
    final Arguments arguments = new Arguments.Builder()
        .parseArgs(args)
        .build();

    if (arguments.help) {
      usage();
      return;
    }

    final Generator generator = new Generator(arguments.schemaFile, new Random());
    final DataGenProducer dataProducer = getProducer(arguments);
    final Properties props = getProperties(arguments);

    dataProducer.populateTopic(
        props,
        generator,
        arguments.topicName,
        arguments.keyName,
        arguments.iterations,
        arguments.maxInterval
    );
  }

  private static Properties getProperties(final Arguments arguments) throws IOException {
    final Properties props = new Properties();
    props.put("bootstrap.servers", arguments.bootstrapServer);
    props.put("client.id", "KSQLDataGenProducer");

    if (arguments.propertiesFile != null) {
      props.load(arguments.propertiesFile);
    }

    return props;
  }

  private static DataGenProducer getProducer(final Arguments arguments) {
    switch (arguments.format) {
      case AVRO:
        return new AvroProducer(
            new KsqlConfig(
                Collections.singletonMap(
                    KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
                    arguments.schemaRegistryUrl
                )
            )
        );

      case JSON:
        return new JsonProducer();

      case DELIMITED:
        return new DelimitedProducer();

      default:
        throw new IllegalArgumentException("Invalid format in '" + arguments.format
                                           + "'; was expecting one of AVRO, JSON, or DELIMITED%n");
    }
  }

  private static void usage() {
    System.err.println(
        "usage: DataGen "
        + "[help] "
        + "[bootstrap-server=<kafka bootstrap server(s)> (defaults to localhost:9092)] "
        + "[quickstart=<quickstart preset> (case-insensitive; one of 'orders', 'users', or "
        + "'pageviews')] "
        + "schema=<avro schema file> "
        + "[schemaRegistryUrl=<url for Confluent Schema Registry> (defaults to http://localhost:8081)] "
        + "format=<message format> (case-insensitive; one of 'avro', 'json', or 'delimited') "
        + "topic=<kafka topic name> "
        + "key=<name of key column> "
        + "[iterations=<number of rows> (defaults to 1,000,000)] "
        + "[maxInterval=<Max time in ms between rows> (defaults to 500)] "
        + "[propertiesFile=<file specifying Kafka client properties>]"
    );
  }

  static class Arguments {

    public enum Format { AVRO, JSON, DELIMITED }

    private final boolean help;
    private final String bootstrapServer;
    private final InputStream schemaFile;
    private final Format format;
    private final String topicName;
    private final String keyName;
    private final int iterations;
    private final long maxInterval;
    private final String schemaRegistryUrl;
    private final InputStream propertiesFile;

    Arguments(
        final boolean help,
        final String bootstrapServer,
        final InputStream schemaFile,
        final Format format,
        final String topicName,
        final String keyName,
        final int iterations,
        final long maxInterval,
        final String schemaRegistryUrl,
        final InputStream propertiesFile
    ) {
      this.help = help;
      this.bootstrapServer = bootstrapServer;
      this.schemaFile = schemaFile;
      this.format = format;
      this.topicName = topicName;
      this.keyName = keyName;
      this.iterations = iterations;
      this.maxInterval = maxInterval;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.propertiesFile = propertiesFile;
    }

    static class ArgumentParseException extends RuntimeException {

      ArgumentParseException(final String message) {
        super(message);
      }
    }

    private static final class Builder {

      private Quickstart quickstart;

      private boolean help;
      private String bootstrapServer;
      private InputStream schemaFile;
      private Format format;
      private String topicName;
      private String keyName;
      private int iterations;
      private long maxInterval;
      private String schemaRegistryUrl;
      private InputStream propertiesFile;

      private Builder() {
        quickstart = null;
        help = false;
        bootstrapServer = "localhost:9092";
        schemaFile = null;
        format = null;
        topicName = null;
        keyName = null;
        iterations = 1000000;
        maxInterval = -1;
        schemaRegistryUrl = "http://localhost:8081";
        propertiesFile = null;
      }

      private enum Quickstart {
        CLICKSTREAM_CODES("clickstream_codes_schema.avro", "clickstream", "code"),
        CLICKSTREAM("clickstream_schema.avro", "clickstream", "ip"),
        CLICKSTREAM_USERS("clickstream_users_schema.avro", "webusers", "user_id"),
        ORDERS("orders_schema.avro", "orders", "orderid"),
        RATINGS("ratings_schema.avro", "ratings", "rating_id"),
        USERS("users_schema.avro", "users", "userid"),
        USERS_("users_array_map_schema.avro", "users", "userid"),
        PAGEVIEWS("pageviews_schema.avro", "pageviews", "viewtime");

        private final String schemaFileName;
        private final String rootTopicName;
        private final String keyName;

        Quickstart(final String schemaFileName, final String rootTopicName, final String keyName) {
          this.schemaFileName = schemaFileName;
          this.rootTopicName = rootTopicName;
          this.keyName = keyName;
        }

        public InputStream getSchemaFile() {
          return getClass().getClassLoader().getResourceAsStream(schemaFileName);
        }

        public String getTopicName(final Format format) {
          return String.format("%s_kafka_topic_%s", rootTopicName, format.name().toLowerCase());
        }

        public String getKeyName() {
          return keyName;
        }

        public Format getFormat() {
          return Format.JSON;
        }

      }

      Arguments build() {
        if (help) {
          return new Arguments(true, null, null, null, null, null, 0, -1, null, null);
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
        } catch (final NullPointerException exception) {
          throw new ArgumentParseException(exception.getMessage());
        }
        return new Arguments(
            help,
            bootstrapServer,
            schemaFile,
            format,
            topicName,
            keyName,
            iterations,
            maxInterval,
            schemaRegistryUrl,
            propertiesFile
        );
      }

      Builder parseArgs(final String[] args) throws IOException {
        for (final String arg : args) {
          parseArg(arg);
        }
        return this;
      }

      private Builder parseArg(final String arg) throws IOException {
        if ("help".equals(arg)) {
          help = true;
          return this;
        }

        final String[] splitOnEquals = arg.split("=");
        if (splitOnEquals.length != 2) {
          throw new ArgumentParseException(String.format(
              "Invalid argument format in '%s'; expected <name>=<value>",
              arg
          ));
        }

        final String argName = splitOnEquals[0].trim();
        final String argValue = splitOnEquals[1].trim();

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
            } catch (final IllegalArgumentException iae) {
              throw new ArgumentParseException(String.format(
                  "Invalid quickstart in '%s'; was expecting one of "
                  + Arrays.toString(Quickstart.values())
                  + " (case-insensitive)",
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
          case "maxInterval":
            maxInterval = parseIterations(argValue);
            break;
          case "schemaRegistryUrl":
            schemaRegistryUrl = argValue;
            break;
          case "propertiesFile":
            propertiesFile = new FileInputStream(argValue);
            break;
          default:
            throw new ArgumentParseException(String.format(
                "Unknown argument name in '%s'",
                argName
            ));
        }
        return this;
      }

      private Format parseFormat(final String formatString) {
        try {
          return Format.valueOf(formatString.toUpperCase());
        } catch (final IllegalArgumentException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid format in '%s'; was expecting one of AVRO, JSON, or DELIMITED "
              + "(case-insensitive)",
              formatString
          ));
        }
      }

      private int parseIterations(final String iterationsString) {
        try {
          final int result = Integer.valueOf(iterationsString, 10);
          if (result <= 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of iterations in '%d'; must be a positive number",
                result
            ));
          }
          return Integer.valueOf(iterationsString, 10);
        } catch (final NumberFormatException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid number of iterations in '%s'; must be a valid base 10 integer",
              iterationsString
          ));
        }
      }
    }
  }
}
