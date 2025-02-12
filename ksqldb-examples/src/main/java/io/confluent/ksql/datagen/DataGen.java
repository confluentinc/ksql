/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.datagen;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

@SuppressWarnings("UnstableApiUsage")
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
    } catch (final Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  static void run(final String... args) throws Throwable {
    final Arguments arguments = new Arguments.Builder()
        .parseArgs(args)
        .build();

    if (arguments.help) {
      usage();
      return;
    }

    final Properties props = getProperties(arguments);
    final DataGenProducer dataProducer = ProducerFactory
        .getProducer(arguments.keyFormat, arguments.valueFormat, arguments.valueDelimiter, props);
    final Optional<RateLimiter> rateLimiter = arguments.msgRate != -1
        ? Optional.of(RateLimiter.create(arguments.msgRate)) : Optional.empty();

    final Executor executor = Executors.newFixedThreadPool(
        arguments.numThreads,
        r -> {
          final Thread thread = new Thread(r);
          thread.setDaemon(true);
          return thread;
        }
    );
    final CompletionService<Void> service = new ExecutorCompletionService<>(executor);

    for (int i = 0; i < arguments.numThreads; i++) {
      service.submit(getProducerTask(arguments, dataProducer, props, rateLimiter));
    }
    for (int i = 0; i < arguments.numThreads; i++) {
      try {
        service.take().get();
      } catch (final InterruptedException e) {
        System.err.println("Interrupted waiting for threads to exit.");
        System.exit(1);
      } catch (final ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  private static Callable<Void> getProducerTask(
      final Arguments arguments,
      final DataGenProducer dataProducer,
      final Properties props,
      final Optional<RateLimiter> rateLimiter) throws IOException {
    final Generator generator = new Generator(arguments.schemaFile.get(), new Random());
    return () -> {
      dataProducer.populateTopic(
          props,
          generator,
          arguments.topicName,
          arguments.keyName,
          arguments.timestampColumnName != null
              ? Optional.of(arguments.timestampColumnName)
              : Optional.empty(),
          arguments.iterations,
          arguments.printRows,
          rateLimiter
      );
      return null;
    };
  }

  static Properties getProperties(final Arguments arguments) throws IOException {
    final Properties props = new Properties();
    props.put("bootstrap.servers", arguments.bootstrapServer);
    props.put("client.id", "KSQLDataGenProducer");
    props.put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, arguments.schemaRegistryUrl);

    if (arguments.propertiesFile != null) {
      props.load(arguments.propertiesFile);
    }

    return props;
  }

  private static void usage() {
    final String newLine = System.lineSeparator();
    System.err.println(
        "usage: DataGen " + newLine
            + "[help] " + newLine
            + "[bootstrap-server=<kafka bootstrap server(s)> (defaults to localhost:9092)] "
            + newLine
            + "[quickstart=<quickstart preset> (case-insensitive; one of 'orders', 'users', or "
            + "'pageviews')] " + newLine
            + "schema=<avro schema file> " + newLine
            + "[schemaRegistryUrl=<url for Confluent Schema Registry> "
            + "(defaults to http://localhost:8081)] " + newLine
            + "key-format=<message key format> (case-insensitive; one of 'avro', 'json', 'kafka' "
            + "or 'delimited') " + newLine
            + "value-format=<message value format> (case-insensitive; one of 'avro', 'json' or "
            + "'delimited') " + newLine
            + "valueDelimiter=<message value delimiter> (case-insensitive; delimiter to use when "
            + "using delimited value format. Must be a single character or special values 'SPACE' "
            + "or 'TAB'. Defaults to ',')" + newLine
            + "topic=<kafka topic name> " + newLine
            + "key=<name of key column> " + newLine
            + "timestamp=<name of timestamp column> " + newLine
            + "[iterations=<number of rows> (if no value is specified, datagen will produce "
            + "indefinitely)] " + newLine
            + "[propertiesFile=<file specifying Kafka client properties>] " + newLine
            + "[nThreads=<number of producer threads to start>] " + newLine
            + "[msgRate=<rate to produce in msgs/second>] " + newLine
            + "[printRows=<true|false>]" + newLine
    );
  }

  static class Arguments {

    private final boolean help;
    private final String bootstrapServer;
    private final Supplier<InputStream> schemaFile;
    private final Format keyFormat;
    private final Format valueFormat;
    private final String valueDelimiter;
    private final String topicName;
    private final String keyName;
    private final String timestampColumnName;
    private final int iterations;
    private final String schemaRegistryUrl;
    private final InputStream propertiesFile;
    private final int numThreads;
    private final int msgRate;
    private final boolean printRows;

    // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
    Arguments(
        final boolean help,
        final String bootstrapServer,
        final Supplier<InputStream> schemaFile,
        final Format keyFormat,
        final Format valueFormat,
        final String valueDelimiter,
        final String topicName,
        final String keyName,
        final String timestampColumnName,
        final int iterations,
        final String schemaRegistryUrl,
        final InputStream propertiesFile,
        final int numThreads,
        final int msgRate,
        final boolean printRows
    ) {
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      this.help = help;
      this.bootstrapServer = bootstrapServer;
      this.schemaFile = schemaFile;
      this.keyFormat = keyFormat;
      this.valueFormat = valueFormat;
      this.valueDelimiter = valueDelimiter;
      this.topicName = topicName;
      this.keyName = keyName;
      this.timestampColumnName = timestampColumnName;
      this.iterations = iterations;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.propertiesFile = propertiesFile;
      this.numThreads = numThreads;
      this.msgRate = msgRate;
      this.printRows = printRows;
    }

    static class ArgumentParseException extends RuntimeException {

      ArgumentParseException(final String message) {
        super(message);
      }
    }

    private static final class Builder {

      private static final Map<String, BiConsumer<Builder, String>> ARG_HANDLERS =
          ImmutableMap.<String, BiConsumer<Builder, String>>builder()
              .put("quickstart", (builder, argVal) -> builder.quickstart = parseQuickStart(argVal))
              .put("bootstrap-server", (builder, argVal) -> builder.bootstrapServer = argVal)
              .put("schema", (builder, argVal) -> builder.schemaFile = toFileInputStream(argVal))
              .put("key-format", (builder, arg) -> builder.keyFormat = parseFormat(arg))
              .put("value-format", (builder, arg) -> builder.valueFormat = parseFormat(arg))
              // "format" is maintained for backwards compatibility, but should be removed later.
              .put("format", (builder, argVal) -> builder.valueFormat = parseFormat(argVal))
              .put("valueDelimiter",
                  (builder, argVal) -> builder.valueDelimiter = parseValueDelimiter(argVal))
              .put("topic", (builder, argVal) -> builder.topicName = argVal)
              .put("key", (builder, argVal) -> builder.keyName = argVal)
              .put("timestamp", (builder, argVal) -> builder.timestampColumnName = argVal)
              .put("iterations", (builder, argVal) -> builder.iterations = parseInt(argVal, 1))
              .put("maxInterval", (builder, argVal) -> printMaxIntervalIsDeprecatedMessage())
              .put("schemaRegistryUrl", (builder, argVal) -> builder.schemaRegistryUrl = argVal)
              .put("propertiesFile",
                  (builder, argVal) -> builder.propertiesFile = toFileInputStream(argVal).get())
              .put("msgRate", (builder, argVal) -> builder.msgRate = parseInt(argVal, 1))
              .put("nThreads", (builder, argVal) -> builder.numThreads = parseNumThreads(argVal))
              .put("printRows", (builder, argVal) -> builder.printRows = parsePrintRows(argVal))
              .build();

      private static void printMaxIntervalIsDeprecatedMessage() {
        System.err.println("*maxInterval* parameter is *DEPRECATED*");
        System.err.println("the value will be ignored "
                               + "and parameter will be removed in future releases");
        System.err.println("Please use *msgRate* parameter to adjust sending message frequency");
        System.err.flush();
      }

      private Quickstart quickstart;

      private boolean help;
      private String bootstrapServer;
      private Supplier<InputStream> schemaFile;
      private Format keyFormat;
      private Format valueFormat;
      private String valueDelimiter;
      private String topicName;
      private String keyName;
      private String timestampColumnName;
      private int iterations;
      private String schemaRegistryUrl;
      private InputStream propertiesFile;
      private int msgRate;
      private int numThreads;
      private boolean printRows;

      private Builder() {
        quickstart = null;
        help = false;
        bootstrapServer = "localhost:9092";
        schemaFile = null;
        keyFormat = FormatFactory.KAFKA;
        valueFormat = null;
        valueDelimiter = null;
        topicName = null;
        keyName = null;
        timestampColumnName = null;
        iterations = -1;
        schemaRegistryUrl = "http://localhost:8081";
        propertiesFile = null;
        msgRate = -1;
        numThreads = 1;
        printRows = true;
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

        public Supplier<InputStream> getSchemaFile() {
          return () -> getClass().getClassLoader().getResourceAsStream(schemaFileName);
        }

        public String getTopicName(final Format format) {
          return String.format("%s_kafka_topic_%s", rootTopicName, format.name().toLowerCase());
        }

        public String getKeyName() {
          return keyName;
        }
      }

      Arguments build() {
        if (help) {
          return new Arguments(
              true,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              0,
              null,
              null,
              1,
              -1,
              true
          );
        }

        if (quickstart != null) {
          schemaFile = Optional.ofNullable(schemaFile).orElse(quickstart.getSchemaFile());
          keyFormat = Optional.ofNullable(keyFormat).orElse(FormatFactory.KAFKA);
          valueFormat = Optional.ofNullable(valueFormat).orElse(FormatFactory.JSON);
          topicName = Optional.ofNullable(topicName).orElse(quickstart.getTopicName(valueFormat));
          keyName = Optional.ofNullable(keyName).orElse(quickstart.getKeyName());
          timestampColumnName = Optional.ofNullable(timestampColumnName).orElse(null);
        }

        if (schemaFile == null) {
          throw new ArgumentParseException("Schema file not provided");
        }
        if (keyFormat == null) {
          throw new ArgumentParseException("Message key format not provided");
        }
        if (valueFormat == null) {
          throw new ArgumentParseException("Message value format not provided");
        }
        if (topicName == null) {
          throw new ArgumentParseException("Kafka topic name not provided");
        }
        if (keyName == null) {
          throw new ArgumentParseException("Name of key column not provided");
        }
        return new Arguments(
            help,
            bootstrapServer,
            schemaFile,
            keyFormat,
            valueFormat,
            valueDelimiter,
            topicName,
            keyName,
            timestampColumnName,
            iterations,
            schemaRegistryUrl,
            propertiesFile,
            numThreads,
            msgRate,
            printRows
        );
      }

      Builder parseArgs(final String[] args) {
        for (final String arg : args) {
          parseArg(arg);
        }
        return this;
      }

      private void parseArg(final String arg) {
        if ("help".equals(arg) || "--help".equals(arg)) {
          help = true;
          return;
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

        setArg(argName, argValue);
      }

      private void setArg(final String argName, final String argVal) {
        final BiConsumer<Builder, String> handler = ARG_HANDLERS.get(argName);
        if (handler == null) {
          throw new ArgumentParseException(String.format(
              "Unknown argument name in '%s'",
              argName
          ));
        }

        handler.accept(this, argVal);
      }

      private static Supplier<InputStream> toFileInputStream(final String argVal) {
        return () -> {
          try {
            return new FileInputStream(argVal);
          } catch (final FileNotFoundException e) {
            throw new IllegalArgumentException("File not found: " + argVal, e);
          }
        };
      }

      private static Quickstart parseQuickStart(final String argValue) {
        try {
          return Quickstart.valueOf(argValue.toUpperCase());
        } catch (final IllegalArgumentException iae) {
          throw new ArgumentParseException(String.format(
              "Invalid quickstart in '%s'; was expecting one of "
              + Arrays.toString(Quickstart.values())
              + " (case-insensitive)",
              argValue
          ));
        }
      }

      private static Format parseFormat(final String formatString) {
        try {
          return FormatFactory.of(FormatInfo.of(formatString));
        } catch (final KsqlException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid format in '%s'; was expecting one of AVRO, JSON, KAFKA or DELIMITED "
              + "(case-insensitive)",
              formatString
          ));
        }
      }

      private static String parseValueDelimiter(final String valueDelimiterString) {
        if (valueDelimiterString == null) {
          return null;
        } else {
          if (!(valueDelimiterString.length() == 1 || valueDelimiterString.equals("TAB")
              || valueDelimiterString.equals("SPACE"))) {
            throw new ArgumentParseException(String.format(
                "Invalid value_delimiter; was expecting a single character, 'TAB', or "
                    + "'SPACE', got '%s'",
                valueDelimiterString
            ));
          }
          return valueDelimiterString;
        }
      }

      private static int parseNumThreads(final String numThreadsString) {
        try {
          final int result = Integer.valueOf(numThreadsString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of threads in '%d'; must be a positive number",
                result));
          }
          return result;
        } catch (final NumberFormatException e) {
          throw new ArgumentParseException(String.format(
              "Invalid number of threads in '%s'; must be a positive number",
              numThreadsString));
        }
      }

      private static boolean parsePrintRows(final String printRowsString) {
        switch (printRowsString.toLowerCase()) {
          case "false":
            return false;
          case "true":
            return true;
          default:
            throw new ArgumentParseException(String.format(
                "Invalid value for printRows in '%s'; must be true or false",
                printRowsString
            ));
        }
      }

      private static int parseInt(final String iterationsString, final int minValue) {
        try {
          final int result = Integer.valueOf(iterationsString, 10);
          if (result < minValue) {
            throw new ArgumentParseException(String.format(
                "Invalid integer value '%d'; must be >= %d",
                result, minValue
            ));
          }
          return Integer.valueOf(iterationsString, 10);
        } catch (final NumberFormatException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid integer value '%s'; must be a valid base 10 integer",
              iterationsString
          ));
        }
      }
    }
  }
}
