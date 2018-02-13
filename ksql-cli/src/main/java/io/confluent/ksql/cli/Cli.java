/**
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

package io.confluent.ksql.cli;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.cli.console.CliSpecificCommand;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.util.CommonUtils;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Version;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Cli implements Closeable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cli.class);

  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);

  private final ExecutorService queryStreamExecutorService;

  private final Long streamedQueryRowLimit;
  private final Long streamedQueryTimeoutMs;

  final KsqlRestClient restClient;
  private final Console terminal;

  public Cli(
      Long streamedQueryRowLimit,
      Long streamedQueryTimeoutMs,
      KsqlRestClient restClient,
      Console terminal
  ) {
    Objects.requireNonNull(restClient, "Must provide the CLI with a REST client");
    Objects.requireNonNull(terminal, "Must provide the CLI with a terminal");

    this.streamedQueryRowLimit = streamedQueryRowLimit;
    this.streamedQueryTimeoutMs = streamedQueryTimeoutMs;
    this.restClient = restClient;
    this.terminal = terminal;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();
  }

  public void runInteractively() {
    displayWelcomeMessage();
    boolean eof = false;
    while (!eof) {
      try {
        handleLine(readLine());
      } catch (EndOfFileException exception) {
        // EOF is fine, just terminate the REPL
        terminal.writer().println("Exiting KSQL.");
        eof = true;
      } catch (Exception exception) {
        LOGGER.error(ExceptionUtils.getStackTrace(exception));
        if (exception.getMessage() != null) {
          terminal.writer().println(exception.getMessage());
        } else {
          terminal.writer().println(exception.getClass().getName());
          // TODO: Maybe ask the user if they'd like to see the stack trace here?
        }
        String causeMsg = CommonUtils.getErrorCauseMessage(exception);
        if (!causeMsg.isEmpty()) {
          terminal.writer().println(causeMsg);
        }
      }
      terminal.flush();
    }
  }

  private void displayWelcomeMessage() {
    String serverVersion;
    try {
      serverVersion = restClient.getServerInfo().getResponse().getVersion();
    } catch (Exception exception) {
      serverVersion = "<unknown>";
    }
    String cliVersion = Version.getVersion();

    /*
        Should look like:
                            =================================
                            =   _  __ _____  ____  _        =
                            =  | |/ // ____|/ __ \| |       =
                            =  | ' /| (___ | |  | | |       =
                            =  |  <  \___ \| |  | | |       =
                            =  | . \ ____) | |__| | |____   =
                            =  |_|\_\_____/ \___\_\______|  =
                            =                               =
                            == Kafka Streams Query Language =
                              Copyright 2017 Confluent Inc.
        CLI v1.0.0, Server v1.0.0 located at http://localhost:9098
        <help message reminder>
        Text generated via http://www.network-science.de/ascii/, with the "big" font
     */
    int logoWidth = 33;
    String copyrightMessage = "Copyright 2017 Confluent Inc.";
    String helpReminderMessage = "Having trouble? "
                                 + "Type 'help' (case-insensitive) for a rundown of how things "
                                 + "work!";
    // Don't want to display the logo if it'll just end up getting wrapped and looking hideous
    if (terminal.getWidth() >= logoWidth) {
      // Want to center the logo, but in the case of something like a fullscreen terminal, just
      // centering around the help message (longest single line of text in the welcome message)
      // should be enough; looks a little weird if you try to center the logo on a wide enough
      // screen and it just kind of ends up out in the middle of nowhere; hence, the call to
      // Math.min(terminal.getWidth(), helpReminderMessage.length())
      int paddedLogoWidth = Math.min(terminal.getWidth(), helpReminderMessage.length());
      int paddingWidth = (paddedLogoWidth - logoWidth) / 2;
      String leftPadding = new String(
          new byte[paddingWidth],
          StandardCharsets.UTF_8
      ).replaceAll(".", " ");
      terminal.writer().printf("%s======================================%n", leftPadding);
      terminal.writer().printf("%s=      _  __ _____  ____  _          =%n", leftPadding);
      terminal.writer().printf("%s=     | |/ // ____|/ __ \\| |         =%n", leftPadding);
      terminal.writer().printf("%s=     | ' /| (___ | |  | | |         =%n", leftPadding);
      terminal.writer().printf("%s=     |  <  \\___ \\| |  | | |         =%n", leftPadding);
      terminal.writer().printf("%s=     | . \\ ____) | |__| | |____     =%n", leftPadding);
      terminal.writer().printf("%s=     |_|\\_\\_____/ \\___\\_\\______|    =%n", leftPadding);
      terminal.writer().printf("%s=                                    =%n", leftPadding);
      terminal.writer().printf("%s=   Streaming SQL Engine for Kafka   =%n", leftPadding);
      terminal.writer().printf("%s  %s%n", copyrightMessage, leftPadding);
    } else {
      terminal.writer().printf("KSQL, %s%n", copyrightMessage);
    }
    terminal.writer().println();
    terminal.writer().printf(
        "CLI v%s, Server v%s located at %s%n",
        cliVersion,
        serverVersion,
        restClient.getServerAddress()
    );
    terminal.writer().println();
    terminal.writer().println(helpReminderMessage);
    terminal.writer().println();
    terminal.flush();

  }

  public void runNonInteractively(String input) throws Exception {
    // Allow exceptions to halt execution of the Ksql script as soon as the first one is encountered
    for (String logicalLine : getLogicalLines(input)) {
      try {
        handleLine(logicalLine);
      } catch (EndOfFileException exception) {
        // Swallow these silently; they're thrown by the exit command to terminate the REPL
        return;
      }
    }
  }

  private List<String> getLogicalLines(String input) {
    // TODO: Convert the input string into an InputStream, then feed it to the terminal via
    // TerminalBuilder.streams(InputStream, OutputStream)
    List<String> result = new ArrayList<>();
    StringBuilder logicalLine = new StringBuilder();
    for (String physicalLine : input.split("\n")) {
      if (!physicalLine.trim().isEmpty()) {
        if (physicalLine.endsWith("\\")) {
          logicalLine.append(physicalLine.substring(0, physicalLine.length() - 1));
        } else {
          result.add(logicalLine.append(physicalLine).toString().trim());
          logicalLine = new StringBuilder();
        }
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    queryStreamExecutorService.shutdownNow();
    restClient.close();
    terminal.close();
  }

  public void handleLine(String line) throws Exception {
    String trimmedLine = Optional.ofNullable(line).orElse("").trim();

    if (trimmedLine.isEmpty()) {
      return;
    }

    String[] commandArgs = trimmedLine.split("\\s+", 2);
    CliSpecificCommand cliSpecificCommand =
        terminal.getCliSpecificCommands().get(commandArgs[0].toLowerCase());
    if (cliSpecificCommand != null) {
      cliSpecificCommand.execute(commandArgs.length > 1 ? commandArgs[1] : "");
    } else {
      handleStatements(line);
    }
  }

  /**
   * Attempt to read a logical line of input from the user. Can span multiple physical lines, as
   * long as all but the last end with '\\'.
   *
   * @return The parsed, logical line.
   * @throws EndOfFileException If there is no more input available from the user.
   * @throws IOException        If any other I/O error occurs.
   */
  private String readLine() throws IOException {
    while (true) {
      try {
        String result = terminal.getLineReader().readLine();
        // A 'dumb' terminal (the kind used at runtime if a 'system' terminal isn't available) will
        // return null on EOF and user interrupt, instead of throwing the more fine-grained
        // exceptions. This null-check helps ensure that, upon encountering EOF, even a 'dumb'
        // terminal will be able to exit intelligently.
        if (result == null) {
          throw new EndOfFileException();
        } else {
          return result.trim();
        }
      } catch (UserInterruptException exception) {
        // User hit ctrl-C, just clear the current line and try again.
        terminal.writer().println("^C");
        terminal.flush();
      }
    }
  }

  private void handleStatements(String line)
      throws IOException, InterruptedException, ExecutionException {
    StringBuilder consecutiveStatements = new StringBuilder();
    for (SqlBaseParser.SingleStatementContext statementContext :
        new KsqlParser().getStatements(line)
        ) {
      String statementText = KsqlEngine.getStatementString(statementContext);
      if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext
          || statementContext.statement() instanceof SqlBaseParser.PrintTopicContext) {
        consecutiveStatements = printOrDisplayQueryResults(
            consecutiveStatements,
            statementContext,
            statementText
        );

      } else if (statementContext.statement() instanceof SqlBaseParser.ListPropertiesContext) {
        listProperties(statementText);

      } else if (statementContext.statement() instanceof SqlBaseParser.SetPropertyContext) {
        setProperty(statementContext);

      } else if (statementContext.statement() instanceof SqlBaseParser.UnsetPropertyContext) {
        consecutiveStatements = unsetProperty(consecutiveStatements, statementContext);
      } else if (statementContext.statement() instanceof SqlBaseParser.RunScriptContext) {
        runScript(statementContext, statementText);
      } else if (statementContext.statement() instanceof SqlBaseParser.RegisterTopicContext) {
        registerTopic(consecutiveStatements, statementContext, statementText);
      } else {
        consecutiveStatements.append(statementText);
      }
    }
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(
          restClient.makeKsqlRequest(consecutiveStatements.toString())
      );
    }
  }

  private void registerTopic(
      StringBuilder consecutiveStatements,
      SqlBaseParser.SingleStatementContext statementContext,
      String statementText
  ) {
    CliUtils cliUtils = new CliUtils();
    Optional<String> avroSchema = cliUtils.getAvroSchemaIfAvroTopic(
        (SqlBaseParser.RegisterTopicContext) statementContext.statement());
    if (avroSchema.isPresent()) {
      setProperty(DdlConfig.AVRO_SCHEMA, avroSchema.get());
    }
    consecutiveStatements.append(statementText);
  }

  private void runScript(
      SqlBaseParser.SingleStatementContext statementContext,
      String statementText
  ) throws IOException {
    SqlBaseParser.RunScriptContext runScriptContext =
        (SqlBaseParser.RunScriptContext) statementContext.statement();
    String schemaFilePath = AstBuilder.unquote(runScriptContext.STRING().getText(), "'");
    String fileContent;
    try {
      fileContent = new String(
          Files.readAllBytes(Paths.get(schemaFilePath)),
          StandardCharsets.UTF_8
      );
    } catch (IOException e) {
      throw new KsqlException(
          " Could not read statements from file: " + schemaFilePath + ". " + "Details: "
          + e.getMessage(),
          e
      );
    }
    setProperty(DdlConfig.RUN_SCRIPT_STATEMENTS_CONTENT, fileContent);
    printKsqlResponse(
        restClient.makeKsqlRequest(statementText)
    );
  }

  private StringBuilder unsetProperty(
      StringBuilder consecutiveStatements,
      SqlBaseParser.SingleStatementContext statementContext
  ) throws IOException {
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(
          restClient.makeKsqlRequest(consecutiveStatements.toString())
      );
      consecutiveStatements = new StringBuilder();
    }
    SqlBaseParser.UnsetPropertyContext unsetPropertyContext =
        (SqlBaseParser.UnsetPropertyContext) statementContext.statement();
    String property = AstBuilder.unquote(unsetPropertyContext.STRING().getText(), "'");
    unsetProperty(property);
    return consecutiveStatements;
  }

  private StringBuilder printOrDisplayQueryResults(
      StringBuilder consecutiveStatements,
      SqlBaseParser.SingleStatementContext statementContext,
      String statementText
  ) throws IOException, InterruptedException, ExecutionException {
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(
          restClient.makeKsqlRequest(consecutiveStatements.toString())
      );
      consecutiveStatements = new StringBuilder();
    }
    if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
      handleStreamedQuery(statementText);
    } else {
      handlePrintedTopic(statementText);
    }
    return consecutiveStatements;
  }

  private void setProperty(SqlBaseParser.SingleStatementContext statementContext) {
    SqlBaseParser.SetPropertyContext setPropertyContext =
        (SqlBaseParser.SetPropertyContext) statementContext.statement();
    String property = AstBuilder.unquote(setPropertyContext.STRING(0).getText(), "'");
    String value = AstBuilder.unquote(setPropertyContext.STRING(1).getText(), "'");
    setProperty(property, value);
  }

  private void listProperties(String statementText) throws IOException {
    KsqlEntityList ksqlEntityList = restClient.makeKsqlRequest(statementText).getResponse();
    PropertiesList propertiesList = (PropertiesList) ksqlEntityList.get(0);
    propertiesList.getProperties().putAll(restClient.getLocalProperties());
    terminal.printKsqlEntityList(
        Collections.singletonList(propertiesList)
    );
  }

  private void printKsqlResponse(RestResponse<KsqlEntityList> response) throws IOException {
    if (response.isSuccessful()) {
      KsqlEntityList ksqlEntities = response.getResponse();
      boolean noErrorFromServer = true;
      for (KsqlEntity entity : ksqlEntities) {
        if (entity instanceof ErrorMessageEntity) {
          ErrorMessageEntity errorMsg = (ErrorMessageEntity) entity;
          terminal.printErrorMessage(errorMsg.getErrorMessage());
          LOGGER.error(errorMsg.getErrorMessage().getMessage());
          noErrorFromServer = false;
        } else if (entity instanceof CommandStatusEntity
            && (
            ((CommandStatusEntity) entity).getCommandStatus().getStatus()
                == CommandStatus.Status.ERROR
        )) {
          String fullMessage = ((CommandStatusEntity) entity).getCommandStatus().getMessage();
          terminal.printError(fullMessage.split("\n")[0], fullMessage);
          noErrorFromServer = false;
        }
      }
      if (noErrorFromServer) {
        terminal.printKsqlEntityList(response.getResponse());
      }
    } else {
      terminal.printErrorMessage(response.getErrorMessage());
    }
  }

  private void handleStreamedQuery(String query)
      throws InterruptedException, ExecutionException {
    RestResponse<KsqlRestClient.QueryStream> queryResponse =
        restClient.makeQueryRequest(query);

    if (queryResponse.isSuccessful()) {
      try (KsqlRestClient.QueryStream queryStream = queryResponse.getResponse()) {
        Future<?> queryStreamFuture = queryStreamExecutorService.submit(new Runnable() {
          @Override
          public void run() {
            for (long rowsRead = 0; keepReading(rowsRead) && queryStream.hasNext(); rowsRead++) {
              try {
                StreamedRow row = queryStream.next();
                terminal.printStreamedRow(row);
                if (row.getErrorMessage() != null) {
                  // got an error in the stream, which means we have reached the end.
                  // the stream interface that queryStream uses isn't smart enough to figure
                  // out when the socket is closed, so just break here since we know there will
                  // be nothing more to read.
                  break;
                }
              } catch (IOException exception) {
                throw new RuntimeException(exception);
              }
            }
          }
        });

        terminal.handle(Terminal.Signal.INT, signal -> {
          terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
          queryStreamFuture.cancel(true);
        });

        try {
          if (streamedQueryTimeoutMs == null) {
            queryStreamFuture.get();
            Thread.sleep(1000); // TODO: Make things work without this
          } else {
            try {
              queryStreamFuture.get(streamedQueryTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException exception) {
              queryStreamFuture.cancel(true);
            }
          }
        } catch (CancellationException exception) {
          // It's fine
        }
      } finally {
        terminal.writer().println("Query terminated");
        terminal.flush();
      }
    } else {
      terminal.printErrorMessage(queryResponse.getErrorMessage());
    }
  }

  private boolean keepReading(long rowsRead) {
    return streamedQueryRowLimit == null || rowsRead < streamedQueryRowLimit;
  }

  private void handlePrintedTopic(String printTopic)
      throws InterruptedException, ExecutionException, IOException {
    RestResponse<InputStream> topicResponse =
        restClient.makePrintTopicRequest(printTopic);

    if (topicResponse.isSuccessful()) {
      try (Scanner topicStreamScanner = new Scanner(
          topicResponse.getResponse(),
          StandardCharsets.UTF_8.name()
      )) {
        Future<?> topicPrintFuture = queryStreamExecutorService.submit(() -> {
          while (topicStreamScanner.hasNextLine()) {
            String line = topicStreamScanner.nextLine();
            if (!line.isEmpty()) {
              terminal.writer().println(line);
              terminal.flush();
            }
          }
        });

        terminal.handle(Terminal.Signal.INT, signal -> {
          terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
          topicPrintFuture.cancel(true);
        });

        try {
          topicPrintFuture.get();
        } catch (CancellationException exception) {
          terminal.writer().println("Topic printing ceased");
          terminal.flush();
        }
        topicResponse.getResponse().close();
      }
    } else {
      terminal.writer().println(topicResponse.getErrorMessage().getMessage());
      terminal.flush();
    }
  }

  private void setProperty(String property, String value) {
    String parsedProperty;
    ConfigDef.Type type;
    if (StreamsConfig.configDef().configKeys().containsKey(property)) {
      type = StreamsConfig.configDef().configKeys().get(property).type;
      parsedProperty = property;
    } else if (CONSUMER_CONFIG_DEF.configKeys().containsKey(property)) {
      type = CONSUMER_CONFIG_DEF.configKeys().get(property).type;
      parsedProperty = property;
    } else if (PRODUCER_CONFIG_DEF.configKeys().containsKey(property)) {
      type = PRODUCER_CONFIG_DEF.configKeys().get(property).type;
      parsedProperty = property;
    } else if (property.startsWith(StreamsConfig.CONSUMER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.CONSUMER_PREFIX.length());
      ConfigDef.ConfigKey configKey = CONSUMER_CONFIG_DEF.configKeys().get(parsedProperty);
      if (configKey == null) {
        throw new IllegalArgumentException(String.format(
            "Invalid consumer property: '%s'",
            parsedProperty
        ));
      }
      type = configKey.type;
    } else if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.PRODUCER_PREFIX.length());
      ConfigDef.ConfigKey configKey = PRODUCER_CONFIG_DEF.configKeys().get(parsedProperty);
      if (configKey == null) {
        throw new IllegalArgumentException(String.format(
            "Invalid producer property: '%s'",
            parsedProperty
        ));
      }
      type = configKey.type;
    } else if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)) {
      restClient.setProperty(property, value);
      return;
    } else if (property.equalsIgnoreCase(DdlConfig.SCHEMA_FILE_CONTENT_PROPERTY)) {
      restClient.setProperty(property, value);
      return;
    } else if (property.equalsIgnoreCase(DdlConfig.RUN_SCRIPT_STATEMENTS_CONTENT)) {
      restClient.setProperty(property, value);
      return;
    } else if (property.equalsIgnoreCase(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)) {
      restClient.setProperty(property, value);
      return;
    } else {
      throw new IllegalArgumentException(String.format(
          "Not recognizable as streams, consumer, or producer property: '%s'",
          property
      ));
    }

    if (KsqlEngine.getImmutableProperties().contains(parsedProperty)) {
      throw new IllegalArgumentException(String.format(
          "Cannot override property '%s'",
          property
      ));
    }

    Object parsedValue = ConfigDef.parseType(parsedProperty, value, type);
    Object priorValue = restClient.setProperty(property, parsedValue);

    terminal.writer().printf(
        "Successfully changed local property '%s' from '%s' to '%s'%n",
        property,
        priorValue,
        parsedValue
    );
    terminal.flush();
  }

  private void unsetProperty(String property) {
    if (restClient.unsetProperty(property)) {
      Object value = restClient.getLocalProperties().get(property);
      terminal.writer().printf(
          "Successfully unset local property '%s' (value was '%s')%n",
          property,
          value
      );
    } else {
      throw new IllegalArgumentException(String.format(
          "Cannot unset local property '%s' which was never set in the first place",
          property
      ));
    }
  }

  // It seemed like a good idea at the time
  private static ConfigDef getConfigDef(Class<? extends AbstractConfig> classs) {
    try {
      java.lang.reflect.Field field = classs.getDeclaredField("CONFIG");
      field.setAccessible(true);
      return (ConfigDef) field.get(null);
    } catch (Exception exception) {
      // uhhh...
      // TODO
      return null;
    }
  }
}
