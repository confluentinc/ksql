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

package io.confluent.ksql.cli;

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommand;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cli implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cli.class);

  private final ExecutorService queryStreamExecutorService;

  private final Long streamedQueryRowLimit;
  private final Long streamedQueryTimeoutMs;

  private final KsqlRestClient restClient;
  private final Console terminal;

  public static Cli build(
      final Long streamedQueryRowLimit,
      final Long streamedQueryTimeoutMs,
      final OutputFormat outputFormat,
      final KsqlRestClient restClient
  ) {
    final Console console = Console.build(outputFormat, restClient);
    return new Cli(streamedQueryRowLimit, streamedQueryTimeoutMs, restClient, console);
  }

  Cli(
      final Long streamedQueryRowLimit,
      final Long streamedQueryTimeoutMs,
      final KsqlRestClient restClient,
      final Console terminal
  ) {
    Objects.requireNonNull(restClient, "Must provide the CLI with a REST client");
    Objects.requireNonNull(terminal, "Must provide the CLI with a terminal");

    this.streamedQueryRowLimit = streamedQueryRowLimit;
    this.streamedQueryTimeoutMs = streamedQueryTimeoutMs;
    this.restClient = restClient;
    this.terminal = terminal;
    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    terminal
        .registerCliSpecificCommand(new RemoteServerSpecificCommand(restClient, terminal.writer()));
  }

  public void runInteractively() {
    displayWelcomeMessage();
    RemoteServerSpecificCommand.validateClient(terminal.writer(), restClient);
    boolean eof = false;
    while (!eof) {
      try {
        handleLine(readLine());
      } catch (final EndOfFileException exception) {
        // EOF is fine, just terminate the REPL
        terminal.writer().println("Exiting KSQL.");
        eof = true;
      } catch (final Exception exception) {
        LOGGER.error("", exception);
        terminal.writer().println(ErrorMessageUtil.buildErrorMessage(exception));
      }
      terminal.flush();
    }
  }

  private void displayWelcomeMessage() {
    String serverVersion;
    try {
      serverVersion = restClient.getServerInfo().getResponse().getVersion();
    } catch (final Exception exception) {
      serverVersion = "<unknown>";
    }
    final String cliVersion = Version.getVersion();

    final String helpReminderMessage =
        "Having trouble? "
        + "Type 'help' (case-insensitive) for a rundown of how things work!";

    final PrintWriter writer = terminal.writer();

    // Want to center the logo, but in the case of something like a fullscreen terminal, just
    // centering around the help message (longest single line of text in the welcome message)
    // should be enough; looks a little weird if you try to center the logo on a wide enough
    // screen and it just kind of ends up out in the middle of nowhere; hence, the call to
    // Math.min(terminal.getWidth(), helpReminderMessage.length())
    final int consoleWidth = Math.min(terminal.getWidth(), helpReminderMessage.length());

    WelcomeMsgUtils.displayWelcomeMessage(consoleWidth, writer);

    writer.printf(
        "CLI v%s, Server v%s located at %s%n",
        cliVersion,
        serverVersion,
        restClient.getServerAddress()
    );
    writer.println();
    writer.println(helpReminderMessage);
    writer.println();
    terminal.flush();
  }

  @Override
  public void close() {
    queryStreamExecutorService.shutdownNow();
    terminal.close();
  }

  void handleLine(final String line) throws Exception {
    final String trimmedLine = Optional.ofNullable(line).orElse("").trim();
    if (trimmedLine.isEmpty()) {
      return;
    }

    handleStatements(line);
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
        final String result = terminal.readLine();
        // A 'dumb' terminal (the kind used at runtime if a 'system' terminal isn't available) will
        // return null on EOF and user interrupt, instead of throwing the more fine-grained
        // exceptions. This null-check helps ensure that, upon encountering EOF, even a 'dumb'
        // terminal will be able to exit intelligently.
        if (result == null) {
          throw new EndOfFileException();
        } else {
          return result.trim();
        }
      } catch (final UserInterruptException exception) {
        // User hit ctrl-C, just clear the current line and try again.
        terminal.writer().println("^C");
        terminal.flush();
      }
    }
  }

  private void handleStatements(final String line)
      throws InterruptedException, IOException, ExecutionException {

    final List<ParsedStatement> statements =
        new KsqlParser().getStatements(line);

    StringBuilder consecutiveStatements = new StringBuilder();
    for (final ParsedStatement statement : statements) {
      final SingleStatementContext statementContext = statement.getStatement();
      final String statementText = statement.getStatementText();

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
      final StringBuilder consecutiveStatements,
      final SqlBaseParser.SingleStatementContext statementContext,
      final String statementText
  ) {
    final CliUtils cliUtils = new CliUtils();
    final Optional<String> avroSchema = cliUtils.getAvroSchemaIfAvroTopic(
        (SqlBaseParser.RegisterTopicContext) statementContext.statement());
    avroSchema.ifPresent(s -> setProperty(DdlConfig.AVRO_SCHEMA, s));
    consecutiveStatements.append(statementText);
  }

  private void runScript(
      final SqlBaseParser.SingleStatementContext statementContext,
      final String statementText
  ) throws IOException {
    final SqlBaseParser.RunScriptContext runScriptContext =
        (SqlBaseParser.RunScriptContext) statementContext.statement();
    final String schemaFilePath = AstBuilder.unquote(runScriptContext.STRING().getText(), "'");
    final String fileContent;
    try {
      fileContent = new String(
          Files.readAllBytes(Paths.get(schemaFilePath)),
          StandardCharsets.UTF_8
      );
    } catch (final IOException e) {
      throw new KsqlException(
          " Could not read statements from the provided script file " + schemaFilePath + ": "
          + e + " Make sure the file exists and can be read by KSQL CLI.",
          e
      );
    }
    setProperty(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT, fileContent);
    printKsqlResponse(
        restClient.makeKsqlRequest(statementText)
    );
  }

  private StringBuilder printOrDisplayQueryResults(
      final StringBuilder consecutiveStatements,
      final SqlBaseParser.SingleStatementContext statementContext,
      final String statementText
  ) throws InterruptedException, IOException, ExecutionException {
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(restClient.makeKsqlRequest(consecutiveStatements.toString()));
      consecutiveStatements.setLength(0);
    }
    if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
      handleStreamedQuery(statementText);
    } else {
      handlePrintedTopic(statementText);
    }
    return consecutiveStatements;
  }

  private void listProperties(final String statementText) throws IOException {
    final KsqlEntityList ksqlEntityList = restClient.makeKsqlRequest(statementText).getResponse();
    terminal.printKsqlEntityList(ksqlEntityList);
  }

  private void printKsqlResponse(final RestResponse<KsqlEntityList> response) throws IOException {
    if (response.isSuccessful()) {
      final KsqlEntityList ksqlEntities = response.getResponse();
      boolean noErrorFromServer = true;
      for (final KsqlEntity entity : ksqlEntities) {
        if (entity instanceof CommandStatusEntity
            && (
            ((CommandStatusEntity) entity).getCommandStatus().getStatus()
                == CommandStatus.Status.ERROR)
        ) {
          final String fullMessage = ((CommandStatusEntity) entity).getCommandStatus().getMessage();
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

  private void handleStreamedQuery(final String query)
      throws InterruptedException, ExecutionException, IOException {
    final RestResponse<KsqlRestClient.QueryStream> queryResponse =
        restClient.makeQueryRequest(query);

    LOGGER.debug("Handling streamed query");

    if (queryResponse.isSuccessful()) {
      try (KsqlRestClient.QueryStream queryStream = queryResponse.getResponse()) {
        final Future<?> queryStreamFuture = queryStreamExecutorService.submit(() -> {
          for (long rowsRead = 0; keepReading(rowsRead) && queryStream.hasNext(); rowsRead++) {
            try {
              final StreamedRow row = queryStream.next();
              terminal.printStreamedRow(row);
              if (row.getFinalMessage() != null || row.getErrorMessage() != null) {
                break;
              }
            } catch (final IOException exception) {
              throw new RuntimeException(exception);
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
            } catch (final TimeoutException exception) {
              queryStreamFuture.cancel(true);
            }
          }
        } catch (final CancellationException exception) {
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

  private boolean keepReading(final long rowsRead) {
    return streamedQueryRowLimit == null || rowsRead < streamedQueryRowLimit;
  }

  private void handlePrintedTopic(final String printTopic)
      throws InterruptedException, ExecutionException, IOException {
    final RestResponse<InputStream> topicResponse =
        restClient.makePrintTopicRequest(printTopic);

    if (topicResponse.isSuccessful()) {
      try (Scanner topicStreamScanner = new Scanner(
          topicResponse.getResponse(),
          StandardCharsets.UTF_8.name()
      )) {
        final Future<?> topicPrintFuture = queryStreamExecutorService.submit(() -> {
          while (!Thread.currentThread().isInterrupted() && topicStreamScanner.hasNextLine()) {
            final String line = topicStreamScanner.nextLine();
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
        } catch (final CancellationException exception) {
          topicResponse.getResponse().close();
          terminal.writer().println("Topic printing ceased");
          terminal.flush();
        }
      }
    } else {
      terminal.writer().println(topicResponse.getErrorMessage().getMessage());
      terminal.flush();
    }
  }

  private void setProperty(final SqlBaseParser.SingleStatementContext statementContext) {
    final SqlBaseParser.SetPropertyContext setPropertyContext =
        (SqlBaseParser.SetPropertyContext) statementContext.statement();
    final String property = AstBuilder.unquote(setPropertyContext.STRING(0).getText(), "'");
    final String value = AstBuilder.unquote(setPropertyContext.STRING(1).getText(), "'");
    setProperty(property, value);
  }

  private void setProperty(final String property, final String value) {
    final Object priorValue = restClient.setProperty(property, value);

    if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)
        || property.equalsIgnoreCase(KsqlConstants.RUN_SCRIPT_STATEMENTS_CONTENT)) {

      // Don't output.
      return;
    }

    terminal.writer().printf(
        "Successfully changed local property '%s'%s to '%s'.%s%n",
        property,
        priorValue == null ? "" : " from '" + priorValue + "'",
        value,
        priorValue == null ? " Use the UNSET command to revert your change." : ""
    );
    terminal.flush();
  }

  private StringBuilder unsetProperty(
      final StringBuilder consecutiveStatements,
      final SqlBaseParser.SingleStatementContext statementContext
  ) throws IOException {
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(
          restClient.makeKsqlRequest(consecutiveStatements.toString())
      );
      consecutiveStatements.setLength(0);
    }
    final SqlBaseParser.UnsetPropertyContext unsetPropertyContext =
        (SqlBaseParser.UnsetPropertyContext) statementContext.statement();
    final String property = AstBuilder.unquote(unsetPropertyContext.STRING().getText(), "'");
    unsetProperty(property);
    return consecutiveStatements;
  }

  private void unsetProperty(final String property) {
    final Object oldValue = restClient.unsetProperty(property);
    if (oldValue == null) {
      throw new IllegalArgumentException(String.format(
          "Cannot unset local property '%s' which was never set in the first place", property));
    }

    terminal.writer()
        .printf("Successfully unset local property '%s' (value was '%s').%n", property, oldValue);
  }
}
