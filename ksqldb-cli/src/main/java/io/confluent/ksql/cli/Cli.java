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

package io.confluent.ksql.cli;

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.KsqlTerminal.StatusClosable;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.cli.console.cmd.CliCommandRegisterUtil;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommand;
import io.confluent.ksql.cli.console.cmd.RequestPipeliningCommand;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.PrintTopicContext;
import io.confluent.ksql.parser.SqlBaseParser.QueryStatementContext;
import io.confluent.ksql.parser.SqlBaseParser.SetPropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.StatementContext;
import io.confluent.ksql.parser.SqlBaseParser.UnsetPropertyContext;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMap2;
import io.confluent.ksql.util.HandlerMaps.Handler2;
import io.confluent.ksql.util.ParserUtil;
import io.confluent.ksql.util.WelcomeMsgUtils;
import io.vertx.core.Context;
import io.vertx.core.VertxException;
import java.io.Closeable;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.jline.reader.EndOfFileException;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cli implements KsqlRequestExecutor, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cli.class);

  private static final int MAX_RETRIES = 10;

  private static final ClassHandlerMap2<StatementContext, Cli, String> STATEMENT_HANDLERS =
      HandlerMaps
          .forClass(StatementContext.class)
          .withArgTypes(Cli.class, String.class)
          .put(QueryStatementContext.class, Cli::handleQuery)
          .put(PrintTopicContext.class, Cli::handlePrintedTopic)
          .put(SetPropertyContext.class, Cli::setPropertyFromCtxt)
          .put(UnsetPropertyContext.class, Cli::unsetPropertyFromCtxt)
          .build();

  private final Long streamedQueryRowLimit;
  private final Long streamedQueryTimeoutMs;

  private final KsqlRestClient restClient;
  private final Console terminal;
  private final RemoteServerState remoteServerState;

  public static Cli build(
      final Long streamedQueryRowLimit,
      final Long streamedQueryTimeoutMs,
      final OutputFormat outputFormat,
      final KsqlRestClient restClient
  ) {
    final Console console = Console.build(outputFormat);
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
    this.remoteServerState = new RemoteServerState();

    final Supplier<String> versionSuppler =
        () -> restClient.getServerInfo().getResponse().getVersion();

    CliCommandRegisterUtil.registerDefaultCommands(
        this,
        terminal,
        versionSuppler,
        restClient,
        remoteServerState::reset,
        remoteServerState::getRequestPipelining,
        remoteServerState::setRequestPipelining);
  }

  @Override
  public void makeKsqlRequest(final String statements) {
    if (statements.isEmpty()) {
      return;
    }

    printKsqlResponse(makeKsqlRequest(statements, restClient::makeKsqlRequest));
  }

  private <R> RestResponse<R> makeKsqlRequest(
      final String ksql,
      final BiFunction<String, Long, RestResponse<R>> requestIssuer) {
    final Long commandSequenceNumberToWaitFor = remoteServerState.getRequestPipelining()
        ? null
        : remoteServerState.getLastCommandSequenceNumber();

    int retries = 0;
    while (retries < MAX_RETRIES) {
      try {
        final RestResponse<R> response = requestIssuer.apply(ksql, commandSequenceNumberToWaitFor);

        if (isSequenceNumberTimeout(response)) {
          terminal.writer().printf(
              "Error: command not executed since the server timed out "
                  + "while waiting for prior commands to finish executing.%n"
                  + "If you wish to execute new commands without waiting for "
                  + "prior commands to finish, run the command '%s ON'.%n",
              RequestPipeliningCommand.NAME);
        } else if (isKsqlEntityList(response)) {
          updateLastCommandSequenceNumber((KsqlEntityList) response.getResponse());
        }
        return response;
      } catch (KsqlRestClientException e) {
        if (e.getCause() instanceof ExecutionException && e.getCause()
            .getCause() instanceof VertxException) {
          // We close the connection asynchronously after a pull query is terminated at the terminal
          // this means there is a chance that a subsequent query can grab the same connection
          // and attempt to use it resulting in an exception. This is extremely unlikely to happen
          // in real-usage, but can happen in the test suite, so we catch this case and retry
          final VertxException ve = (VertxException) e.getCause().getCause();
          if (ve.getMessage().equals("Connection was closed")) {
            retries++;
            continue;
          }
        }
        throw e;
      }
    }
    throw new KsqlRestClientException("Failed to execute request " + ksql);
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
        terminal.writer().println("Exiting ksqlDB.");
        eof = true;
      } catch (final Exception exception) {
        LOGGER.error("", exception);
        terminal.printError(ErrorMessageUtil.buildErrorMessage(exception),
            exception.toString());
      }
      terminal.flush();
    }
  }

  private void displayWelcomeMessage() {
    String serverVersion;
    String serverStatus;
    try {
      final ServerInfo serverInfo = restClient.getServerInfo().getResponse();
      serverVersion = serverInfo.getVersion();
      serverStatus = serverInfo.getServerStatus() == null
          ? "<unknown>" : serverInfo.getServerStatus();
    } catch (final Exception exception) {
      serverVersion = "<unknown>";
      serverStatus = "<unknown>";
    }
    final String cliVersion = AppInfo.getVersion();

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
    writer.println("Server Status: " + serverStatus);
    writer.println();
    writer.println(helpReminderMessage);
    writer.println();
    terminal.flush();
  }

  @Override
  public void close() {
    terminal.close();
  }

  void handleLine(final String line) {
    final String trimmedLine = Optional.ofNullable(line).orElse("").trim();
    if (trimmedLine.isEmpty()) {
      return;
    }

    handleStatements(trimmedLine);
  }

  /**
   * Attempt to read a logical line of input from the user. Can span multiple physical lines, as
   * long as all but the last end with '\\'.
   *
   * @return The parsed, logical line.
   * @throws EndOfFileException If there is no more input available from the user.
   */
  private String readLine() {
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

  private void handleStatements(final String line) {
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(line);

    final StringBuilder consecutiveStatements = new StringBuilder();
    for (final ParsedStatement parsed : statements) {
      final StatementContext statementContext = parsed.getStatement().statement();
      final String statementText = parsed.getStatementText();

      final Handler2<StatementContext, Cli, String> handler = STATEMENT_HANDLERS
          .get(statementContext.getClass());

      if (handler == null) {
        consecutiveStatements.append(statementText);
      } else {
        makeKsqlRequest(consecutiveStatements.toString());
        consecutiveStatements.setLength(0);

        handler.handle(this, statementText, statementContext);
      }
    }
    if (consecutiveStatements.length() != 0) {
      makeKsqlRequest(consecutiveStatements.toString());
    }
  }

  private void printKsqlResponse(final RestResponse<KsqlEntityList> response) {
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

  @SuppressWarnings({"try", "unused"}) // ignored param is required to compile.
  private void handleQuery(
      final String statement,
      final SqlBaseParser.QueryStatementContext query
  ) {
    final RestResponse<StreamPublisher<StreamedRow>> queryResponse =
        makeKsqlRequest(statement, restClient::makeQueryRequestStreamed);

    if (!queryResponse.isSuccessful()) {
      terminal.printErrorMessage(queryResponse.getErrorMessage());
      terminal.flush();
    } else {
      try (StatusClosable toClose = terminal.setStatusMessage("Press CTRL-C to interrupt")) {
        final StreamPublisher<StreamedRow> publisher = queryResponse.getResponse();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final QueryStreamSubscriber subscriber = new QueryStreamSubscriber(publisher.getContext(),
            future);
        publisher.subscribe(subscriber);

        terminal.handle(Terminal.Signal.INT, signal -> {
          terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
          subscriber.close();
          future.complete(null);
        });

        try {
          if (streamedQueryTimeoutMs != null) {
            future.get(streamedQueryTimeoutMs, TimeUnit.MILLISECONDS);
          } else {
            future.get();
          }
        } catch (Exception e) {
          LOGGER.error("Unexpected exception in waiting for query", e);
        } finally {
          terminal.writer().println("Query terminated");
          terminal.flush();
          publisher.close();
        }
      }
    }
  }

  @SuppressWarnings({"try", "unused"})
  private void handlePrintedTopic(
      final String printTopic,
      final SqlBaseParser.PrintTopicContext ignored
  ) {
    final RestResponse<StreamPublisher<String>> topicResponse =
        makeKsqlRequest(printTopic, restClient::makePrintTopicRequest);

    if (topicResponse.isSuccessful()) {

      try (StatusClosable toClose = terminal.setStatusMessage("Press CTRL-C to interrupt")) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamPublisher<String> publisher = topicResponse.getResponse();
        final PrintTopicSubscriber subscriber = new PrintTopicSubscriber(publisher.getContext(),
            future);
        publisher.subscribe(subscriber);

        terminal.handle(Terminal.Signal.INT, signal -> {
          terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
          subscriber.close();
          future.complete(null);
        });

        try {
          future.get();
        } catch (Exception e) {
          LOGGER.error("Unexpected exception in waiting for print topic completion", e);
        } finally {
          publisher.close();
        }
        terminal.writer().println("Topic printing ceased");
      }
    } else {
      terminal.writer().println(topicResponse.getErrorMessage().getMessage());
    }
    terminal.flush();
  }

  @SuppressWarnings("unused")
  private void setPropertyFromCtxt(
      final String ignored,
      final SqlBaseParser.SetPropertyContext setPropertyContext
  ) {
    final String property = ParserUtil.unquote(setPropertyContext.STRING(0).getText(), "'");
    final String value = ParserUtil.unquote(setPropertyContext.STRING(1).getText(), "'");
    setProperty(property, value);
  }

  private void setProperty(final String property, final String value) {
    final Object priorValue = restClient.setProperty(property, value);

    terminal.writer().printf(
        "Successfully changed local property '%s'%s to '%s'.%s%n",
        property,
        priorValue == null ? "" : " from '" + priorValue + "'",
        value,
        priorValue == null ? " Use the UNSET command to revert your change." : ""
    );
    terminal.flush();
  }

  @SuppressWarnings("unused")
  private void unsetPropertyFromCtxt(
      final String ignored,
      final SqlBaseParser.UnsetPropertyContext unsetPropertyContext
  ) {
    final String property = ParserUtil.unquote(unsetPropertyContext.STRING().getText(), "'");
    unsetProperty(property);
  }

  private void unsetProperty(final String property) {
    final Object oldValue = restClient.unsetProperty(property);
    if (oldValue == null) {
      throw new IllegalArgumentException(String.format(
          "Cannot unset local property '%s' which was never set in the first place", property));
    }

    terminal.writer()
        .printf("Successfully unset local property '%s' (value was '%s').%n", property, oldValue);
    terminal.flush();
  }

  private static boolean isSequenceNumberTimeout(final RestResponse<?> response) {
    return response.isErroneous()
        && (response.getErrorMessage().getErrorCode()
            == Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT);
  }

  private static boolean isKsqlEntityList(final RestResponse<?> response) {
    return response.isSuccessful() && response.getResponse() instanceof KsqlEntityList;
  }

  private void updateLastCommandSequenceNumber(final KsqlEntityList entities) {
    entities.stream()
        .filter(entity -> entity instanceof CommandStatusEntity)
        .map(entity -> (CommandStatusEntity)entity)
        .mapToLong(CommandStatusEntity::getCommandSequenceNumber)
        .max()
        .ifPresent(remoteServerState::setLastCommandSequenceNumber);
  }

  private static final class RemoteServerState {
    private long lastCommandSequenceNumber;
    private boolean requestPipelining;

    private RemoteServerState() {
      reset();
    }

    private void reset() {
      lastCommandSequenceNumber = -1L;
      requestPipelining = false;
    }

    private long getLastCommandSequenceNumber() {
      return lastCommandSequenceNumber;
    }

    private boolean getRequestPipelining() {
      return requestPipelining;
    }

    private void setLastCommandSequenceNumber(final long seqNum) {
      lastCommandSequenceNumber = seqNum;
    }

    private void setRequestPipelining(final boolean newSetting) {
      requestPipelining = newSetting;
    }
  }

  private class QueryStreamSubscriber extends BaseSubscriber<StreamedRow> {

    private final CompletableFuture<Void> future;
    private boolean closed;
    private long rowsRead;

    QueryStreamSubscriber(final Context context, final CompletableFuture<Void> future) {
      super(context);
      this.future = Objects.requireNonNull(future);
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final StreamedRow row) {
      if (closed) {
        return;
      }
      terminal.printStreamedRow(row);
      terminal.flush();
      if (row.isTerminal()) {
        future.complete(null);
        close();
        return;
      }
      if (row.getRow().isPresent()) {
        rowsRead++;
        if (streamedQueryRowLimit != null && streamedQueryRowLimit == rowsRead) {
          future.complete(null);
          close();
          return;
        }
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
      future.complete(null);
    }

    @Override
    protected void handleError(final Throwable t) {
      future.completeExceptionally(t);
    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

  private class PrintTopicSubscriber extends BaseSubscriber<String> {

    private final CompletableFuture<Void> future;
    private boolean closed;

    PrintTopicSubscriber(final Context context, final CompletableFuture<Void> future) {
      super(context);
      this.future = Objects.requireNonNull(future);
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final String line) {
      if (closed) {
        return;
      }
      if (line.isEmpty()) {
        // Ignore it - the server can insert these
      } else {
        terminal.writer().println(line);
        terminal.flush();
        makeRequest(1);
      }
    }

    @Override
    protected void handleComplete() {
      future.complete(null);
    }

    @Override
    protected void handleError(final Throwable t) {
      future.completeExceptionally(t);
    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

}
