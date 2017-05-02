/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.cli;

import io.confluent.kql.rest.client.KQLRestClient;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import javax.json.Json;
import javax.json.JsonStructure;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli implements Closeable, AutoCloseable {

  private static final String DEFAULT_PROMPT = "kql> ";
  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private final ExecutorService queryStreamExecutorService;
  private final JsonWriterFactory jsonWriterFactory;

  protected final KQLRestClient restClient;
  protected final Terminal terminal;
  protected final LineReader lineReader;

  private String prompt;

  public Cli(KQLRestClient restClient) throws IOException {
    this.restClient = restClient;

    this.terminal = TerminalBuilder.builder().system(true).build();
    this.lineReader = LineReaderBuilder.builder().appName("KQL").terminal(terminal).build();

    // Otherwise, things like '!=' will cause things to break
    this.lineReader.setOpt(LineReader.Option.DISABLE_EVENT_EXPANSION);

    this.prompt = DEFAULT_PROMPT;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    this.jsonWriterFactory = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));
  }

  public void repl() throws IOException {
    terminal.flush();
    try {
      while (true) {
        handleLine(readLine());
        terminal.flush();
      }
    } catch (EndOfFileException exception) {
      // EOF is fine, just terminate the REPL
    }
    terminal.flush();
  }

  @Override
  public void close() throws IOException {
    queryStreamExecutorService.shutdownNow();
    restClient.close();
    terminal.close();
  }

  private String readLine() throws IOException {
    while (true) {
      try {
        return lineReader.readLine(prompt);
      } catch (UserInterruptException exception) {
        terminal.writer().println("^C");
        terminal.flush();
      }
    }
  }

  private void handleLine(String line) {
    String trimmedLine = Optional.ofNullable(line).orElse("").trim();

    if (trimmedLine.isEmpty()) {
      return;
    }

    try {
      if (trimmedLine.startsWith(":")) {
        handleMetaCommand(trimmedLine);
      } else {
        StringBuilder multiLineBuffer = new StringBuilder();
        while (trimmedLine.endsWith("\\")) {
          multiLineBuffer.append(trimmedLine.substring(0, trimmedLine.length() - 1));
          try {
            trimmedLine = lineReader.readLine("");
            if (trimmedLine == null) {
              return;
            }
            trimmedLine = trimmedLine.trim();
          } catch (UserInterruptException exception) {
            terminal.writer().println("^C");
            return;
          }
        }
        multiLineBuffer.append(trimmedLine);
        handleStatements(multiLineBuffer.toString());
      }
    } catch (Exception exception) {
      exception.printStackTrace(terminal.writer());
    }
  }

  protected void handleMetaCommand(String trimmedLine) throws IOException, InterruptedException, ExecutionException {
    String[] commandArgs = trimmedLine.split("\\s+", 2);
    String command = commandArgs[0];
    switch (command) {
      case ":":
        terminal.writer().println("Lines beginning with ':' must contain exactly one meta-command");
        break;
      case ":status": {
        JsonStructure jsonResponse;
        if (commandArgs.length == 1) {
          jsonResponse = restClient.makeStatusRequest();
        } else {
          String statementId = commandArgs[1];
          jsonResponse = restClient.makeStatusRequest(statementId);
        }
        printJsonResponse(jsonResponse);
        break;
      }
      case ":query":
        if (commandArgs.length == 1) {
          throw new RuntimeException(String.format("'%s' meta-command must be followed by a query to stream", command));
        }
        String query = commandArgs[1];
        handleStreamedQuery(query);
        break;
      case ":prompt":
        if (commandArgs.length == 1) {
          throw new RuntimeException(String.format(
              "'%s' meta-command must be followed by a new prompt value, optionally enclosed in single quotes",
              command
          ));
        }
        String newPrompt = commandArgs[1];
        if (newPrompt.startsWith("'")) {
          Matcher quotedPromptMatcher = QUOTED_PROMPT_PATTERN.matcher(newPrompt);
          if (quotedPromptMatcher.matches()) {
            prompt = newPrompt.substring(1, newPrompt.length() - 1).replace("''", "'");
          } else {
            throw new RuntimeException(
                "Failed to parse prompt string. All non-enclosing single quotes must be doubled."
            );
          }
        } else {
          prompt = newPrompt;
        }
        break;
      case ":help":
        printHelpMessage();
        break;
      default:
        throw new RuntimeException(String.format("Unknown meta-command: '%s'", command));
    }
  }

  private void handleStatements(String line) throws IOException {
    JsonStructure jsonResponse = restClient.makeKQLRequest(line);
    printJsonResponse(jsonResponse);
  }

  private void handleStreamedQuery(String query) throws IOException, InterruptedException, ExecutionException {
    try (KQLRestClient.QueryStream queryStream = restClient.makeQueryRequest(query)) {
      Future<?> queryStreamFuture = queryStreamExecutorService.submit(new Runnable() {
        @Override
        public void run() {
          while (queryStream.hasNext()) {
            terminal.writer().println(queryStream.next());
            terminal.flush();
          }
        }
      });

      terminal.handle(Terminal.Signal.INT, new Terminal.SignalHandler() {
        @Override
        public void handle(Terminal.Signal signal) {
          queryStreamFuture.cancel(true);
        }
      });

      try {
        queryStreamFuture.get();
      } catch (CancellationException exception) {
        terminal.writer().println("Query terminated");
      }

      terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_DFL);
    }
  }

  private void printJsonResponse(JsonStructure jsonResponse) throws IOException {
    jsonWriterFactory.createWriter(terminal.writer()).write(jsonResponse);
    terminal.writer().println();
    terminal.flush();
  }

  private void printHelpMessage() throws IOException {
    terminal.writer().println("meta-commands (one per line, must be on their own line):");
    terminal.writer().println();
    terminal.writer().println("    :exit               - Exit the REPL (^D is also fine)");
    terminal.writer().println("    :help               - Show this message");
    terminal.writer().println("    :prompt <prompt>    - Change the prompt to <prompt>");
    terminal.writer().println("                          Example: :prompt 'my ''special'' prompt> '");
//    terminal.writer().println("    :output             - Show the current output format");
//    terminal.writer().println("                          Example: :output");
//    terminal.writer().println("    :output <format>    - Change the output format to <format> (case-insensitive), "
//        + "currently only 'json' and 'cli' are supported)"
//    );
//    terminal.writer().println("                          Example: :output JSON");
//    terminal.writer().println("    :output CLI <width> - Change the output format to CLI, with a minimum column width of <width>");
//    terminal.writer().println("                          Example: :output CLI 69");
    terminal.writer().println("    :status             - Check on the statuses of all KQL statements processed by the server");
    terminal.writer().println("                          Example: :status");
    terminal.writer().println("    :status <id>        - Check on the status of the statement with ID <id>");
    terminal.writer().println("                          Example: :status KQL_NODE_69_STATEMENT_69");
    terminal.writer().println("    :query <query>      - Stream the results of <query> to the console");
    terminal.writer().println("                          Example: :query SELECT * FROM movies WHERE imdb_score = 69;");
    terminal.writer().println("                          NOTE: Use ctrl-C to stop streaming a query");
    printExtraMetaCommandsHelp();
    terminal.writer().println();
    terminal.writer().println();
    terminal.writer().println("default behavior:");
    terminal.writer().println();
    terminal.writer().println("    Lines are read one at a time and are sent to the server as KQL unless one of "
        + "the following is true:"
    );
    terminal.writer().println();
    terminal.writer().println("    1. The line is empty or entirely whitespace. In this case, no request is made to the server.");
    terminal.writer().println();
    terminal.writer().println("    2. The line begins with ':'. In this case, the line is parsed as a meta-command "
        + "(as detailed above)."
    );
    terminal.writer().println();
    terminal.writer().println("    3. The line ends with '\\'. In this case, lines are continuously read and stripped of their "
        + "trailing newline and '\\' until one is encountered that does not end with '\\'; then, the concatenation of "
        + "all lines read during this time is sent to the server as KQL."
    );
  }

  protected void printExtraMetaCommandsHelp() throws IOException {
  }
}
