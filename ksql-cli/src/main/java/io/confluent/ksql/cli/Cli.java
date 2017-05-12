/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.cli;

import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.rest.client.KSQLRestClient;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import javax.json.Json;
import javax.json.JsonStructure;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli implements Closeable, AutoCloseable {

  private static final String DEFAULT_PROMPT = "ksql> ";
  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private final ExecutorService queryStreamExecutorService;
  private final JsonWriterFactory jsonWriterFactory;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;

  protected final KSQLRestClient restClient;
  protected final Terminal terminal;
  protected final LineReader lineReader;

  private String prompt;

  public Cli(KSQLRestClient restClient) throws IOException {
    this.restClient = restClient;

    this.terminal = TerminalBuilder.builder().system(true).build();
    this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN); // Ignore ^C when not reading a line

    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder().appName("KSQL").terminal(terminal).build();

    // Otherwise, things like '!=' will cause things to break
    this.lineReader.setOpt(LineReader.Option.DISABLE_EVENT_EXPANSION);

    this.prompt = DEFAULT_PROMPT;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    this.jsonWriterFactory = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

    this.cliSpecificCommands = new LinkedHashMap<>();
    registerDefaultCliSpecificCommands();
  }

  public void repl() {
    terminal.flush();
    while (true) {
      try {
        handleLine(readLine());
        terminal.flush();
      } catch (EndOfFileException exception) {
        break;
      } catch (Exception exception) {
        exception.printStackTrace(terminal.writer());
      }
    }
    terminal.flush();
  }

  @Override
  public void close() throws IOException {
    queryStreamExecutorService.shutdownNow();
    restClient.close();
    terminal.close();
  }

  protected void registerCliSpecificCommand(CliSpecificCommand cliSpecificCommand) {
    cliSpecificCommands.put(cliSpecificCommand.getName(), cliSpecificCommand);
  }

  private void registerDefaultCliSpecificCommands() {
    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "help";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\thelp: Show this message");
      }

      @Override
      public void execute(String line) {
        terminal.writer().println("CLI-specific commands (exactly one per line):");
        terminal.writer().println();
        for (CliSpecificCommand cliSpecificCommand : cliSpecificCommands.values()) {
          cliSpecificCommand.printHelp();
          terminal.writer().println();
        }
        terminal.writer().println();
        terminal.writer().println("default behavior:");
        terminal.writer().println();
        terminal.writer().println("    Lines are read one at a time and are sent to the server as KSQL unless one of "
            + "the following is true:"
        );
        terminal.writer().println();
        terminal.writer().println("    1. The line is empty or entirely whitespace. In this case, no request is made to the server.");
        terminal.writer().println();
        terminal.writer().println("    2. The line ends with '\\'. In this case, lines are continuously read and stripped of their "
            + "trailing newline and '\\' until one is encountered that does not end with '\\'; then, the concatenation of "
            + "all lines read during this time is sent to the server as KSQL."
        );
        terminal.writer().println();
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "status";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tstatus:      Get status information on all distributed statements");
        terminal.writer().println("\tstatus <id>: Get detailed status information on the command with an ID of <id>");
        terminal.writer().println("\t             example: \"status stream/MY_AWESOME_KSQL_STREAM\"");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        JsonStructure jsonResponse;
        if (commandStrippedLine.trim().isEmpty()) {
          jsonResponse = restClient.makeStatusRequest();
        } else {
          String statementId = commandStrippedLine.trim();
          jsonResponse = restClient.makeStatusRequest(statementId);
        }
        printJsonResponse(jsonResponse);
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "prompt";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tprompt <prompt>: Set the CLI prompt to <prompt>");
        terminal.writer().println("\t                 example: \"prompt my_awesome_prompt>\", or \"prompt 'my ''awesome'' prompt> '\"");
      }

      @Override
      public void execute(String commandStrippedLine) {
        if (commandStrippedLine.trim().isEmpty()) {
          throw new RuntimeException("Prompt command must be followed by a new prompt to use");
        }

        String trimmedLine = commandStrippedLine.trim();
        if (trimmedLine.contains("'")) {
          Matcher quotedPromptMatcher = QUOTED_PROMPT_PATTERN.matcher(trimmedLine);
          if (quotedPromptMatcher.matches()) {
            prompt = trimmedLine.substring(1, trimmedLine.length() - 1).replace("''", "'");
          } else {
            throw new RuntimeException(
                "Failed to parse prompt string. All non-enclosing single quotes must be doubled."
            );
          }
        } else {
          prompt = trimmedLine;
        }
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "exit";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\texit: Exit the CLI (^D works as well)");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        throw new EndOfFileException();
      }
    });
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

  private void handleLine(String line) throws Exception {
    String trimmedLine = Optional.ofNullable(line).orElse("").trim();

    if (trimmedLine.isEmpty()) {
      return;
    }

    String[] commandArgs = trimmedLine.split("\\s+", 2);
    CliSpecificCommand cliSpecificCommand = cliSpecificCommands.get(commandArgs[0].toLowerCase());
    if (cliSpecificCommand != null) {
      cliSpecificCommand.execute(commandArgs.length > 1 ? commandArgs[1] : "");
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
  }

  private void handleStatements(String line) throws IOException, InterruptedException, ExecutionException {
    for (SqlBaseParser.SingleStatementContext statementContext : new KSQLParser().getStatements(line)) {
      String ksql = KSQLEngine.getStatementString(statementContext);
      if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
        handleStreamedQuery(ksql);
      } else {
        printJsonResponse(restClient.makeKSQLRequest(ksql));
      }
    }
  }

  private void handleStreamedQuery(String query) throws IOException, InterruptedException, ExecutionException {
    displayQueryTerminateInstructions();

    try (KSQLRestClient.QueryStream queryStream = restClient.makeQueryRequest(query)) {
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
          terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
          eraseQueryTerminateInstructions();
          queryStreamFuture.cancel(true);
        }
      });

      try {
        queryStreamFuture.get();
      } catch (CancellationException exception) {
        terminal.writer().println("Query terminated");
      }
    }
  }

  private void displayQueryTerminateInstructions() throws InterruptedException {
//    Doesn't work on my Mac
//    terminal.puts(InfoCmp.Capability.cursor_to_ll);
    terminal.puts(InfoCmp.Capability.cursor_address, terminal.getHeight() - 1, 0);

    terminal.puts(InfoCmp.Capability.delete_line);

    terminal.puts(InfoCmp.Capability.enter_standout_mode);
    terminal.writer().print("Use ^C to terminate the query");
    terminal.puts(InfoCmp.Capability.exit_standout_mode);

    terminal.puts(InfoCmp.Capability.change_scroll_region, 0, terminal.getHeight() - 2);

    // There's got to be a better way to do this. The idea is to clear the screen and move the cursor to the top-left
    // corner, but without actually erasing any output that might be on the terminal screen beforehand.
    terminal.puts(InfoCmp.Capability.cursor_home);
    for (int i = 0; i < terminal.getHeight(); i++) {
      terminal.puts(InfoCmp.Capability.scroll_forward);
    }

    terminal.flush();
  }

  private void eraseQueryTerminateInstructions() {
    terminal.puts(InfoCmp.Capability.save_cursor);

    terminal.puts(InfoCmp.Capability.change_scroll_region, 0, terminal.getHeight() - 1);

    terminal.puts(InfoCmp.Capability.cursor_address, terminal.getHeight() - 1, 0);
    terminal.puts(InfoCmp.Capability.delete_line);

    terminal.puts(InfoCmp.Capability.restore_cursor);

    terminal.puts(InfoCmp.Capability.scroll_forward);

    terminal.flush();
  }

  private void printJsonResponse(JsonStructure jsonResponse) throws IOException {
    jsonWriterFactory.createWriter(terminal.writer()).write(jsonResponse);
    terminal.writer().println();
    terminal.flush();
  }

  protected interface CliSpecificCommand {
    String getName();

    void printHelp();

    void execute(String commandStrippedLine) throws IOException;
  }
}
