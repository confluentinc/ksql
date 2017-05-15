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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cli implements Closeable, AutoCloseable {

  private static final String DEFAULT_PRIMARY_PROMPT = "ksql> ";
  private static final String DEFAULT_SECONDARY_PROMPT = "      ";
  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private final ExecutorService queryStreamExecutorService;
  private final JsonWriterFactory jsonWriterFactory;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;

  protected final KSQLRestClient restClient;
  protected final Terminal terminal;
  protected final LineReader lineReader;

  private String primaryPrompt;
  private String secondaryPrompt;

  public Cli(KSQLRestClient restClient) throws IOException {
    this.restClient = restClient;

    this.terminal = TerminalBuilder.builder().system(true).build();
    this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN); // Ignore ^C when not reading a line

    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder().appName("KSQL").terminal(terminal).build();

    // Otherwise, things like '!=' will cause things to break
    this.lineReader.setOpt(LineReader.Option.DISABLE_EVENT_EXPANSION);

    this.primaryPrompt = DEFAULT_PRIMARY_PROMPT;
    this.secondaryPrompt = DEFAULT_SECONDARY_PROMPT;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    this.jsonWriterFactory = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

    this.cliSpecificCommands = new LinkedHashMap<>();
    registerDefaultCliSpecificCommands();
  }

  public void runInteractively() throws IOException {
    terminal.flush();
    while (true) {
      try {
        handleLine(readLine());
      } catch (EndOfFileException exception) {
        // EOF is fine, just terminate the REPL
        return;
      } catch (Exception exception) {
        exception.printStackTrace(terminal.writer());
      }
      terminal.flush();
    }
  }

  public void runNonInteractively(String input) throws Exception {
    // Allow exceptions to halt execution of the KSQL script as soon as the first one is encountered
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
        terminal.writer().println("\tprompt <prompt>: Set the primary prompt to <prompt>");
        terminal.writer().println("\t                 example: \"prompt my_awesome_prompt>\", or \"prompt 'my ''awesome'' prompt> '\"");
      }

      @Override
      public void execute(String commandStrippedLine) {
        primaryPrompt = parsePromptString(commandStrippedLine);
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "prompt2";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tprompt2 <prompt>: Set the secondary prompt to <prompt>");
        terminal.writer().println("\t                  example: \"prompt2 my_awesome_prompt\", or \"prompt2 'my ''awesome'' prompt> '\"");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        secondaryPrompt = parsePromptString(commandStrippedLine);
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

  private String parsePromptString(String commandStrippedLine) {
    if (commandStrippedLine.trim().isEmpty()) {
      throw new RuntimeException("Prompt command must be followed by a new prompt to use");
    }

    String trimmedLine = commandStrippedLine.trim();
    if (trimmedLine.contains("'")) {
      Matcher quotedPromptMatcher = QUOTED_PROMPT_PATTERN.matcher(trimmedLine);
      if (quotedPromptMatcher.matches()) {
        return trimmedLine.substring(1, trimmedLine.length() - 1).replace("''", "'");
      } else {
        throw new RuntimeException(
            "Failed to parse prompt string. All non-enclosing single quotes must be doubled."
        );
      }
    } else {
      return trimmedLine;
    }
  }

  public void handleStatements(String line) throws IOException, InterruptedException, ExecutionException {
    for (SqlBaseParser.SingleStatementContext statementContext : new KSQLParser().getStatements(line)) {
      String ksql = KSQLEngine.getStatementString(statementContext);
      if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
        handleStreamedQuery(ksql);
      } else {
        printJsonResponse(restClient.makeKSQLRequest(ksql));
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
      handleStatements(line);
    }
  }

  private String readLine() throws IOException {
    StringBuilder result = new StringBuilder();
    while (true) {
      try {
        String line = Optional.ofNullable(lineReader.readLine(primaryPrompt)).orElse("").trim();
        while (line.endsWith("\\")) {
          result.append(line.substring(0, line.length() - 1));
          line = Optional.ofNullable(lineReader.readLine(secondaryPrompt)).orElse("").trim();
        }
        return result.append(line).toString().trim();
      } catch (UserInterruptException exception) {
        terminal.writer().println("^C");
        terminal.flush();
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
