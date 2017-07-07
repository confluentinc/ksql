/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.cli.util.CliUtils;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ErrorMessage;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.entity.TopicsList;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.util.Version;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.StreamsConfig;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultExpander;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Cli implements Closeable, AutoCloseable {

  public enum OutputFormat {
    JSON,
    TABULAR
  }

  private static final String DEFAULT_PRIMARY_PROMPT   = "ksql> ";
  private static final String DEFAULT_SECONDARY_PROMPT = "      ";

  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private static final ConfigDef CONSUMER_CONFIG_DEF = getConfigDef(ConsumerConfig.class);
  private static final ConfigDef PRODUCER_CONFIG_DEF = getConfigDef(ProducerConfig.class);

  private final ExecutorService queryStreamExecutorService;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;
  private final ObjectMapper objectMapper;
  private final Map<String, Object> localProperties;

  private final Long streamedQueryRowLimit;
  private final Long streamedQueryTimeoutMs;

  protected final KsqlRestClient restClient;
  protected final Terminal terminal;
  protected final LineReader lineReader;

  private String primaryPrompt;

  private OutputFormat outputFormat;

  public Cli(
      KsqlRestClient restClient,
      Long streamedQueryRowLimit,
      Long streamedQueryTimeoutMs,
      OutputFormat outputFormat
  ) throws IOException {
    Objects.requireNonNull(restClient, "Must provide the CLI with a REST client");
    Objects.requireNonNull(outputFormat, "Must provide the CLI with a beginning output format");

    this.streamedQueryRowLimit  = streamedQueryRowLimit;
    this.streamedQueryTimeoutMs = streamedQueryTimeoutMs;
    this.restClient = restClient;

    this.terminal = TerminalBuilder.builder().system(true).build();
    // Ignore ^C when not reading a line
    this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);

    // The combination of parser/expander here allow for multiple-line commands connected by '\\'
    DefaultParser parser = new DefaultParser();
    parser.setEofOnEscapedNewLine(true);
    parser.setQuoteChars(new char[0]);
    parser.setEscapeChars(new char[] {'\\'});

    Expander expander = new NoOpExpander();

    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder()
        .appName("KSQL")
        .expander(expander)
        .parser(parser)
        .terminal(terminal)
        .build();

    this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_DUPS);
    this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_SPACE);
    this.lineReader.setVariable(LineReader.SECONDARY_PROMPT_PATTERN, DEFAULT_SECONDARY_PROMPT);

    this.primaryPrompt = DEFAULT_PRIMARY_PROMPT;

    this.outputFormat = outputFormat;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    this.cliSpecificCommands = new LinkedHashMap<>();
    registerDefaultCliSpecificCommands();

    this.objectMapper = new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    new SchemaMapper().registerToObjectMapper(objectMapper);

    this.localProperties = new HashMap<>();
  }

  public void runInteractively() throws IOException {
    displayWelcomeMessage();
    while (true) {
      try {
        handleLine(readLine());
      } catch (EndOfFileException exception) {
        // EOF is fine, just terminate the REPL
        return;
      } catch (Exception exception) {
        if (exception.getMessage() != null) {
          terminal.writer().println(exception.getMessage());
        } else {
          terminal.writer().println(exception.getClass().getName());
          // TODO: Maybe ask the user if they'd like to see the stack trace here?
        }
      }
      terminal.flush();
    }
  }

  private void displayWelcomeMessage() {
    String serverVersion;
    try {
      serverVersion = restClient.makeRootRequest().getResponse().getVersion();
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
        + "Need quick refresher? "
        + "Wanna RTFM but perturbed by our lack of man pages? "
        + "Type 'help' (case-insensitive) for a rundown of how things work!";
    // Don't want to display the logo if it'll just end up getting wrapped and looking hideous
    if (terminal.getWidth() >= logoWidth) {
      // Want to center the logo, but in the case of something like a fullscreen terminal, just
      // centering around the help message (longest single line of text in the welcome message)
      // should be enough; looks a little weird if you try to center the logo on a wide enough
      // screen and it just kind of ends up out in the middle of nowhere; hence, the call to
      // Math.min(terminal.getWidth(), helpReminderMessage.length())
      int paddedLogoWidth = Math.min(terminal.getWidth(), helpReminderMessage.length());
      int paddingWidth = (paddedLogoWidth - logoWidth) / 2;
      String leftPadding = new String(new byte[paddingWidth]).replaceAll(".", " ");
      terminal.writer().printf("%s=================================%n", leftPadding);
      terminal.writer().printf("%s=   _  __ _____  ____  _        =%n", leftPadding);
      terminal.writer().printf("%s=  | |/ // ____|/ __ \\| |       =%n", leftPadding);
      terminal.writer().printf("%s=  | ' /| (___ | |  | | |       =%n", leftPadding);
      terminal.writer().printf("%s=  |  <  \\___ \\| |  | | |       =%n", leftPadding);
      terminal.writer().printf("%s=  | . \\ ____) | |__| | |____   =%n", leftPadding);
      terminal.writer().printf("%s=  |_|\\_\\_____/ \\___\\_\\______|  =%n", leftPadding);
      terminal.writer().printf("%s=                               =%n", leftPadding);
      terminal.writer().printf("%s=  Kafka Streams Query Language =%n", leftPadding);
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
        terminal.writer().println("CLI-specific commands (EACH MUST BE ON THEIR OWN LINE):");
        terminal.writer().println();
        for (CliSpecificCommand cliSpecificCommand : cliSpecificCommands.values()) {
          cliSpecificCommand.printHelp();
          terminal.writer().println();
        }
        terminal.writer().println();
        terminal.writer().println("default behavior:");
        terminal.writer().println();
        terminal.writer().println("    Lines are read one at a time and are sent to the server as"
            + "Ksql unless one of the following is true:"
        );
        terminal.writer().println();
        terminal.writer().println("    1. The line is empty or entirely whitespace. In this case, "
            + "no request is made to the server."
        );
        terminal.writer().println();
        terminal.writer().println("    2. The line ends with '\\'. In this case, lines are "
            + "continuously read and stripped of their trailing newline and '\\' until one is "
            + "encountered that does not end with '\\'; then, the concatenation of all lines read "
            + "during this time is sent to the server as Ksql."
        );
        terminal.writer().println();
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "clear";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tclear: Clear the current terminal");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        terminal.puts(InfoCmp.Capability.clear_screen);
        terminal.flush();
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "status";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tstatus:      Get status information on all distributed "
            + "statements"
        );
        terminal.writer().println("\tstatus <id>: Get detailed status information on the command "
            + "with an ID of <id>"
        );
        terminal.writer().println("\t             example: "
            + "\"status stream/MY_AWESOME_KSQL_STREAM\""
        );
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        if (commandStrippedLine.trim().isEmpty()) {
          RestResponse<CommandStatuses> response = restClient.makeStatusRequest();
          if (response.isSuccessful()) {
            printCommandStatuses(response.getResponse());
          } else {
            printErrorMessage(response.getErrorMessage());
          }
        } else {
          String statementId = commandStrippedLine.trim();
          RestResponse<CommandStatus> response = restClient.makeStatusRequest(statementId);
          if (response.isSuccessful()) {
            printCommandStatus(response.getResponse());
          } else {
            printErrorMessage(response.getErrorMessage());
          }
        }
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
        terminal.writer().println("\t                 example: \"prompt my_awesome_prompt>\", or "
            + "\"prompt 'my ''awesome'' prompt> '\""
        );
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
        terminal.writer().println("\t                  example: \"prompt2 my_awesome_prompt\", or "
            + "\"prompt2 'my ''awesome'' prompt> '\""
        );
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        lineReader.setVariable(
            LineReader.SECONDARY_PROMPT_PATTERN,
            parsePromptString(commandStrippedLine)
        );
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      private final String outputFormats = String.format(
          "'%s'",
          String.join(
              "', '",
              Arrays.stream(OutputFormat.values())
                  .map(Object::toString)
                  .collect(Collectors.toList())
          )
      );

      @Override
      public String getName() {
        return "output";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\toutput:          View the current output format");
        terminal.writer() .printf(
            "\toutput <format>: Set the output format to <format> (valid formats: %s)%n",
            outputFormats
        );
        terminal.writer().println("\t                 example: \"output JSON\"");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        String newFormat = commandStrippedLine.trim().toUpperCase();
        if (newFormat.isEmpty()) {
          terminal.writer().printf("Current output format: %s%n", outputFormat.name());
        } else {
          try {
            outputFormat = OutputFormat.valueOf(newFormat);
            terminal.writer().printf("Output format set to %s%n", outputFormat.name());
          } catch (IllegalArgumentException exception) {
            terminal.writer().printf(
                "Invalid output format: '%s' (valid formats: %s)%n",
                newFormat,
                outputFormats
            );
          }
        }
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "history";
      }

      @Override
      public void printHelp() {
        terminal.writer().println(
            "\thistory: Show previous lines entered during the current CLI session. You can use "
            + "up and down arrow keys to navigate to the previous lines too."
        );
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        for (History.Entry historyEntry : lineReader.getHistory()) {
          terminal.writer().printf("%4d: %s%n", historyEntry.index(), historyEntry.line());
        }
        terminal.flush();
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "version";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tversion: Get the current KSQL version");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        ServerInfo serverInfo = restClient.makeRootRequest().getResponse();
        terminal.writer().printf("Version: %s%n", serverInfo.getVersion());
        terminal.flush();
      }
    });

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "exit";
      }

      @Override
      public void printHelp() {
        terminal.writer().println(
            "\texit: Exit the CLI; EOF (i.e., ^D) works as well"
        );
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

    String trimmedLine = commandStrippedLine.trim().replace("%", "%%");
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

  /**
   * Attempt to read a logical line of input from the user. Can span multiple physical lines, as
   * long as all but the last end with '\\'.
   * @return The parsed, logical line.
   * @throws EndOfFileException If there is no more input available from the user.
   * @throws IOException If any other I/O error occurs.
   */
  private String readLine() throws IOException {
    while (true) {
      try {
        String result = lineReader.readLine(primaryPrompt);
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
        if (consecutiveStatements.length() != 0) {
          printKsqlResponse(
              restClient.makeKsqlRequest(consecutiveStatements.toString(), localProperties)
          );
          consecutiveStatements = new StringBuilder();
        }
        if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
          handleStreamedQuery(statementText);
        } else {
          handlePrintedTopic(statementText);
        }
      } else if (statementContext.statement() instanceof SqlBaseParser.SetPropertyContext) {
        if (consecutiveStatements.length() != 0) {
          printKsqlResponse(
              restClient.makeKsqlRequest(consecutiveStatements.toString(), localProperties)
          );
          consecutiveStatements = new StringBuilder();
        }
        SqlBaseParser.SetPropertyContext setPropertyContext =
            (SqlBaseParser.SetPropertyContext) statementContext.statement();
        String property = AstBuilder.unquote(setPropertyContext.STRING(0).getText(), "'");
        String value = AstBuilder.unquote(setPropertyContext.STRING(1).getText(), "'");
        setProperty(property, value);
      } else if (statementContext.statement() instanceof SqlBaseParser.UnsetPropertyContext) {
        if (consecutiveStatements.length() != 0) {
          printKsqlResponse(
              restClient.makeKsqlRequest(consecutiveStatements.toString(), localProperties)
          );
          consecutiveStatements = new StringBuilder();
        }
        SqlBaseParser.UnsetPropertyContext unsetPropertyContext =
            (SqlBaseParser.UnsetPropertyContext) statementContext.statement();
        String property = AstBuilder.unquote(unsetPropertyContext.STRING().getText(), "'");
        unsetProperty(property);
      } else if (statementContext.statement() instanceof SqlBaseParser.CreateTopicContext) {
        CliUtils cliUtils = new CliUtils();
        Optional<String> avroSchema = cliUtils.getAvroSchemaIfAvroTopic(
            (SqlBaseParser.CreateTopicContext) statementContext.statement());
        if (avroSchema.isPresent()) {
          setProperty(DdlConfig.AVRO_SCHEMA, avroSchema.get());
        }
        consecutiveStatements.append(statementText);
      } else {
        consecutiveStatements.append(statementText);
      }
    }
    if (consecutiveStatements.length() != 0) {
      printKsqlResponse(
          restClient.makeKsqlRequest(consecutiveStatements.toString(), localProperties)
      );
    }
  }

  private void handleStreamedQuery(String query)
      throws IOException, InterruptedException, ExecutionException {
    RestResponse<KsqlRestClient.QueryStream> queryResponse =
        restClient.makeQueryRequest(query, localProperties);

    if (queryResponse.isSuccessful()) {
      try (KsqlRestClient.QueryStream queryStream = queryResponse.getResponse()) {
        Future<?> queryStreamFuture = queryStreamExecutorService.submit(new Runnable() {
          @Override
          public void run() {
            for (long rowsRead = 0; keepReading(rowsRead) && queryStream.hasNext(); rowsRead++) {
              try {
                printStreamedRow(queryStream.next());
              } catch (IOException exception) {
                throw new RuntimeException(exception);
              }
            }
          }
        });

        terminal.handle(Terminal.Signal.INT, new Terminal.SignalHandler() {
          @Override
          public void handle(Terminal.Signal signal) {
            terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
            queryStreamFuture.cancel(true);
          }
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
      printErrorMessage(queryResponse.getErrorMessage());
    }
  }

  private boolean keepReading(long rowsRead) {
    return streamedQueryRowLimit == null || rowsRead < streamedQueryRowLimit;
  }

  private void handlePrintedTopic(String printTopic)
      throws InterruptedException, ExecutionException, IOException {
    RestResponse<InputStream> topicResponse =
        restClient.makePrintTopicRequest(printTopic, localProperties);

    if (topicResponse.isSuccessful()) {
      try (Scanner topicStreamScanner = new Scanner(topicResponse.getResponse())) {
        Future<?> topicPrintFuture = queryStreamExecutorService.submit(new Runnable() {
          @Override
          public void run() {
            while (topicStreamScanner.hasNextLine()) {
              String line = topicStreamScanner.nextLine();
              if (!line.isEmpty()) {
                terminal.writer().println(line);
                terminal.flush();
              }
            }
          }
        });

        terminal.handle(Terminal.Signal.INT, new Terminal.SignalHandler() {
          @Override
          public void handle(Terminal.Signal signal) {
            terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
            topicPrintFuture.cancel(true);
          }
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

  private void printKsqlResponse(RestResponse<KsqlEntityList> response) throws IOException {
    if (response.isSuccessful()) {
      printKsqlEntityList(response.getResponse());
    } else {
      printErrorMessage(response.getErrorMessage());
    }
  }

  private void printStreamedRow(StreamedRow row) throws IOException {
    if (row.getErrorMessage() != null) {
      printErrorMessage(row.getErrorMessage());
    } else {
      switch (outputFormat) {
        case JSON:
          printAsJson(row.getRow().columns);
          break;
        case TABULAR:
          printAsTable(row.getRow());
          break;
        default:
          throw new RuntimeException(String.format(
              "Unexpected output format: '%s'",
              outputFormat.name()
          ));
      }
    }
  }

  private void printCommandStatus(CommandStatus status) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(status);
        break;
      case TABULAR:
        printAsTable(status);
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected output format: '%s'",
            outputFormat.name()
        ));
    }
  }

  private void printCommandStatuses(CommandStatuses statuses) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(statuses);
        break;
      case TABULAR:
        printAsTable(statuses);
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected output format: '%s'", outputFormat.name()
        ));
    }
  }

  private void printKsqlEntityList(List<KsqlEntity> entityList) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(entityList);
        break;
      case TABULAR:
        for (KsqlEntity ksqlEntity : entityList) {
          terminal.writer().println();
          printAsTable(ksqlEntity);
        }
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected output format: '%s'",
            outputFormat.name()
        ));
    }
  }

  private void printErrorMessage(ErrorMessage errorMessage) {
    terminal.writer().println(errorMessage.getMessage());
  }

  private PropertiesList propertiesListWithOverrides(PropertiesList propertiesList) {
    Map<String, Object> properties = propertiesList.getProperties();
    for (Map.Entry<String, Object> localPropertyEntry : localProperties.entrySet()) {
      properties.put(
          "(LOCAL OVERRIDE) " + localPropertyEntry.getKey(),
          localPropertyEntry.getValue()
      );
    }
    return new PropertiesList(propertiesList.getStatementText(), properties);
  }

  private void printAsJson(Object o) throws IOException {
    if (o instanceof PropertiesList) {
      o = propertiesListWithOverrides((PropertiesList) o);
    } else if (o instanceof KsqlEntityList) {
      List<KsqlEntity> newEntities = new ArrayList<>();
      for (KsqlEntity ksqlEntity : (KsqlEntityList) o) {
        if (ksqlEntity instanceof PropertiesList) {
          ksqlEntity = propertiesListWithOverrides((PropertiesList) ksqlEntity);
        }
        newEntities.add(ksqlEntity);
      }
      o = newEntities;
    }
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(terminal.writer(), o);
    terminal.writer().println();
    terminal.flush();
  }

  private void printAsTable(KsqlEntity ksqlEntity) {
    List<String> columnHeaders;
    List<List<String>> rowValues;
    if (ksqlEntity instanceof CommandStatusEntity) {
      CommandStatusEntity commandStatusEntity = (CommandStatusEntity) ksqlEntity;
      columnHeaders = Arrays.asList("Command ID", "Status", "Message");
      CommandId commandId = commandStatusEntity.getCommandId();
      CommandStatus commandStatus = commandStatusEntity.getCommandStatus();
      rowValues = Collections.singletonList(Arrays.asList(
          commandId.toString(),
          commandStatus.getStatus().name(),
          commandStatus.getMessage().split("\n", 2)[0]
      ));
    } else if (ksqlEntity instanceof ErrorMessageEntity) {
      ErrorMessage errorMessage = ((ErrorMessageEntity) ksqlEntity).getErrorMessage();
      printErrorMessage(errorMessage);
      return;
    } else if (ksqlEntity instanceof PropertiesList) {
      PropertiesList propertiesList = propertiesListWithOverrides((PropertiesList) ksqlEntity);
      Map<String, Object> properties = propertiesList.getProperties();
      columnHeaders = Arrays.asList("Property", "Value");
      rowValues = properties.entrySet().stream()
          .map(propertyEntry -> Arrays.asList(
              propertyEntry.getKey(),
              Objects.toString(propertyEntry.getValue())
          )).collect(Collectors.toList());
    } else if (ksqlEntity instanceof Queries) {
      List<Queries.RunningQuery> runningQueries = ((Queries) ksqlEntity).getQueries();
      columnHeaders = Arrays.asList("ID", "Kafka Topic", "Query String");
      rowValues = runningQueries.stream()
          .map(runningQuery -> Arrays.asList(
              Long.toString(runningQuery.getId()),
              runningQuery.getKafkaTopic(),
              runningQuery.getQueryString()
          )).collect(Collectors.toList());
    } else if (ksqlEntity instanceof SourceDescription) {
      List<Field> fields = ((SourceDescription) ksqlEntity).getSchema().fields();
      columnHeaders = Arrays.asList("Field", "Type");
      rowValues = fields.stream()
          .map(field -> Arrays.asList(field.name(), field.schema().type().toString()))
          .collect(Collectors.toList());
    } else if (ksqlEntity instanceof TopicDescription) {
      columnHeaders = new ArrayList<>();
      columnHeaders.add("Topic Name");
      columnHeaders.add("Kafka Topic");
      columnHeaders.add("Type");
      List<String> topicInfo = new ArrayList<>();
      TopicDescription topicDescription = (TopicDescription) ksqlEntity;
      topicInfo.add(topicDescription.getName());
      topicInfo.add(topicDescription.getKafkaTopic());
      topicInfo.add(topicDescription.getFormat());
      if (topicDescription.getFormat().equalsIgnoreCase("AVRO")) {
        columnHeaders.add("AvroSchema");
        topicInfo.add(topicDescription.getSchemaString());
      }
      rowValues = Arrays.asList(topicInfo);
    } else if (ksqlEntity instanceof StreamsList) {
      List<StreamsList.StreamInfo> streamInfos = ((StreamsList) ksqlEntity).getStreams();
      columnHeaders = Arrays.asList("Stream Name", "Ksql Topic");
      rowValues = streamInfos.stream()
          .map(streamInfo -> Arrays.asList(streamInfo.getName(), streamInfo.getTopic()))
          .collect(Collectors.toList());
    } else if (ksqlEntity instanceof TablesList) {
      List<TablesList.TableInfo> tableInfos = ((TablesList) ksqlEntity).getTables();
      columnHeaders = Arrays.asList("Table Name", "Ksql Topic", "Statestore", "Windowed");
      rowValues = tableInfos.stream()
          .map(tableInfo -> Arrays.asList(
              tableInfo.getName(),
              tableInfo.getTopic(),
              tableInfo.getStateStoreName(),
              Boolean.toString(tableInfo.getIsWindowed()))
          ).collect(Collectors.toList());
    } else if (ksqlEntity instanceof TopicsList) {
      List<TopicsList.TopicInfo> topicInfos = ((TopicsList) ksqlEntity).getTopics();
      columnHeaders = Arrays.asList("Topic Name", "Kafka Topic", "Format");
      rowValues = topicInfos.stream()
          .map(topicInfo -> Arrays.asList(
              topicInfo.getName(),
              topicInfo.getKafkaTopic(),
              topicInfo.getFormat().name()
          )).collect(Collectors.toList());
    } else if (ksqlEntity instanceof ExecutionPlan) {
      ExecutionPlan executionPlan = (ExecutionPlan) ksqlEntity;
      columnHeaders = Arrays.asList("Execution Plan");
      rowValues = Collections.singletonList(Arrays.asList(
          executionPlan.getExecutionPlan()
      ));
    } else {
      throw new RuntimeException(String.format(
          "Unexpected KsqlEntity class: '%s'",
          ksqlEntity.getClass().getCanonicalName()
      ));
    }
    printTable(columnHeaders, rowValues);
  }

  private void printAsTable(GenericRow row) {
    terminal.writer().println(
        String.join(" | ", row.columns.stream().map(Objects::toString).collect(Collectors.toList()))
    );
    terminal.flush();
  }

  private void printAsTable(CommandStatuses statuses) {
    List<String> columnHeaders = Arrays.asList("Command ID", "Status");
    List<List<String>> rowValues = statuses.entrySet().stream()
        .map(statusEntry -> Arrays.asList(
            statusEntry.getKey().toString(),
            statusEntry.getValue().name()
        )).collect(Collectors.toList());
    printTable(columnHeaders, rowValues);
  }

  private void printAsTable(CommandStatus status) {
    printTable(
        Arrays.asList("Status", "Message"),
        Collections.singletonList(Arrays.asList(
            status.getStatus().name(),
            status.getMessage().split("\n", 2)[0]
        ))
    );
  }

  private void printTable(List<String> columnHeaders, List<List<String>> rowValues) {
    if (columnHeaders.size() == 0) {
      throw new RuntimeException("Cannot print table without columns");
    }

    Integer[] columnLengths = new Integer[columnHeaders.size()];
    int separatorLength = -1;

    for (int i = 0; i < columnLengths.length; i++) {
      int columnLength = columnHeaders.get(i).length();
      for (List<String> row : rowValues) {
        columnLength = Math.max(columnLength, row.get(i).length());
      }
      columnLengths[i] = columnLength;
      separatorLength += columnLength + 3;
    }

    String rowFormatString = constructRowFormatString(columnLengths);

    terminal.writer().printf(rowFormatString, columnHeaders.toArray());

    terminal.writer().println(new String(new char[separatorLength]).replaceAll(".", "-"));
    for (List<String> row : rowValues) {
      terminal.writer().printf(rowFormatString, row.toArray());
    }

    terminal.flush();
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
      ConfigDef.ConfigKey configKey =
          CONSUMER_CONFIG_DEF.configKeys().get(parsedProperty);
      if (configKey == null) {
        throw new IllegalArgumentException(String.format(
            "Invalid consumer property: '%s'",
            parsedProperty
        ));
      }
      type = configKey.type;
    } else if (property.startsWith(StreamsConfig.PRODUCER_PREFIX)) {
      parsedProperty = property.substring(StreamsConfig.PRODUCER_PREFIX.length());
      ConfigDef.ConfigKey configKey =
          PRODUCER_CONFIG_DEF.configKeys().get(parsedProperty);
      if (configKey == null) {
        throw new IllegalArgumentException(String.format(
            "Invalid producer property: '%s'",
            parsedProperty
        ));
      }
      type = configKey.type;
    } else if (property.equalsIgnoreCase(DdlConfig.AVRO_SCHEMA)) {
      localProperties.put(property, value);
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
    Object priorValue = localProperties.put(property, parsedValue);

    terminal.writer().printf(
        "Successfully changed local property '%s' from '%s' to '%s'%n",
        property,
        priorValue,
        parsedValue
    );
    terminal.flush();
  }

  private void unsetProperty(String property) {
    if (localProperties.containsKey(property)) {
      Object value = localProperties.remove(property);
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

  private static String constructRowFormatString(Integer... lengths) {
    List<String> columnFormatStrings = Arrays.stream(lengths)
        .map(Cli::constructSingleColumnFormatString)
        .collect(Collectors.toList());
    return String.format(" %s %n", String.join(" | ", columnFormatStrings));
  }

  private static String constructSingleColumnFormatString(Integer length) {
    return String.format("%%%ds", length);
  }

  protected interface CliSpecificCommand {
    String getName();

    void printHelp();

    void execute(String commandStrippedLine) throws IOException;
  }

  // Have to enable event expansion or multi-line parsing won't work, so a quick 'n dirty workaround
  // will have to do to prevent strings like !! from being expanded by the line reader
  private static class NoOpExpander extends DefaultExpander {
    @Override
    public String expandHistory(History history, String line) {
      return line;
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
