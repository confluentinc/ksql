/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.parser.KSQLParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.rest.client.KSQLRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandIdEntity;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ErrorMessage;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.KSQLEntity;
import io.confluent.ksql.rest.entity.KSQLEntityList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.RunningQueries;
import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.entity.SetProperty;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicsList;
import io.confluent.ksql.rest.server.computation.CommandId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
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

  private static final String DEFAULT_PRIMARY_PROMPT = "ksql> ";
  private static final String DEFAULT_SECONDARY_PROMPT = "      ";

  private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("'(''|[^'])*'");

  private final ExecutorService queryStreamExecutorService;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;
  private final ObjectMapper objectMapper;

  private final Long streamedQueryRowLimit;
  private final Long streamedQueryTimeoutMs;

  protected final KSQLRestClient restClient;
  protected final Terminal terminal;
  protected final LineReader lineReader;

  private String primaryPrompt;
  private String secondaryPrompt;

  private OutputFormat outputFormat;

  public Cli(
      KSQLRestClient restClient,
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
    this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN); // Ignore ^C when not reading a line

    // TODO: specify a completer to use here via a call to LineReaderBuilder.completer()
    this.lineReader = LineReaderBuilder.builder().appName("KSQL").terminal(terminal).build();

    // Otherwise, things like '!=' will cause things to break
    this.lineReader.setOpt(LineReader.Option.DISABLE_EVENT_EXPANSION);

    this.primaryPrompt = DEFAULT_PRIMARY_PROMPT;
    this.secondaryPrompt = DEFAULT_SECONDARY_PROMPT;

    this.outputFormat = outputFormat;

    this.queryStreamExecutorService = Executors.newSingleThreadExecutor();

    this.cliSpecificCommands = new LinkedHashMap<>();
    registerDefaultCliSpecificCommands();

    this.objectMapper = new ObjectMapper()
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

    new SchemaMapper().registerToObjectMapper(objectMapper);
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
        terminal.writer().println("CLI-specific commands (EACH MUST BE ON THEIR OWN LINE):");
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
      private final String outputFormats = String.format(
          "'%s'",
          String.join(
              "', '",
              Arrays.stream(OutputFormat.values()).map(Object::toString).collect(Collectors.toList())
          )
      );

      @Override
      public String getName() {
        return "output";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\toutput:          View the current output format");
        terminal.writer() .printf("\toutput <format>: Set the output format to <format> (valid formats: %s)%n", outputFormats);
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
            terminal.writer().printf("Invalid output format: '%s' (valid formats: %s)%n", newFormat, outputFormats);
          }
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

  private void handleStatements(String line) throws IOException, InterruptedException, ExecutionException {
    StringBuilder consecutiveStatements = new StringBuilder();
    for (SqlBaseParser.SingleStatementContext statementContext : new KSQLParser().getStatements(line)) {
      String statementText = KSQLEngine.getStatementString(statementContext);
      if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext ||
          statementContext.statement() instanceof SqlBaseParser.PrintTopicContext) {
        if (consecutiveStatements.length() != 0) {
          printKSQLResponse(restClient.makeKSQLRequest(consecutiveStatements.toString()));
          consecutiveStatements = new StringBuilder();
        }
        if (statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
          handleStreamedQuery(statementText);
        } else {
          handlePrintedTopic(statementText);
        }
      } else {
        consecutiveStatements.append(statementText);
      }
    }
    if (consecutiveStatements.length() != 0) {
      printKSQLResponse(restClient.makeKSQLRequest(consecutiveStatements.toString()));
    }
  }

  private void handleStreamedQuery(String query) throws IOException, InterruptedException, ExecutionException {
    RestResponse<KSQLRestClient.QueryStream> queryResponse = restClient.makeQueryRequest(query);

    if (queryResponse.isSuccessful()) {
      displayQueryTerminateInstructions();
      try (KSQLRestClient.QueryStream queryStream = queryResponse.getResponse()) {
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
        eraseQueryTerminateInstructions();
        terminal.writer().println("Query terminated");
        terminal.writer().flush();
      }
    } else {
      printErrorMessage(queryResponse.getErrorMessage());
    }
  }

  private boolean keepReading(long rowsRead) {
    return streamedQueryRowLimit == null || rowsRead < streamedQueryRowLimit;
  }

  private void handlePrintedTopic(String printTopic) throws InterruptedException, ExecutionException, IOException {
    RestResponse<InputStream> topicResponse = restClient.makePrintTopicRequest(printTopic);

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

  private void printKSQLResponse(RestResponse<KSQLEntityList> response) throws IOException {
    if (response.isSuccessful()) {
      printKSQLEntityList(response.getResponse());
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
          throw new RuntimeException(String.format("Unexpected output format: '%s'", outputFormat.name()));
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
        throw new RuntimeException(String.format("Unexpected output format: '%s'", outputFormat.name()));
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
        throw new RuntimeException(String.format("Unexpected output format: '%s'", outputFormat.name()));
    }
  }

  private void printKSQLEntityList(List<KSQLEntity> entityList) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(entityList);
        break;
      case TABULAR:
        for (KSQLEntity ksqlEntity : entityList) {
          terminal.writer().println();
          printAsTable(ksqlEntity);
        }
        break;
      default:
        throw new RuntimeException(String.format("Unexpected output format: '%s'", outputFormat.name()));
    }
  }

  private void printErrorMessage(ErrorMessage errorMessage) {
    terminal.writer().println(errorMessage.getMessage());
  }

  private void printAsJson(Object o) throws IOException {
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(terminal.writer(), o);
    terminal.writer().println();
    terminal.flush();
  }

  private void printAsTable(KSQLEntity ksqlEntity) {
    List<String> columnHeaders;
    List<List<String>> rowValues;
    if (ksqlEntity instanceof CommandIdEntity) {
      CommandId commandId = ((CommandIdEntity) ksqlEntity).getCommandId();
      columnHeaders = Collections.singletonList("Command ID");
      rowValues = Collections.singletonList(Collections.singletonList(commandId.toString()));
    } else if (ksqlEntity instanceof ErrorMessageEntity) {
      ErrorMessage errorMessage = ((ErrorMessageEntity) ksqlEntity).getErrorMessage();
      printErrorMessage(errorMessage);
      return;
    } else if (ksqlEntity instanceof PropertiesList) {
      Map<String, Object> properties = ((PropertiesList) ksqlEntity).getProperties();
      columnHeaders = Arrays.asList("Property", "Value");
      rowValues = properties.entrySet().stream()
          .map(propertyEntry -> Arrays.asList(propertyEntry.getKey(), Objects.toString(propertyEntry.getValue())))
          .collect(Collectors.toList());
    } else if (ksqlEntity instanceof RunningQueries) {
      List<RunningQueries.RunningQuery> runningQueries = ((RunningQueries) ksqlEntity).getRunningQueries();
      columnHeaders = Arrays.asList("ID", "Kafka Topic", "Query String");
      rowValues = runningQueries.stream()
          .map(runningQuery -> Arrays.asList(
              Long.toString(runningQuery.getId()),
              runningQuery.getKafkaTopic(),
              runningQuery.getQueryString())
          ).collect(Collectors.toList());
    } else if (ksqlEntity instanceof SetProperty) {
      SetProperty setProperty = (SetProperty) ksqlEntity;
      columnHeaders = Arrays.asList("Property", "Prior Value", "New Value");
      rowValues = Collections.singletonList(Arrays.asList(
          setProperty.getProperty(),
          Objects.toString(setProperty.getOldValue()),
          Objects.toString(setProperty.getNewValue())
      ));
    } else if (ksqlEntity instanceof SourceDescription) {
      List<Field> fields = ((SourceDescription) ksqlEntity).getSchema().fields();
      columnHeaders = fields.stream().map(Field::name).collect(Collectors.toList());
      rowValues = Collections.singletonList(
          fields.stream().map(Field::schema).map(Schema::type).map(Schema.Type::toString).collect(Collectors.toList())
      );
    } else if (ksqlEntity instanceof StreamsList) {
      List<StreamsList.StreamInfo> streamInfos = ((StreamsList) ksqlEntity).getStreams();
      columnHeaders = Arrays.asList("Stream Name", "KSQL Topic");
      rowValues = streamInfos.stream()
          .map(streamInfo -> Arrays.asList(streamInfo.getName(), streamInfo.getTopic()))
          .collect(Collectors.toList());
    } else if (ksqlEntity instanceof TablesList) {
      List<TablesList.TableInfo> tableInfos = ((TablesList) ksqlEntity).getTables();
      columnHeaders = Arrays.asList("Table Name", "KSQL Topic", "Statestore", "Windowed");
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
          .map(topicInfo -> Arrays.asList(topicInfo.getName(), topicInfo.getKafkaTopic(), topicInfo.getFormat().name()))
          .collect(Collectors.toList());
    } else {
      throw new RuntimeException(
          String.format("Unexpected KSQLEntity class: '%s'", ksqlEntity.getClass().getCanonicalName())
      );
    }
    printTable(columnHeaders, rowValues);
  }

  private void printAsTable(GenericRow row) {
    terminal.writer().println(
        String.join(" | ", row.columns.stream().map(x -> (x == null)? "null" : x.toString()).collect
            (Collectors.toList
            ()))
    );
    terminal.writer().flush();
  }

  private void printAsTable(CommandStatuses statuses) {
    List<String> columnHeaders = Arrays.asList("Command ID", "Status");
    List<List<String>> rowValues = statuses.entrySet().stream()
        .map(statusEntry -> Arrays.asList(statusEntry.getKey().toString(), statusEntry.getValue().name()))
        .collect(Collectors.toList());
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

  private static String constructRowFormatString(Integer... lengths) {
    List<String> columnFormatStrings =
        Arrays.stream(lengths).map(Cli::constructSingleColumnFormatString).collect(Collectors.toList());
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
}
