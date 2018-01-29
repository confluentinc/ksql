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

package io.confluent.ksql.cli.console;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jline.reader.EndOfFileException;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ErrorMessage;
import io.confluent.ksql.rest.entity.ErrorMessageEntity;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SchemaMapper;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.util.CliUtils;

public abstract class Console implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Console.class);

  private LineReader lineReader;
  private final ObjectMapper objectMapper;
  private final KsqlRestClient restClient;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;

  private OutputFormat outputFormat;

  public Console(OutputFormat outputFormat, KsqlRestClient restClient) {
    Objects.requireNonNull(
        outputFormat,
        "Must provide the terminal with a beginning output format"
    );
    Objects.requireNonNull(restClient, "Must provide the terminal with a REST client");

    this.outputFormat = outputFormat;
    this.restClient = restClient;

    this.cliSpecificCommands = new LinkedHashMap<>();

    this.objectMapper = new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    new SchemaMapper().registerToObjectMapper(objectMapper);

    registerDefaultCommands();
  }

  public abstract PrintWriter writer();

  public abstract void flush();

  public abstract int getWidth();

  /* jline specific */

  protected abstract LineReader buildLineReader();

  protected abstract void puts(InfoCmp.Capability capability);

  public abstract Terminal.SignalHandler handle(
      Terminal.Signal signal,
      Terminal.SignalHandler signalHandler
  );

  /* public */

  public void addResult(GenericRow row) {
    // do nothing by default, test classes can use this method to obtain typed results
  }

  public void addResult(List<String> columnHeaders, List<List<String>> rowValues) {
    // do nothing by default, test classes can use this method to obtain typed results
  }

  public LinkedHashMap<String, CliSpecificCommand> getCliSpecificCommands() {
    return cliSpecificCommands;
  }

  public LineReader getLineReader() {
    if (lineReader == null) {
      lineReader = buildLineReader();
    }
    return lineReader;
  }

  public void printErrorMessage(ErrorMessage errorMessage) {
    printError(errorMessage.getMessage(), errorMessage.toString());
  }

  public void printError(String shortMsg, String fullMsg) {
    log.error(fullMsg);
    writer().println(shortMsg);
  }

  public void printStreamedRow(StreamedRow row) throws IOException {
    if (row.getErrorMessage() != null) {
      printErrorMessage(row.getErrorMessage());
    } else {
      switch (outputFormat) {
        case JSON:
          printAsJson(row.getRow().getColumns());
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

  public void printKsqlEntityList(List<KsqlEntity> entityList) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(entityList);
        break;
      case TABULAR:
        for (KsqlEntity ksqlEntity : entityList) {
          writer().println();
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

  public void registerCliSpecificCommand(CliSpecificCommand cliSpecificCommand) {
    cliSpecificCommands.put(cliSpecificCommand.getName(), cliSpecificCommand);
  }

  public void setOutputFormat(String newFormat) {
    try {
      outputFormat = OutputFormat.get(newFormat);
      writer().printf("Output format set to %s%n", outputFormat.name());
    } catch (IllegalArgumentException exception) {
      writer().printf(
          "Invalid output format: '%s' (valid formats: %s)%n",
          newFormat,
          OutputFormat.VALID_FORMATS
      );
    }
  }

  /* private */

  private void registerDefaultCommands() {
    registerCliSpecificCommand(new Help());

    registerCliSpecificCommand(new Clear());

    registerCliSpecificCommand(new Output());

    registerCliSpecificCommand(new History());

    registerCliSpecificCommand(new Version());

    registerCliSpecificCommand(new Exit());
  }


  private void printTable(
      List<String> columnHeaders,
      List<List<String>> rowValues,
      List<String> header,
      List<String> footer
  ) {

    header.forEach(m -> writer().println(m));

    if (columnHeaders.size() > 0) {
      addResult(columnHeaders, rowValues);

      Integer[] columnLengths = new Integer[columnHeaders.size()];
      int separatorLength = -1;

      for (int i = 0; i < columnLengths.length; i++) {
        int columnLength = getColumnLength(columnHeaders, rowValues, i);
        columnLengths[i] = columnLength;
        separatorLength += columnLength + 3;
      }

      String rowFormatString = constructRowFormatString(columnLengths);

      writer().printf(rowFormatString, columnHeaders.toArray());

      writer().println(new String(new char[separatorLength]).replaceAll(".", "-"));
      for (List<String> row : rowValues) {
        writer().printf(rowFormatString, row.toArray());
      }
      writer().println(new String(new char[separatorLength]).replaceAll(".", "-"));
    }

    footer.forEach(m -> writer().println(m));

    flush();
  }

  private int getColumnLength(List<String> columnHeaders, List<List<String>> rowValues, int i) {
    int columnLength = columnHeaders.get(i).length();
    for (List<String> row : rowValues) {
      columnLength = Math.max(columnLength, getMultiLineStringLength(row.get(i)));
    }
    return columnLength;
  }

  private int getMultiLineStringLength(String s) {
    String[] split = s.split("\n");
    String longest = Collections.max(
        Arrays.asList(split),
        Comparator.comparing(line -> line.length())
    );
    return longest.length();
  }

  private void printAsTable(KsqlEntity ksqlEntity) {
    List<String> header = new ArrayList<>();
    List<String> footer = new ArrayList<>();
    List<String> columnHeaders = new ArrayList<>();
    List<List<String>> rowValues = new ArrayList<>();

    if (ksqlEntity instanceof CommandStatusEntity) {
      CommandStatusEntity commandStatusEntity = (CommandStatusEntity) ksqlEntity;
      columnHeaders = Arrays.asList("Message");
      CommandStatus commandStatus = commandStatusEntity.getCommandStatus();
      rowValues = Collections.singletonList(Arrays.asList(
          commandStatus.getMessage().split("\n", 2)[0]
      ));
    } else if (ksqlEntity instanceof ErrorMessageEntity) {
      ErrorMessage errorMessage = ((ErrorMessageEntity) ksqlEntity).getErrorMessage();
      printErrorMessage(errorMessage);
      return;
    } else if (ksqlEntity instanceof PropertiesList) {
      PropertiesList
          propertiesList =
          CliUtils.propertiesListWithOverrides(
              (PropertiesList) ksqlEntity,
              restClient.getLocalProperties()
          );
      Map<String, Object> properties = propertiesList.getProperties();
      columnHeaders = Arrays.asList("Property", "Value");
      rowValues = properties.entrySet().stream()
          .map(propertyEntry -> Arrays.asList(
              propertyEntry.getKey(),
              Objects.toString(propertyEntry.getValue())
          )).collect(Collectors.toList());
    } else if (ksqlEntity instanceof Queries) {
      List<Queries.RunningQuery> runningQueries = ((Queries) ksqlEntity).getQueries();
      columnHeaders = Arrays.asList("Query ID", "Kafka Topic", "Query String");
      rowValues = runningQueries.stream()
          .map(runningQuery -> Arrays.asList(
              runningQuery.getId().toString(),
              runningQuery.getKafkaTopic(),
              runningQuery.getQueryString()
          )).collect(Collectors.toList());
      footer.add("For detailed information on a Query run: EXPLAIN <Query ID>;");
    } else if (ksqlEntity instanceof SourceDescription) {
      SourceDescription sourceDescription = (SourceDescription) ksqlEntity;
      List<SourceDescription.FieldSchemaInfo> fields = sourceDescription.getSchema();

      if (!fields.isEmpty()) {
        columnHeaders = Arrays.asList("Field", "Type");
        rowValues = fields.stream()
            .map(field -> Arrays.asList(
                field.getName(),
                formatFieldType(field, sourceDescription.getKey())
            ))
            .collect(Collectors.toList());
      }

      printExtendedInformation(header, footer, sourceDescription);


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
      columnHeaders = Arrays.asList("Stream Name", "Kafka Topic", "Format");
      rowValues = streamInfos.stream()
          .map(streamInfo -> Arrays.asList(streamInfo.getName(), streamInfo.getTopic(),
                                           streamInfo.getFormat()
          ))
          .collect(Collectors.toList());
    } else if (ksqlEntity instanceof TablesList) {
      List<TablesList.TableInfo> tableInfos = ((TablesList) ksqlEntity).getTables();
      columnHeaders = Arrays.asList("Table Name", "Kafka Topic", "Format", "Windowed");
      rowValues = tableInfos.stream()
          .map(tableInfo -> Arrays.asList(
              tableInfo.getName(),
              tableInfo.getTopic(),
              tableInfo.getFormat(),
              Boolean.toString(tableInfo.getIsWindowed())
               )
          ).collect(Collectors.toList());
    } else if (ksqlEntity instanceof KsqlTopicsList) {
      List<KsqlTopicInfo> topicInfos = ((KsqlTopicsList) ksqlEntity).getTopics();
      columnHeaders = Arrays.asList("Ksql Topic", "Kafka Topic", "Format");
      rowValues = topicInfos.stream()
          .map(topicInfo -> Arrays.asList(
              topicInfo.getName(),
              topicInfo.getKafkaTopic(),
              topicInfo.getFormat().name()
          )).collect(Collectors.toList());
    } else if (ksqlEntity instanceof KafkaTopicsList) {
      List<KafkaTopicInfo> topicInfos = ((KafkaTopicsList) ksqlEntity).getTopics();
      columnHeaders = Arrays.asList(
          "Kafka Topic",
          "Registered",
          "Partitions",
          "Partition Replicas",
          "Consumers",
          "Consumer Groups"
      );
      rowValues = topicInfos.stream()
          .map(topicInfo -> Arrays.asList(
              topicInfo.getName(),
              topicInfo.getRegistered(),
              Integer.toString(topicInfo.getPartitionCount()),
              topicInfo.getReplicaInfo(),
              Integer.toString(topicInfo.getConsumerCount()),
              Integer.toString(topicInfo.getConsumerGroupCount())
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
    printTable(columnHeaders, rowValues, header, footer);
  }

  private String formatFieldType(SourceDescription.FieldSchemaInfo field, String keyField) {

    if (field.getName().equals("ROWTIME") || field.getName().equals("ROWKEY")) {
      return String.format("%-16s %s", field.getType(), "(system)");
    } else if (keyField.contains("." + field.getName())) {
      return String.format("%-16s %s", field.getType(), "(key)");
    } else {
      return field.getType();
    }
  }

  private void printExtendedInformation(
      List<String> header,
      List<String> footer,
      SourceDescription source
  ) {
    if (source.isExtended()) {
      header.add(String.format("%-20s : %s", "Type", source.getType()));
      if (source.getStatementText().length() > 0) {
        header.add(String.format("%-20s : %s", "SQL", source.getStatementText()));
      }

      if (!source.getType().equals("QUERY")) {
        header.add(String.format("%-20s : %s", "Key field", source.getKey()));
        header.add(String.format(
            "%-20s : %s",
            "Timestamp field",
            source.getTimestamp().length() == 0
            ? "Not set - using <ROWTIME>"
            : source.getTimestamp()
        ));
        header.add(String.format("%-20s : %s", "Key format", "STRING"));
        header.add(String.format("%-20s : %s", "Value format", source.getSerdes()));
        if (source.getKafkaTopic().length() > 0) {
          header.add(String.format(
              "%-20s : %s (partitions: %d, replication: %d)",
              "Kafka output topic",
              source.getKafkaTopic(),
              source.getPartitions(),
              source.getReplication()
          ));
        }
      }
      header.add("");

      if (!source.getWriteQueries().isEmpty()) {
        footer.add(String.format(
            "\n%-20s\n%-20s",
            "Queries that write into this " + source.getType(),
            "-----------------------------------"
        ));
        for (String writeQuery : source.getWriteQueries()) {
          footer.add(writeQuery);
        }
        footer.add("\nFor query topology and execution plan please run: EXPLAIN <QueryId>");
      }

      footer.add(String.format(
          "\n%-20s\n%s",
          "Local runtime statistics",
          "------------------------"
      ));
      footer.add(source.getStatistics());
      footer.add(source.getErrorStats());
      footer.add(String.format(
          "(%s)",
          "Statistics of the local KSQL server interaction with the Kafka topic "
          + source.getKafkaTopic()
      ));

      if (source.getExecutionPlan().length() > 0) {
        footer.add(String.format(
            "\n%-20s\n%-20s\n%s",
            "Execution plan",
            "--------------",
            source.getExecutionPlan()
        ));
      }
      if (source.getTopology().length() > 0) {
        footer.add(String.format(
            "\n%-20s\n%-20s\n%s",
            "Processing topology",
            "-------------------",
            source.getTopology()
        ));
      }
    } else {
      footer.add("For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;");
    }
  }

  private void printAsTable(GenericRow row) {
    addResult(row);
    writer().println(
        String.join(
            " | ",
            row.getColumns().stream().map(Objects::toString).collect(Collectors.toList())
        )
    );
    flush();
  }

  private void printAsJson(Object o) throws IOException {
    if (o instanceof PropertiesList) {
      o = CliUtils.propertiesListWithOverrides((PropertiesList) o, restClient.getLocalProperties());
    } else if (o instanceof KsqlEntityList) {
      List<KsqlEntity> newEntities = new ArrayList<>();
      for (KsqlEntity ksqlEntity : (KsqlEntityList) o) {
        if (ksqlEntity instanceof PropertiesList) {
          ksqlEntity = CliUtils.propertiesListWithOverrides(
              (PropertiesList) ksqlEntity,
              restClient.getLocalProperties()
          );
        }
        newEntities.add(ksqlEntity);
      }
      o = newEntities;
    } else {
      log.warn(
          "Unexpected result class: '{}' found in printAsJson",
          o.getClass().getCanonicalName()
      );
    }
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(writer(), o);
    writer().println();
    flush();
  }

  private static String constructRowFormatString(Integer... lengths) {
    List<String> columnFormatStrings = Arrays.stream(lengths)
        .map(Console::constructSingleColumnFormatString)
        .collect(Collectors.toList());
    return String.format(" %s %n", String.join(" | ", columnFormatStrings));
  }

  private static String constructSingleColumnFormatString(Integer length) {
    return String.format("%%%ds", (-1 * length));
  }

  private class Help implements CliSpecificCommand {
    @Override
    public String getName() {
      return "help";
    }

    @Override
    public void printHelp() {
      writer().println("help:");
      writer().println("\tShow this message.");
    }

    @Override
    public void execute(String line) {
      writer().println();
      writer().println("Description:");
      writer().println(
          "\tThe KSQL CLI provides a terminal-based interactive shell"
          + " for running queries. Each command must be on a separate line. "
          + "For KSQL command syntax, see the documentation at "
          + "https://github.com/confluentinc/ksql/docs/."
      );
      writer().println();
      for (CliSpecificCommand cliSpecificCommand : cliSpecificCommands.values()) {
        cliSpecificCommand.printHelp();
        writer().println();
      }
      writer().println();
      writer().println("Keyboard shortcuts:");
      writer().println();
      writer().println("    The KSQL CLI supports these keyboard shorcuts:");
      writer().println();
      writer().println("CTRL+D:");
      writer().println("\tEnd your KSQL CLI session.");
      writer().println("CTRL+R:");
      writer().println("\tSearch your command history.");
      writer().println("Up and Down arrow keys:");
      writer().println("\tScroll up or down through your command history.");
      writer().println();
      writer().println("Default behavior:");
      writer().println();
      writer().println(
          "    Lines are read one at a time and are sent to the "
          + "server as KSQL unless one of the following is true:"
      );
      writer().println();
      writer().println(
          "    1. The line is empty or entirely whitespace. In this"
          + " case, no request is made to the server."
      );
      writer().println();
      writer().println(
          "    2. The line ends with backslash ('\\'). In this case, lines are "
          + "continuously read and stripped of their trailing newline and '\\' "
          + "until one is "
          + "encountered that does not end with '\\'; then, the concatenation of "
          + "all lines read "
          + "during this time is sent to the server as KSQL."
      );
      writer().println();
    }
  }

  private class Clear implements CliSpecificCommand {
    @Override
    public String getName() {
      return "clear";
    }

    @Override
    public void printHelp() {
      writer().println("clear:");
      writer().println("\tClear the current terminal.");
    }

    @Override
    public void execute(String commandStrippedLine) throws IOException {
      puts(InfoCmp.Capability.clear_screen);
      flush();
    }
  }

  private class Output implements CliSpecificCommand {

    @Override
    public String getName() {
      return "output";
    }

    @Override
    public void printHelp() {
      writer().println("output:");
      writer().println("\tView the current output format.");
      writer().println("");
      writer().println("output <format>");
      writer().println("");
      writer().printf(
          "\tSet the output format to <format> (valid formats: %s)%n",
          OutputFormat.VALID_FORMATS
      );
      writer().println("\tFor example: \"output JSON\"");
    }

    @Override
    public void execute(String commandStrippedLine) throws IOException {
      String newFormat = commandStrippedLine.trim().toUpperCase();
      if (newFormat.isEmpty()) {
        writer().printf("Current output format: %s%n", outputFormat.name());
      } else {
        setOutputFormat(newFormat);
      }
    }
  }

  private class History implements CliSpecificCommand {
    @Override
    public String getName() {
      return "history";
    }

    @Override
    public void printHelp() {
      writer().println(
          "history:");
      writer().println(
          "\tShow previous lines entered during the current CLI session. You can"
          + " use up and down arrow keys to view previous lines."
      );
    }

    @Override
    public void execute(String commandStrippedLine) throws IOException {
      for (org.jline.reader.History.Entry historyEntry : lineReader.getHistory()) {
        writer().printf("%4d: %s%n", historyEntry.index(), historyEntry.line());
      }
      flush();
    }
  }

  private class Version implements CliSpecificCommand {
    @Override
    public String getName() {
      return "version";
    }

    @Override
    public void printHelp() {
      writer().println("version:");
      writer().println("\tGet the current KSQL version.");
    }

    @Override
    public void execute(String commandStrippedLine) {
      ServerInfo serverInfo = restClient.makeRootRequest().getResponse();
      writer().printf("Version: %s%n", serverInfo.getVersion());
      flush();
    }
  }

  private class Exit implements CliSpecificCommand {
    @Override
    public String getName() {
      return "exit";
    }

    @Override
    public void printHelp() {
      writer().println("exit:");
      writer().println(
          "\tExit the CLI."
      );
    }

    @Override
    public void execute(String commandStrippedLine) throws IOException {
      throw new EndOfFileException();
    }
  }
}
