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

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import org.apache.commons.lang3.StringUtils;
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
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
import io.confluent.ksql.util.StringUtil;

public abstract class Console implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Console.class);
  private static final List<String> PROPERTIES_COLUMN_HEADERS = Arrays.asList("Property", "Value");

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

  public void printErrorMessage(KsqlErrorMessage errorMessage) throws IOException {
    if (errorMessage instanceof KsqlStatementErrorMessage) {
      printKsqlEntityList(((KsqlStatementErrorMessage)errorMessage).getEntities());
    }
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

  private static List<List<String>> propertiesRowValues(Map<String, Object> properties) {
    return properties.entrySet().stream()
        .map(propertyEntry -> Arrays.asList(
            propertyEntry.getKey(),
            Objects.toString(propertyEntry.getValue())
        )).collect(Collectors.toList());
  }


  private void registerDefaultCommands() {
    registerCliSpecificCommand(new Help());

    registerCliSpecificCommand(new Clear());

    registerCliSpecificCommand(new Output());

    registerCliSpecificCommand(new History());

    registerCliSpecificCommand(new Version());

    registerCliSpecificCommand(new Exit());
  }


  private static class Table {
    private final List<String> columnHeaders;
    private final List<List<String>> rowValues;
    private final List<String> header;
    private final List<String> footer;

    public Table(List<String> columnHeaders, List<List<String>> rowValues, List<String> header,
                 List<String> footer) {
      this.columnHeaders = columnHeaders;
      this.rowValues = rowValues;
      this.header = header;
      this.footer = footer;
    }

    public Table(List<String> columnHeaders, List<List<String>> rowValues) {
      this(columnHeaders, rowValues, Collections.emptyList(), Collections.emptyList());
    }

    public static class Builder {
      private final List<String> columnHeaders = new LinkedList<>();
      private final List<List<String>> rowValues = new LinkedList<>();
      private final List<String> header = new LinkedList<>();
      private final List<String> footer = new LinkedList<>();

      Builder() {}

      Table build() {
        return new Table(columnHeaders, rowValues, header, footer);
      }

      Builder withColumnHeaders(List<String> columnHeaders) {
        this.columnHeaders.addAll(columnHeaders);
        return this;
      }

      Builder withColumnHeaders(String... columnHeaders) {
        this.columnHeaders.addAll(Arrays.asList(columnHeaders));
        return this;
      }

      Builder withRows(List<List<String>> rowValues) {
        this.rowValues.addAll(rowValues);
        return this;
      }

      Builder withRow(String... row) {
        this.rowValues.add(Arrays.asList(row));
        return this;
      }

      Builder withRow(List<String> row) {
        this.rowValues.add(row);
        return this;
      }

      Builder withHeaderLine(String headerLine) {
        this.header.add(headerLine);
        return this;
      }

      Builder withFooterLine(String footerLine) {
        this.footer.add(footerLine);
        return this;
      }
    }

    private int getMultiLineStringLength(String multiLineString) {
      String[] split = multiLineString.split("\n");
      return Arrays.asList(split)
          .stream()
          .mapToInt(String::length)
          .max()
          .orElse(0);
    }

    private int getColumnLength(List<String> columnHeaders, List<List<String>> rowValues, int i) {
      return Math.max(
          columnHeaders.get(i).length(),
          rowValues
              .stream()
              .mapToInt(r -> getMultiLineStringLength(r.get(i)))
              .max()
              .orElse(0));
    }

    public void print(final Console console) {

      header.forEach(m -> console.writer().println(m));

      if (columnHeaders.size() > 0) {
        console.addResult(columnHeaders, rowValues);

        Integer[] columnLengths = new Integer[columnHeaders.size()];
        int separatorLength = -1;

        for (int i = 0; i < columnLengths.length; i++) {
          int columnLength = getColumnLength(columnHeaders, rowValues, i);
          columnLengths[i] = columnLength;
          separatorLength += columnLength + 3;
        }

        String rowFormatString = constructRowFormatString(columnLengths);

        console.writer().printf(rowFormatString, columnHeaders.toArray());

        String separator = StringUtils.repeat('-', separatorLength);
        console.writer().println(separator);
        for (List<String> row : rowValues) {
          console.writer().printf(rowFormatString, row.toArray());
        }
        console.writer().println(separator);
      }

      footer.forEach(m -> console.writer().println(m));

      console.flush();
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

  private void printAsTable(KsqlEntity ksqlEntity) {
    Table.Builder tableBuilder = new Table.Builder();

    if (ksqlEntity instanceof CommandStatusEntity) {
      CommandStatusEntity commandStatusEntity = (CommandStatusEntity) ksqlEntity;
      CommandStatus commandStatus = commandStatusEntity.getCommandStatus();
      tableBuilder
          .withColumnHeaders("Message")
          .withRow(commandStatus.getMessage().split("\n", 2)[0]);
    } else if (ksqlEntity instanceof PropertiesList) {
      PropertiesList
          propertiesList =
          CliUtils.propertiesListWithOverrides(
              (PropertiesList) ksqlEntity,
              restClient.getLocalProperties()
          );
      Map<String, Object> properties = propertiesList.getProperties();
      tableBuilder
          .withColumnHeaders(PROPERTIES_COLUMN_HEADERS)
          .withRows(propertiesRowValues(properties));
    } else if (ksqlEntity instanceof Queries) {
      List<Queries.RunningQuery> runningQueries = ((Queries) ksqlEntity).getQueries();
      tableBuilder.withColumnHeaders("Query ID", "Kafka Topic", "Query String");
      runningQueries.forEach(
          r -> tableBuilder.withRow(r.getId().toString(), r.getKafkaTopic(), r.getQueryString()));
      tableBuilder.withFooterLine("For detailed information on a Query run: EXPLAIN <Query ID>;");
    } else if (ksqlEntity instanceof SourceDescription) {
      SourceDescription sourceDescription = (SourceDescription) ksqlEntity;
      printExtendedInformation(sourceDescription);
      return;
    } else if (ksqlEntity instanceof TopicDescription) {
      tableBuilder.withColumnHeaders("Topic Name", "Kafka Topic", "Type");
      List<String> topicInfo = new ArrayList<>();
      TopicDescription topicDescription = (TopicDescription) ksqlEntity;
      topicInfo.add(topicDescription.getName());
      topicInfo.add(topicDescription.getKafkaTopic());
      topicInfo.add(topicDescription.getFormat());
      if (topicDescription.getFormat().equalsIgnoreCase("AVRO")) {
        tableBuilder.withColumnHeaders("AvroSchema");
        topicInfo.add(topicDescription.getSchemaString());
      }
      tableBuilder.withRow(topicInfo);
    } else if (ksqlEntity instanceof StreamsList) {
      tableBuilder.withColumnHeaders("Stream Name", "Kafka Topic", "Format");
      ((StreamsList) ksqlEntity).getStreams().forEach(
          s -> tableBuilder.withRow(s.getName(), s.getTopic(), s.getFormat()));
    } else if (ksqlEntity instanceof TablesList) {
      tableBuilder.withColumnHeaders("Table Name", "Kafka Topic", "Format", "Windowed");
      ((TablesList) ksqlEntity).getTables().forEach(
          t -> tableBuilder.withRow(t.getName(), t.getTopic(), t.getFormat(),
              Boolean.toString(t.getIsWindowed())));
    } else if (ksqlEntity instanceof KsqlTopicsList) {
      tableBuilder.withColumnHeaders("Ksql Topic", "Kafka Topic", "Format");
      ((KsqlTopicsList) ksqlEntity).getTopics().forEach(
          t -> tableBuilder.withRow(t.getName(), t.getKafkaTopic(), t.getFormat().name()));
    } else if (ksqlEntity instanceof KafkaTopicsList) {
      tableBuilder.withColumnHeaders(
          "Kafka Topic",
          "Registered",
          "Partitions",
          "Partition Replicas",
          "Consumers",
          "ConsumerGroups");
      ((KafkaTopicsList) ksqlEntity).getTopics().forEach(
          t -> tableBuilder.withRow(
              t.getName(),
              Boolean.toString(t.getRegistered()),
              Integer.toString(t.getReplicaInfo().size()),
              getTopicReplicaInfo(t.getReplicaInfo()),
              Integer.toString(t.getConsumerCount()),
              Integer.toString(t.getConsumerGroupCount())));
    } else if (ksqlEntity instanceof ExecutionPlan) {
      ExecutionPlan executionPlan = (ExecutionPlan) ksqlEntity;
      tableBuilder.withColumnHeaders("Execution Plan");
      tableBuilder.withRow(executionPlan.getExecutionPlan());
    } else {
      throw new RuntimeException(String.format(
          "Unexpected KsqlEntity class: '%s'",
          ksqlEntity.getClass().getCanonicalName()
      ));
    }
    tableBuilder.build().print(this);
  }

  /**
   * Pretty print replica info.
   * @param replicaSizes list of replicas per partition
   * @return single value if all values are equal, else a csv representation
   */
  private static String getTopicReplicaInfo(List<Integer> replicaSizes) {
    if (replicaSizes.isEmpty()) {
      return "0";
    } else if (replicaSizes.stream().distinct().limit(2).count() <= 1) {
      return String.valueOf(replicaSizes.get(0));
    } else {
      return StringUtil.join(", ", replicaSizes);
    }
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

  private void printSchema(SourceDescription source) {
    Table.Builder tableBuilder = new Table.Builder();
    List<SourceDescription.FieldSchemaInfo> fields = source.getSchema();
    if (!fields.isEmpty()) {
      tableBuilder.withColumnHeaders("Field", "Type");
      fields.forEach(
          f -> tableBuilder.withRow(f.getName(), formatFieldType(f, source.getKey())));
      tableBuilder.build().print(this);
    }
  }

  private void printQueryInfo(SourceDescription source) {
    if (!"QUERY".equals(source.getType())) {
      writer().println(String.format("%-20s : %s", "Key field", source.getKey()));
      writer().println(String.format(
          "%-20s : %s",
          "Timestamp field",
          source.getTimestamp().isEmpty()
              ? "Not set - using <ROWTIME>"
              : source.getTimestamp()
      ));
      writer().println(String.format("%-20s : %s", "Key format", "STRING"));
      writer().println(String.format("%-20s : %s", "Value format", source.getSerdes()));
      if (!source.getTopic().isEmpty()) {
        writer().println(String.format(
            "%-20s : %s (partitions: %d, replication: %d)",
            "Kafka output topic",
            source.getTopic(),
            source.getPartitions(),
            source.getReplication()
        ));
      }
    }
  }

  private void printWriteQueries(SourceDescription source) {
    if (!source.getWriteQueries().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s",
          "Queries that write into this " + source.getType(),
          "-----------------------------------"
      ));
      for (String writeQuery : source.getWriteQueries()) {
        writer().println(writeQuery);
      }
      writer().println("\nFor query topology and execution plan please run: EXPLAIN <QueryId>");
    }
  }

  private void printExecutionPlan(SourceDescription source) {
    if (!source.getExecutionPlan().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s\n%s",
          "Execution plan",
          "--------------",
          source.getExecutionPlan()
      ));
    }
  }

  private void printTopology(SourceDescription source) {
    if (!source.getTopology().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s\n%s",
          "Processing topology",
          "-------------------",
          source.getTopology()
      ));
    }
  }

  private void printOverriddenProperties(SourceDescription source) {
    if (source.getOverriddenProperties().size() > 0) {
      new Table.Builder()
          .withHeaderLine(String.format(
              "\n%-20s\n%-20s",
              "Overridden Properties",
              "---------------------"))
          .withColumnHeaders(PROPERTIES_COLUMN_HEADERS)
          .withRows(propertiesRowValues(source.getOverriddenProperties()))
          .build()
          .print(this);
    }
  }

  private void printExtendedInformation(
      SourceDescription source
  ) {
    if (!source.isExtended()) {
      printSchema(source);
      writer().println(
          "For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;");
      return;
    }
    writer().println(String.format("%-20s : %s", "Type", source.getType()));
    if (source.getStatementText().length() > 0) {
      writer().println(String.format("%-20s : %s", "SQL", source.getStatementText()));
    }

    printQueryInfo(source);
    writer().println("");

    printSchema(source);

    printWriteQueries(source);

    writer().println(String.format(
        "\n%-20s\n%s",
        "Local runtime statistics",
        "------------------------"
    ));
    writer().println(source.getStatistics());
    writer().println(source.getErrorStats());
    writer().println(String.format(
        "(%s)",
        "Statistics of the local KSQL server interaction with the Kafka topic "
            + source.getTopic()
    ));

    printExecutionPlan(source);

    printTopology(source);

    printOverriddenProperties(source);
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
      ServerInfo serverInfo = restClient.getServerInfo().getResponse();
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
