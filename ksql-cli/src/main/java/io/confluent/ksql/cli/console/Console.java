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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.util.StringUtil;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.EndOfFileException;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Console implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Console.class);
  private static final List<String> PROPERTIES_COLUMN_HEADERS = Arrays.asList("Property", "Value");

  private LineReader lineReader;
  private final ObjectMapper objectMapper;
  private final KsqlRestClient restClient;
  private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;

  private OutputFormat outputFormat;

  public Console(final OutputFormat outputFormat, final KsqlRestClient restClient) {
    Objects.requireNonNull(
        outputFormat,
        "Must provide the terminal with a beginning output format"
    );
    Objects.requireNonNull(restClient, "Must provide the terminal with a REST client");

    this.outputFormat = outputFormat;
    this.restClient = restClient;

    this.cliSpecificCommands = new LinkedHashMap<>();

    this.objectMapper = JsonMapper.INSTANCE.mapper;

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

  public void addResult(final GenericRow row) {
    // do nothing by default, test classes can use this method to obtain typed results
  }

  public void addResult(final List<String> columnHeaders, final List<List<String>> rowValues) {
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

  public void printErrorMessage(final KsqlErrorMessage errorMessage) throws IOException {
    if (errorMessage instanceof KsqlStatementErrorMessage) {
      printKsqlEntityList(((KsqlStatementErrorMessage)errorMessage).getEntities());
    }
    printError(errorMessage.getMessage(), errorMessage.toString());
  }

  public void printError(final String shortMsg, final String fullMsg) {
    log.error(fullMsg);
    writer().println(shortMsg);
  }

  public void printStreamedRow(final StreamedRow row) throws IOException {
    if (row.getErrorMessage() != null) {
      printErrorMessage(row.getErrorMessage());
      return;
    }

    if (row.getFinalMessage() != null) {
      writer().println(row.getFinalMessage());
      return;
    }

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

  public void printKsqlEntityList(final List<KsqlEntity> entityList) throws IOException {
    switch (outputFormat) {
      case JSON:
        printAsJson(entityList);
        break;
      case TABULAR:
        for (final KsqlEntity ksqlEntity : entityList) {
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

  public void registerCliSpecificCommand(final CliSpecificCommand cliSpecificCommand) {
    cliSpecificCommands.put(cliSpecificCommand.getName(), cliSpecificCommand);
  }

  public void setOutputFormat(final String newFormat) {
    try {
      outputFormat = OutputFormat.get(newFormat);
      writer().printf("Output format set to %s%n", outputFormat.name());
    } catch (final IllegalArgumentException exception) {
      writer().printf(
          "Invalid output format: '%s' (valid formats: %s)%n",
          newFormat,
          OutputFormat.VALID_FORMATS
      );
    }
  }

  public OutputFormat getOutputFormat() {
    return outputFormat;
  }

  /* private */

  private static List<List<String>> propertiesRowValues(final Map<String, Object> properties) {
    return properties.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(
            propertyEntry -> Arrays.asList(
                propertyEntry.getKey(),
                Objects.toString(propertyEntry.getValue())
            ))
        .collect(Collectors.toList());
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

    Table(final List<String> columnHeaders, final List<List<String>> rowValues,
                 final List<String> header, final List<String> footer) {
      this.columnHeaders = columnHeaders;
      this.rowValues = rowValues;
      this.header = header;
      this.footer = footer;
    }

    Table(final List<String> columnHeaders, final List<List<String>> rowValues) {
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

      Builder withColumnHeaders(final List<String> columnHeaders) {
        this.columnHeaders.addAll(columnHeaders);
        return this;
      }

      Builder withColumnHeaders(final String... columnHeaders) {
        this.columnHeaders.addAll(Arrays.asList(columnHeaders));
        return this;
      }

      Builder withRows(final List<List<String>> rowValues) {
        this.rowValues.addAll(rowValues);
        return this;
      }

      Builder withRow(final String... row) {
        this.rowValues.add(Arrays.asList(row));
        return this;
      }

      Builder withRow(final List<String> row) {
        this.rowValues.add(row);
        return this;
      }

      Builder withHeaderLine(final String headerLine) {
        this.header.add(headerLine);
        return this;
      }

      Builder withFooterLine(final String footerLine) {
        this.footer.add(footerLine);
        return this;
      }
    }

    private int getMultiLineStringLength(final String multiLineString) {
      final String[] split = multiLineString.split("\n");
      return Arrays.asList(split)
          .stream()
          .mapToInt(String::length)
          .max()
          .orElse(0);
    }

    private int getColumnLength(final List<String> columnHeaders,
        final List<List<String>> rowValues,
        final int i) {
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

        final Integer[] columnLengths = new Integer[columnHeaders.size()];
        int separatorLength = -1;

        for (int i = 0; i < columnLengths.length; i++) {
          final int columnLength = getColumnLength(columnHeaders, rowValues, i);
          columnLengths[i] = columnLength;
          separatorLength += columnLength + 3;
        }

        final String rowFormatString = constructRowFormatString(columnLengths);

        console.writer().printf(rowFormatString, columnHeaders.toArray());

        final String separator = StringUtils.repeat('-', separatorLength);
        console.writer().println(separator);
        for (final List<String> row : rowValues) {
          console.writer().printf(rowFormatString, row.toArray());
        }
        console.writer().println(separator);
      }

      footer.forEach(m -> console.writer().println(m));

      console.flush();
    }
  }

  private void printAsTable(final GenericRow row) {
    addResult(row);
    writer().println(
        String.join(
            " | ",
            row.getColumns().stream().map(Objects::toString).collect(Collectors.toList())
        )
    );
    flush();
  }

  private void printAsTable(final KsqlEntity ksqlEntity) {
    final Table.Builder tableBuilder = new Table.Builder();

    if (ksqlEntity instanceof CommandStatusEntity) {
      final CommandStatusEntity commandStatusEntity = (CommandStatusEntity) ksqlEntity;
      final CommandStatus commandStatus = commandStatusEntity.getCommandStatus();
      tableBuilder
          .withColumnHeaders("Message")
          .withRow(commandStatus.getMessage().split("\n", 2)[0]);
    } else if (ksqlEntity instanceof PropertiesList) {
      final Map<String, Object> properties = CliUtils.propertiesListWithOverrides(
          (PropertiesList) ksqlEntity);
      tableBuilder
          .withColumnHeaders(PROPERTIES_COLUMN_HEADERS)
          .withRows(propertiesRowValues(properties));
    } else if (ksqlEntity instanceof Queries) {
      final List<RunningQuery> runningQueries = ((Queries) ksqlEntity).getQueries();
      tableBuilder.withColumnHeaders("Query ID", "Kafka Topic", "Query String");
      runningQueries.forEach(
          r -> tableBuilder.withRow(
              r.getId().getId(), String.join(",", r.getSinks()), r.getQueryString()));
      tableBuilder.withFooterLine("For detailed information on a Query run: EXPLAIN <Query ID>;");
    } else if (ksqlEntity instanceof SourceDescriptionEntity) {
      final SourceDescriptionEntity sourceDescriptionEntity = (SourceDescriptionEntity) ksqlEntity;
      printSourceDescription(sourceDescriptionEntity.getSourceDescription());
      return;
    } else if (ksqlEntity instanceof SourceDescriptionList) {
      printSourceDescriptionList((SourceDescriptionList) ksqlEntity);
      return;
    } else if (ksqlEntity instanceof QueryDescriptionEntity) {
      final QueryDescriptionEntity queryDescriptionEntity = (QueryDescriptionEntity) ksqlEntity;
      printQueryDescription(queryDescriptionEntity.getQueryDescription());
      return;
    } else if (ksqlEntity instanceof QueryDescriptionList) {
      printQueryDescriptionList((QueryDescriptionList) ksqlEntity);
      return;
    } else if (ksqlEntity instanceof TopicDescription) {
      tableBuilder.withColumnHeaders("Topic Name", "Kafka Topic", "Type");
      final List<String> topicInfo = new ArrayList<>();
      final TopicDescription topicDescription = (TopicDescription) ksqlEntity;
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
      final ExecutionPlan executionPlan = (ExecutionPlan) ksqlEntity;
      tableBuilder.withColumnHeaders("Execution Plan");
      tableBuilder.withRow(executionPlan.getExecutionPlan());
    } else if (ksqlEntity instanceof FunctionNameList) {
      tableBuilder.withColumnHeaders("Function Name", "Type");
      ((FunctionNameList) ksqlEntity)
          .getFunctions().stream().sorted()
          .forEach(func -> tableBuilder.withRow(
              Arrays.asList(func.getName(), func.getType().name().toUpperCase())));
    } else if (ksqlEntity instanceof FunctionDescriptionList) {
      printFunctionDescription((FunctionDescriptionList) ksqlEntity);
      return;
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
  private static String getTopicReplicaInfo(final List<Integer> replicaSizes) {
    if (replicaSizes.isEmpty()) {
      return "0";
    } else if (replicaSizes.stream().distinct().limit(2).count() <= 1) {
      return String.valueOf(replicaSizes.get(0));
    } else {
      return StringUtil.join(", ", replicaSizes);
    }
  }

  private String schemaToTypeString(final SchemaInfo schema) {
    // For now just dump the whole type out into 1 string.
    // In the future we should consider a more readable format
    switch (schema.getType()) {
      case ARRAY:
        return new StringBuilder()
            .append(SchemaInfo.Type.ARRAY.name()).append("<")
            .append(schemaToTypeString(schema.getMemberSchema().get()))
            .append(">")
            .toString();
      case MAP:
        return new StringBuilder()
            .append(SchemaInfo.Type.MAP.name())
            .append("<").append(SchemaInfo.Type.STRING).append(", ")
            .append(schemaToTypeString(schema.getMemberSchema().get()))
            .append(">")
            .toString();
      case STRUCT:
        return schema.getFields().get()
            .stream()
            .map(f -> f.getName() + " " + schemaToTypeString(f.getSchema()))
            .collect(Collectors.joining(", ", SchemaInfo.Type.STRUCT.name() + "<", ">"));
      case STRING:
        return "VARCHAR(STRING)";
      default:
        return schema.getType().name();
    }
  }

  private String formatFieldType(final FieldInfo field, final String keyField) {

    if (field.getName().equals("ROWTIME") || field.getName().equals("ROWKEY")) {
      return String.format("%-16s %s", schemaToTypeString(field.getSchema()), "(system)");
    } else if (keyField != null && keyField.contains("." + field.getName())) {
      return String.format("%-16s %s", schemaToTypeString(field.getSchema()), "(key)");
    } else {
      return schemaToTypeString(field.getSchema());
    }
  }

  private void printSchema(final List<FieldInfo> fields, final String keyField) {
    final Table.Builder tableBuilder = new Table.Builder();
    if (!fields.isEmpty()) {
      tableBuilder.withColumnHeaders("Field", "Type");
      fields.forEach(
          f -> tableBuilder.withRow(f.getName(), formatFieldType(f, keyField)));
      tableBuilder.build().print(this);
    }
  }

  private void printTopicInfo(final SourceDescription source) {
    final String timestamp = source.getTimestamp().isEmpty()
                             ? "Not set - using <ROWTIME>"
                             : source.getTimestamp();

    writer().println(String.format("%-20s : %s", "Key field", source.getKey()));
    writer().println(String.format("%-20s : %s", "Key format", "STRING"));
    writer().println(String.format("%-20s : %s", "Timestamp field", timestamp));
    writer().println(String.format("%-20s : %s", "Value format", source.getFormat()));

    if (!source.getTopic().isEmpty()) {
      writer().println(String.format(
          "%-20s : %s (partitions: %d, replication: %d)",
          "Kafka topic",
          source.getTopic(),
          source.getPartitions(),
          source.getReplication()
      ));
    }
  }

  private void printWriteQueries(final SourceDescription source) {
    if (!source.getWriteQueries().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s",
          "Queries that write into this " + source.getType(),
          "-----------------------------------"
      ));
      for (final RunningQuery writeQuery : source.getWriteQueries()) {
        writer().println(writeQuery.getId().getId() + " : " + writeQuery.getQueryString());
      }
      writer().println("\nFor query topology and execution plan please run: EXPLAIN <QueryId>");
    }
  }

  private void printExecutionPlan(final QueryDescription queryDescription) {
    if (!queryDescription.getExecutionPlan().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s\n%s",
          "Execution plan",
          "--------------",
          queryDescription.getExecutionPlan()
      ));
    }
  }

  private void printTopology(final QueryDescription queryDescription) {
    if (!queryDescription.getTopology().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s\n%s",
          "Processing topology",
          "-------------------",
          queryDescription.getTopology()
      ));
    }
  }

  private void printOverriddenProperties(final QueryDescription queryDescription) {
    if (queryDescription.getOverriddenProperties().size() > 0) {
      new Table.Builder()
          .withHeaderLine(String.format(
              "\n%-20s\n%-20s",
              "Overridden Properties",
              "---------------------"))
          .withColumnHeaders(PROPERTIES_COLUMN_HEADERS)
          .withRows(propertiesRowValues(queryDescription.getOverriddenProperties()))
          .build()
          .print(this);
    }
  }

  private void printSourceDescription(final SourceDescription source) {
    writer().println(String.format("%-20s : %s", "Name", source.getName()));
    if (!source.isExtended()) {
      printSchema(source.getFields(), source.getKey());
      writer().println(
          "For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;");
      return;
    }
    writer().println(String.format("%-20s : %s", "Type", source.getType()));

    printTopicInfo(source);
    writer().println("");

    printSchema(source.getFields(), source.getKey());

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
  }

  private void printSourceDescriptionList(final SourceDescriptionList sourceDescriptionList) {
    sourceDescriptionList.getSourceDescriptions().forEach(
        sourceDescription -> {
          printSourceDescription(sourceDescription);
          writer().println();
        });
  }

  private void printQuerySources(final QueryDescription query) {
    if (!query.getSources().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s",
          "Sources that this query reads from: ",
          "-----------------------------------"
      ));
      for (final String sources : query.getSources()) {
        writer().println(sources);
      }
      writer().println("\nFor source description please run: DESCRIBE [EXTENDED] <SourceId>");
    }
  }

  private void printQuerySinks(final QueryDescription query) {
    if (!query.getSinks().isEmpty()) {
      writer().println(String.format(
          "\n%-20s\n%-20s",
          "Sinks that this query writes to: ",
          "-----------------------------------"
      ));
      for (final String sinks : query.getSinks()) {
        writer().println(sinks);
      }
      writer().println("\nFor sink description please run: DESCRIBE [EXTENDED] <SinkId>");
    }
  }

  private void printQueryDescription(final QueryDescription query) {
    writer().println(String.format("%-20s : %s", "ID", query.getId().getId()));
    if (query.getStatementText().length() > 0) {
      writer().println(String.format("%-20s : %s", "SQL", query.getStatementText()));
    }
    writer().println();
    printSchema(query.getFields(), "");
    printQuerySources(query);
    printQuerySinks(query);
    printExecutionPlan(query);
    printTopology(query);
    printOverriddenProperties(query);
  }

  private void printQueryDescriptionList(final QueryDescriptionList queryDescriptionList) {
    queryDescriptionList.getQueryDescriptions().forEach(
        queryDescription -> {
          printQueryDescription(queryDescription);
          writer().println();
        });
  }

  private void printFunctionDescription(final FunctionDescriptionList describeFunction) {
    writer().printf("%-12s: %s%n", "Name", describeFunction.getName().toUpperCase());
    writer().printf("%-12s: %s%n", "Author", describeFunction.getAuthor());
    writer().printf("%-12s: %s%n", "Version", describeFunction.getVersion());
    writer().printf("%-12s: %s%n", "Overview", describeFunction.getDescription());
    writer().printf("%-12s: %s%n", "Type", describeFunction.getType().name());
    writer().printf("%-12s: %s%n", "Jar", describeFunction.getPath());
    writer().printf("%-12s: %n", "Variations");
    final Collection<FunctionInfo> functions = describeFunction.getFunctions();
    functions.forEach(functionInfo -> {
          writer().printf("%n\t%-12s: %s%n",
              "Arguments",
              functionInfo.getArgumentTypes()
                  .toString()
                  .replaceAll("\\[", "")
                  .replaceAll("]", ""));
          writer().printf("\t%-12s: %s%n", "Returns", functionInfo.getReturnType());
          writer().printf("\t%-12s: %s%n", "Description", functionInfo.getDescription());
        }
    );
  }

  private void printAsJson(final Object o) throws IOException {
    if (!((o instanceof PropertiesList || (o instanceof KsqlEntityList)))) {
      log.warn(
          "Unexpected result class: '{}' found in printAsJson",
          o.getClass().getCanonicalName()
      );
    }
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(writer(), o);
    writer().println();
    flush();
  }

  private static String constructRowFormatString(final Integer... lengths) {
    final List<String> columnFormatStrings = Arrays.stream(lengths)
        .map(Console::constructSingleColumnFormatString)
        .collect(Collectors.toList());
    return String.format(" %s %n", String.join(" | ", columnFormatStrings));
  }

  private static String constructSingleColumnFormatString(final Integer length) {
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
    public void execute(final String line) {
      writer().println();
      writer().println("Description:");
      writer().println(
          "\tThe KSQL CLI provides a terminal-based interactive shell"
          + " for running queries. Each command must be on a separate line. "
          + "For KSQL command syntax, see the documentation at "
          + "https://github.com/confluentinc/ksql/docs/."
      );
      writer().println();
      for (final CliSpecificCommand cliSpecificCommand : cliSpecificCommands.values()) {
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
    public void execute(final String commandStrippedLine) throws IOException {
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
    public void execute(final String commandStrippedLine) {
      final String newFormat = commandStrippedLine.trim().toUpperCase();
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
    public void execute(final String commandStrippedLine) {
      for (final org.jline.reader.History.Entry historyEntry : lineReader.getHistory()) {
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
    public void execute(final String commandStrippedLine) {
      final ServerInfo serverInfo = restClient.getServerInfo().getResponse();
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
    public void execute(final String commandStrippedLine) throws IOException {
      throw new EndOfFileException();
    }
  }
}
