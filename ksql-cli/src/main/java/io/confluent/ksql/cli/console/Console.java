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

package io.confluent.ksql.cli.console;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.cli.console.cmd.CliCommandRegisterUtil;
import io.confluent.ksql.cli.console.cmd.CliSpecificCommand;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.builder.CommandStatusTableBuilder;
import io.confluent.ksql.cli.console.table.builder.ExecutionPlanTableBuilder;
import io.confluent.ksql.cli.console.table.builder.FunctionNameListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.KafkaTopicsListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.KsqlTopicsListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.PropertiesListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.QueriesTableBuilder;
import io.confluent.ksql.cli.console.table.builder.StreamsListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.TableBuilder;
import io.confluent.ksql.cli.console.table.builder.TablesListTableBuilder;
import io.confluent.ksql.cli.console.table.builder.TopicDescriptionTableBuilder;
import io.confluent.ksql.rest.client.KsqlRestClient;
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
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.util.HandlerMap;
import io.confluent.ksql.util.HandlerMap.Handler;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public abstract class Console implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(Console.class);

  private static final HandlerMap<KsqlEntity, Console> PRINT_HANDLERS =
      HandlerMap.<KsqlEntity, Console>builder()
          .put(CommandStatusEntity.class,
              tablePrinter(CommandStatusTableBuilder.class, CommandStatusEntity.class))
          .put(PropertiesList.class,
              tablePrinter(PropertiesListTableBuilder.class, PropertiesList.class))
          .put(Queries.class,
              tablePrinter(QueriesTableBuilder.class, Queries.class))
          .put(SourceDescriptionEntity.class,
              (console, entity) -> console.printSourceDescription(entity.getSourceDescription()))
          .put(SourceDescriptionList.class,
              Console::printSourceDescriptionList)
          .put(QueryDescriptionEntity.class,
              (console, entity) -> console.printQueryDescription(entity.getQueryDescription()))
          .put(QueryDescriptionList.class,
              Console::printQueryDescriptionList)
          .put(TopicDescription.class,
              tablePrinter(TopicDescriptionTableBuilder.class, TopicDescription.class))
          .put(StreamsList.class,
              tablePrinter(StreamsListTableBuilder.class, StreamsList.class))
          .put(TablesList.class,
              tablePrinter(TablesListTableBuilder.class, TablesList.class))
          .put(KsqlTopicsList.class,
              tablePrinter(KsqlTopicsListTableBuilder.class, KsqlTopicsList.class))
          .put(KafkaTopicsList.class,
              tablePrinter(KafkaTopicsListTableBuilder.class, KafkaTopicsList.class))
          .put(ExecutionPlan.class,
              tablePrinter(ExecutionPlanTableBuilder.class, ExecutionPlan.class))
          .put(FunctionNameList.class,
              tablePrinter(FunctionNameListTableBuilder.class, FunctionNameList.class))
          .put(FunctionDescriptionList.class,
              Console::printFunctionDescription)
          .build();

  private static <T extends KsqlEntity> Handler<KsqlEntity, Console> tablePrinter(
      final Class<? extends TableBuilder<T>> tableBuilderType,
      final Class<T> entityType) {

    try {
      final TableBuilder<T> tableBuilder = tableBuilderType.newInstance();

      return (console, type) -> {
        final Table table = tableBuilder.buildTable(entityType.cast(type));
        table.print(console);
      };
    } catch (final Exception e) {
      throw new IllegalStateException("Error instantiating tableBuilder: " + tableBuilderType);
    }
  }

  private LineReader lineReader;
  private final ObjectMapper objectMapper;
  private final Map<String, CliSpecificCommand> cliSpecificCommands;

  private OutputFormat outputFormat;

  public Console(final OutputFormat outputFormat, final KsqlRestClient restClient) {
    Objects.requireNonNull(
        outputFormat,
        "Must provide the terminal with a beginning output format"
    );
    Objects.requireNonNull(restClient, "Must provide the terminal with a REST client");

    this.outputFormat = outputFormat;

    this.cliSpecificCommands = Maps.newLinkedHashMap();

    this.objectMapper = new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

    final Supplier<String> versionSuppler =
        () -> restClient.getServerInfo().getResponse().getVersion();
    CliCommandRegisterUtil.registerDefaultCommands(this, versionSuppler);
  }

  public abstract PrintWriter writer();

  public abstract void flush();

  public abstract int getWidth();

  /* jline specific */

  protected abstract LineReader buildLineReader();

  public abstract void puts(InfoCmp.Capability capability);

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

  public Map<String, CliSpecificCommand> getCliSpecificCommands() {
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

  private void printAsTable(final KsqlEntity entity) {
    final Handler<KsqlEntity, Console> handler = PRINT_HANDLERS.get(entity.getClass());
    
    if (handler == null) {
      throw new RuntimeException(String.format(
          "Unexpected KsqlEntity class: '%s'", entity.getClass().getCanonicalName()
      ));
    }
    
    handler.handle(this, entity);
  }

  private String schemaToTypeString(final SchemaInfo schema) {
    // For now just dump the whole type out into 1 string.
    // In the future we should consider a more readable format
    switch (schema.getType()) {
      case ARRAY:
        return SchemaInfo.Type.ARRAY.name() + "<"
               + schemaToTypeString(schema.getMemberSchema().get())
               + ">";
      case MAP:
        return SchemaInfo.Type.MAP.name()
               + "<" + SchemaInfo.Type.STRING + ", "
               + schemaToTypeString(schema.getMemberSchema().get())
               + ">";
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
          "%n%-20s%n%-20s",
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
          "%n%-20s%n%-20s%n%s",
          "Execution plan",
          "--------------",
          queryDescription.getExecutionPlan()
      ));
    }
  }

  private void printTopology(final QueryDescription queryDescription) {
    if (!queryDescription.getTopology().isEmpty()) {
      writer().println(String.format(
          "%n%-20s%n%-20s%n%s",
          "Processing topology",
          "-------------------",
          queryDescription.getTopology()
      ));
    }
  }

  private void printOverriddenProperties(final QueryDescription queryDescription) {
    final Map<String, Object> overriddenProperties = queryDescription.getOverriddenProperties();
    if (overriddenProperties.isEmpty()) {
      return;
    }

    new PropertiesListTableBuilder().print(overriddenProperties)
        .withHeaderLine(String.format(
            "%n%-20s%n%-20s",
            "Overridden Properties",
            "---------------------"))
        .build()
        .print(this);
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
        "%n%-20s%n%s",
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
          "%n%-20s%n%-20s",
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
          "%n%-20s%n%-20s",
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
    final String functionName = describeFunction.getName().toUpperCase();
    final String baseFormat = "%-12s: %s%n";
    final String subFormat = "\t%-12s: %s%n";
    writer().printf(baseFormat, "Name", functionName);
    if (!describeFunction.getAuthor().trim().isEmpty()) {
      writer().printf(baseFormat, "Author", describeFunction.getAuthor());
    }
    if (!describeFunction.getVersion().trim().isEmpty()) {
      writer().printf(baseFormat, "Version", describeFunction.getVersion());
    }

    printDescription(baseFormat, "Overview", describeFunction.getDescription());

    writer().printf(baseFormat, "Type", describeFunction.getType().name());
    writer().printf(baseFormat, "Jar", describeFunction.getPath());
    writer().printf(baseFormat, "Variations", "");
    final Collection<FunctionInfo> functions = describeFunction.getFunctions();
    functions.forEach(functionInfo -> {
          final String arguments = functionInfo.getArguments().stream()
              .map(arg -> arg.getName().isEmpty()
                      ? arg.getType()
                      : arg.getName() + " " + arg.getType())
              .collect(Collectors.joining(", "));

          writer().printf("%n\t%-12s: %s(%s)%n", "Variation", functionName, arguments);

          writer().printf(subFormat, "Returns", functionInfo.getReturnType());
          printDescription(subFormat, "Description", functionInfo.getDescription());
          functionInfo.getArguments()
              .forEach(a -> printDescription(subFormat, a.getName(), a.getDescription()));
        }
    );
  }

  private void printDescription(final String format, final String name, final String description) {
    final String trimmed = description.trim();
    if (trimmed.isEmpty()) {
      return;
    }

    final int labelLen = String.format(format.replace("%n", ""), name, "")
        .replace("\t", "  ")
        .length();

    final int width = Math.max(getWidth(), 80) - labelLen;

    final String fixedWidth = splitLongLine(trimmed, width);

    final String indent = String.format("%-" + labelLen + "s", "");

    final String result = fixedWidth
        .replace(System.lineSeparator(), System.lineSeparator() + indent);

    writer().printf(format, name, result);
  }

  private static String splitLongLine(final String input, final int maxLineLength) {
    final StringTokenizer spaceTok = new StringTokenizer(input, " \n", true);
    final StringBuilder output = new StringBuilder(input.length());
    int lineLen = 0;
    while (spaceTok.hasMoreTokens()) {
      final String word = spaceTok.nextToken();
      final boolean isNewLineChar = word.equals("\n");

      if (isNewLineChar || lineLen + word.length() > maxLineLength) {
        output.append(System.lineSeparator());
        lineLen = 0;

        if (isNewLineChar) {
          continue;
        }
      }

      output.append(word);
      lineLen += word.length();
    }
    return output.toString();
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
}
