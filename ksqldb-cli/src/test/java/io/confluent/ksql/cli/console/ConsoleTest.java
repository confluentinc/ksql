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

package io.confluent.ksql.cli.console;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.FakeException;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.Console.NoOpRowCaptor;
import io.confluent.ksql.cli.console.cmd.CliSpecificCommand;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metrics.TopicSensors.Stat;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.ConsumerPartitionOffsets;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryOffsetSummary;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.QueryTopicOffsetSummary;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.rest.entity.SimpleConnectorInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.entity.TopicPartitionEntity;
import io.confluent.ksql.rest.entity.TypeList;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.ConnectorState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class ConsoleTest {

  private static final String CLI_CMD_NAME = "some command";
  private static final String WHITE_SPACE = " \t ";
  private static final String NEWLINE = System.lineSeparator();
  private static final String STATUS_COUNT_STRING = "RUNNING:1,ERROR:2";
  private static final String AGGREGATE_STATUS = "ERROR";
  protected static final Stat STAT =  new Stat("TEST", 0, 0);
  protected static final ImmutableMap<String, Stat> IMMUTABLE_MAP = new ImmutableMap.Builder<String, Stat>().put("TEST", STAT).build();

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("foo"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
      .build().withPseudoAndKeyColsInValue(false);

  private final TestTerminal terminal;
  private final Console console;
  private final Supplier<String> lineSupplier;
  private final CliSpecificCommand cliCommand;
  private final SourceDescription sourceDescription = new SourceDescription(
      "TestSource",
      Optional.empty(),
      Collections.emptyList(),
      Collections.emptyList(),
      buildTestSchema(SqlTypes.INTEGER, SqlTypes.STRING),
      DataSourceType.KTABLE.getKsqlType(),
      "2000-01-01",
      "stats",
      "errors",
      IMMUTABLE_MAP,
      IMMUTABLE_MAP,
      true,
      "kafka",
      "avro",
      "kafka-topic",
      2,
      1,
      "statement",
      Collections.emptyList(),
      Collections.emptyList());

  @Mock
  private QueryStatusCount queryStatusCount;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<OutputFormat> data() {
    return ImmutableList.of(OutputFormat.JSON, OutputFormat.TABULAR);
  }

  @SuppressWarnings("unchecked")
  public ConsoleTest(final OutputFormat outputFormat) {
    this.lineSupplier = mock(Supplier.class);
    this.cliCommand = mock(CliSpecificCommand.class);
    this.terminal = new TestTerminal(lineSupplier);
    this.console = new Console(outputFormat, terminal, new NoOpRowCaptor());

    when(cliCommand.getName()).thenReturn(CLI_CMD_NAME);
    when(cliCommand.matches(any()))
        .thenAnswer(i -> ((String) i.getArgument(0)).toLowerCase().startsWith(CLI_CMD_NAME.toLowerCase()));
    console.registerCliSpecificCommand(cliCommand);
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(queryStatusCount.toString()).thenReturn(STATUS_COUNT_STRING);
    when(queryStatusCount.getAggregateStatus()).thenReturn(KsqlQueryStatus.valueOf(AGGREGATE_STATUS));
    final EnumMap<KsqlQueryStatus, Integer> mockStatusCount = new EnumMap<>(KsqlQueryStatus.class);
    mockStatusCount.put(KsqlQueryStatus.RUNNING, 1);
    mockStatusCount.put(KsqlQueryStatus.ERROR, 2);
    when(queryStatusCount.getStatuses()).thenReturn(mockStatusCount);
  }

  @After
  public void after() {
    console.close();
  }

  @Test
  public void testPrintDataRow() {
    // Given:
    final StreamedRow row = StreamedRow.pushRow(genericRow("col_1", "col_2"));

    // When:
    console.printStreamedRow(row);

    // Then:
    assertThat(terminal.getOutputString(), containsString("col_1"));
    assertThat(terminal.getOutputString(), containsString("col_2"));
  }

  @Test
  public void testPrintTableTombstone() {
    // Given:
    console.printStreamedRow(StreamedRow.header(new QueryId("id"), SCHEMA));

    final StreamedRow row = StreamedRow.tombstone(genericRow(null, "v_0", null));

    // When:
    console.printStreamedRow(row);

    // Then:
    assertThat(terminal.getOutputString(), containsString("v_0"));

    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(terminal.getOutputString(), containsString("<TOMBSTONE>"));
    } else {
      assertThat(terminal.getOutputString(), containsString("\"tombstone\" : true"));
    }
  }

  @Test
  public void shouldPrintHeader() {
    // Given:
    final StreamedRow header = StreamedRow.header(new QueryId("id"), SCHEMA);

    // When:
    console.printStreamedRow(header);

    // Then:
    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(terminal.getOutputString(), containsString("foo"));
      assertThat(terminal.getOutputString(), containsString("bar"));
    }
  }

  @Test
  public void testPrintErrorStreamedRow() {
    final FakeException exception = new FakeException();

    console.printStreamedRow(StreamedRow.error(exception, Errors.ERROR_CODE_SERVER_ERROR));

    assertThat(terminal.getOutputString(), is(exception.getMessage() + NEWLINE));
  }

  @Test
  public void testPrintFinalMessageStreamedRow() {
    console.printStreamedRow(StreamedRow.finalMessage("Some message"));
    assertThat(terminal.getOutputString(), is("Some message" + NEWLINE));
  }

  @Test
  public void testPrintCommandStatus() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new CommandStatusEntity(
            "e",
            CommandId.fromString("topic/1/create"),
            new CommandStatus(
                CommandStatus.Status.SUCCESS,
                "Success Message",
                Optional.of(new QueryId("CSAS_0"))
            ),
            0L)
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"currentStatus\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"commandId\" : \"topic/1/create\"," + NEWLINE
          + "  \"commandStatus\" : {" + NEWLINE
          + "    \"status\" : \"SUCCESS\"," + NEWLINE
          + "    \"message\" : \"Success Message\"," + NEWLINE
          + "    \"queryId\" : \"CSAS_0\"" + NEWLINE
          + "  }," + NEWLINE
          + "  \"commandSequenceNumber\" : 0," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Message         " + NEWLINE
          + "-----------------" + NEWLINE
          + " Success Message " + NEWLINE
          + "-----------------" + NEWLINE));
    }
  }

  @Test
  public void testPrintPropertyList() {
    // Given:
    final List<Property> properties = new ArrayList<>();
    properties.add(new Property("k1", "KSQL", "1"));
    properties.add(new Property("k2", "KSQL", "v2"));
    properties.add(new Property("k3", "KSQL", "true"));

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new PropertiesList("e", properties, Collections.emptyList(), Collections.emptyList())
      ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"properties\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"properties\" : [ {" + NEWLINE
          + "    \"name\" : \"k1\"," + NEWLINE
          + "    \"scope\" : \"KSQL\"," + NEWLINE
          + "    \"value\" : \"1\"" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"name\" : \"k2\"," + NEWLINE
          + "    \"scope\" : \"KSQL\"," + NEWLINE
          + "    \"value\" : \"v2\"" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"name\" : \"k3\"," + NEWLINE
          + "    \"scope\" : \"KSQL\"," + NEWLINE
          + "    \"value\" : \"true\"" + NEWLINE
          + "  } ]," + NEWLINE
          + "  \"overwrittenProperties\" : [ ]," + NEWLINE
          + "  \"defaultProperties\" : [ ]," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Property | Scope | Default override | Effective Value " + NEWLINE
          + "-------------------------------------------------------" + NEWLINE
          + " k1       | KSQL  | SERVER           | 1               " + NEWLINE
          + " k2       | KSQL  | SERVER           | v2              " + NEWLINE
          + " k3       | KSQL  | SERVER           | true            " + NEWLINE
          + "-------------------------------------------------------" + NEWLINE));
    }
  }
  @Test
  public void testPrintQueries() {
    // Given:
    final List<RunningQuery> queries = new ArrayList<>();
    queries.add(
        new RunningQuery(
            "select * from t1 emit changes", Collections.singleton("Test"), Collections.singleton("Test topic"), new QueryId("0"), queryStatusCount, KsqlConstants.KsqlQueryType.PUSH));

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new Queries("e", queries)
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"queries\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"queries\" : [ {" + NEWLINE
          + "    \"queryString\" : \"select * from t1 emit changes\"," + NEWLINE
          + "    \"sinks\" : [ \"Test\" ]," + NEWLINE
          + "    \"sinkKafkaTopics\" : [ \"Test topic\" ]," + NEWLINE
          + "    \"id\" : \"0\"," + NEWLINE
          + "    \"statusCount\" : {" + NEWLINE
          + "      \"RUNNING\" : 1," + NEWLINE
          + "      \"ERROR\" : 2" + NEWLINE
          + "    }," + NEWLINE
          + "    \"queryType\" : \"PUSH\"," + NEWLINE
          + "    \"state\" : \"" + AGGREGATE_STATUS +"\"" + NEWLINE
          + "  } ]," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Query ID | Query Type | Status            | Sink Name | Sink Kafka Topic | Query String                  " + NEWLINE
          + "----------------------------------------------------------------------------------------------------" + NEWLINE
          + " 0        | PUSH       | " + STATUS_COUNT_STRING + " | Test      | Test topic       | select * from t1 emit changes " + NEWLINE
          + "----------------------------------------------------------------------------------------------------" + NEWLINE
          + "For detailed information on a Query run: EXPLAIN <Query ID>;" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintExplainQueryWithError() {
    final long timestamp = 1596644936314L;

    // Given:
    final QueryDescriptionEntity queryEntity = new QueryDescriptionEntity(
        "statement",
        new QueryDescription(
            new QueryId("id"),
            "statement",
            Optional.empty(),
            ImmutableList.of(
                new FieldInfo(
                    "name",
                    new SchemaInfo(SqlBaseType.STRING, ImmutableList.of(), null),
                    Optional.empty())),
            ImmutableSet.of("source"),
            ImmutableSet.of("sink"),
            "topology",
            "executionPlan",
            ImmutableMap.of("overridden.prop", 42),
            ImmutableMap.of(new KsqlHostInfoEntity("foo", 123), KsqlQueryStatus.ERROR),
            KsqlQueryType.PERSISTENT,
            ImmutableList.of(new QueryError(timestamp, "error", Type.SYSTEM)),
            ImmutableSet.of(
                new StreamsTaskMetadata(
                    "test",
                    Collections.emptySet(),
                    Optional.empty()
                )
            )
        )
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(queryEntity));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {\n" +
          "  \"@type\" : \"queryDescription\",\n" +
          "  \"statementText\" : \"statement\",\n" +
          "  \"queryDescription\" : {\n" +
          "    \"id\" : \"id\",\n" +
          "    \"statementText\" : \"statement\",\n" +
          "    \"windowType\" : null,\n" +
          "    \"fields\" : [ {\n" +
          "      \"name\" : \"name\",\n" +
          "      \"schema\" : {\n" +
          "        \"type\" : \"STRING\",\n" +
          "        \"fields\" : [ ],\n" +
          "        \"memberSchema\" : null\n" +
          "      }\n" +
          "    } ],\n" +
          "    \"sources\" : [ \"source\" ],\n" +
          "    \"sinks\" : [ \"sink\" ],\n" +
          "    \"topology\" : \"topology\",\n" +
          "    \"executionPlan\" : \"executionPlan\",\n" +
          "    \"overriddenProperties\" : {\n" +
          "      \"overridden.prop\" : 42\n" +
          "    },\n" +
          "    \"ksqlHostQueryStatus\" : {\n" +
          "      \"foo:123\" : \"ERROR\"\n" +
          "    },\n" +
          "    \"queryType\" : \"PERSISTENT\",\n" +
          "    \"queryErrors\" : [ {\n" +
          "      \"timestamp\" : 1596644936314,\n" +
          "      \"errorMessage\" : \"error\",\n" +
          "      \"type\" : \"SYSTEM\"\n" +
          "    } ],\n" +
          "    \"tasksMetadata\" : [ {\n" +
          "      \"taskId\" : \"test\",\n" +
          "      \"topicOffsets\" : [ ],\n" +
          "      \"timeCurrentIdlingStarted\" : null\n" +
          "    } ],\n" +
          "    \"state\" : \"ERROR\"\n" +
          "  },\n" +
          "  \"warnings\" : [ ]\n" +
          "} ]\n"));
    } else {
      final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss,SSS (z)");
      final String localTime = Instant.ofEpochMilli(timestamp)
          .atZone(ZoneId.systemDefault()).format(format);

      assertThat(output, is("\n" +
          "ID                   : id\n" +
          "Query Type           : PERSISTENT\n" +
          "SQL                  : statement\n" +
          "Host Query Status    : {foo:123=ERROR}\n" +
          "\n" +
          " Field | Type            \n" +
          "-------------------------\n" +
          " name  | VARCHAR(STRING) \n" +
          "-------------------------\n" +
          "\n" +
          "Sources that this query reads from: \n" +
          "-----------------------------------\n" +
          "source\n" +
          "\n" +
          "For source description please run: DESCRIBE [EXTENDED] <SourceId>\n" +
          "\n" +
          "Sinks that this query writes to: \n" +
          "-----------------------------------\n" +
          "sink\n" +
          "\n" +
          "For sink description please run: DESCRIBE [EXTENDED] <SinkId>\n" +
          "\n" +
          "Execution plan      \n" +
          "--------------      \n" +
          "executionPlan\n" +
          "\n" +
          "Processing topology \n" +
          "------------------- \n" +
          "topology\n" +
          "\n" +
          "Overridden Properties\n" +
          "---------------------\n" +
          " Property        | Value \n" +
          "-------------------------\n" +
          " overridden.prop | 42    \n" +
          "-------------------------\n" +
          "\n" +
          "Error Date           : " + localTime + "\n" +
          "Error Details        : error\n" +
          "Error Type           : SYSTEM\n"
      ));
    }
  }

  @Test
  public void testPrintSourceDescription() {
    // Given:
    final List<FieldInfo> fields = buildTestSchema(
        SqlTypes.BOOLEAN,
        SqlTypes.INTEGER,
        SqlTypes.BIGINT,
        SqlTypes.DOUBLE,
        SqlTypes.STRING,
        SqlTypes.array(SqlTypes.STRING),
        SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT),
        SqlTypes.struct()
            .field("a", SqlTypes.DOUBLE)
            .build()
    );

    final List<RunningQuery> readQueries = ImmutableList.of(
        new RunningQuery("read query", ImmutableSet.of("sink1"), ImmutableSet.of("sink1 topic"), new QueryId("readId"), queryStatusCount, KsqlConstants.KsqlQueryType.PERSISTENT)
    );
    final List<RunningQuery> writeQueries = ImmutableList.of(
        new RunningQuery("write query", ImmutableSet.of("sink2"), ImmutableSet.of("sink2 topic"), new QueryId("writeId"), queryStatusCount, KsqlConstants.KsqlQueryType.PERSISTENT)
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new SourceDescriptionEntity(
            "some sql",
            new SourceDescription(
                "TestSource",
                Optional.empty(),
                readQueries,
                writeQueries,
                fields,
                DataSourceType.KTABLE.getKsqlType(),
                "2000-01-01",
                "stats",
                "errors",
                IMMUTABLE_MAP,
                IMMUTABLE_MAP,
                false,
                "kafka",
                "avro",
                "kafka-topic",
                1,
                1,
                "sql statement",
                Collections.emptyList(),
                Collections.emptyList()),
            Collections.emptyList()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"sourceDescription\"," + NEWLINE
          + "  \"statementText\" : \"some sql\"," + NEWLINE
          + "  \"sourceDescription\" : {" + NEWLINE
          + "    \"name\" : \"TestSource\"," + NEWLINE
          + "    \"windowType\" : null," + NEWLINE
          + "    \"readQueries\" : [ {" + NEWLINE
          + "      \"queryString\" : \"read query\"," + NEWLINE
          + "      \"sinks\" : [ \"sink1\" ]," + NEWLINE
          + "      \"sinkKafkaTopics\" : [ \"sink1 topic\" ]," + NEWLINE
          + "      \"id\" : \"readId\"," + NEWLINE
          + "      \"statusCount\" : {" + NEWLINE
          + "        \"RUNNING\" : 1," + NEWLINE
          + "        \"ERROR\" : 2" + NEWLINE
          + "      }," + NEWLINE
          + "      \"queryType\" : \"PERSISTENT\"," + NEWLINE
          + "      \"state\" : \"" + AGGREGATE_STATUS +"\"" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"writeQueries\" : [ {" + NEWLINE
          + "      \"queryString\" : \"write query\"," + NEWLINE
          + "      \"sinks\" : [ \"sink2\" ]," + NEWLINE
          + "      \"sinkKafkaTopics\" : [ \"sink2 topic\" ]," + NEWLINE
          + "      \"id\" : \"writeId\"," + NEWLINE
          + "      \"statusCount\" : {" + NEWLINE
          + "        \"RUNNING\" : 1," + NEWLINE
          + "        \"ERROR\" : 2" + NEWLINE
          + "      }," + NEWLINE
          + "      \"queryType\" : \"PERSISTENT\"," + NEWLINE
          + "      \"state\" : \"" + AGGREGATE_STATUS +"\"" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"fields\" : [ {" + NEWLINE
          + "      \"name\" : \"ROWKEY\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }," + NEWLINE
          + "      \"type\" : \"KEY\"" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_0\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"BOOLEAN\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_1\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"INTEGER\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_2\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"BIGINT\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_3\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"DOUBLE\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_4\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_5\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"ARRAY\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : {" + NEWLINE
          + "          \"type\" : \"STRING\"," + NEWLINE
          + "          \"fields\" : null," + NEWLINE
          + "          \"memberSchema\" : null" + NEWLINE
          + "        }" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_6\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"MAP\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : {" + NEWLINE
          + "          \"type\" : \"BIGINT\"," + NEWLINE
          + "          \"fields\" : null," + NEWLINE
          + "          \"memberSchema\" : null" + NEWLINE
          + "        }" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_7\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRUCT\"," + NEWLINE
          + "        \"fields\" : [ {" + NEWLINE
          + "          \"name\" : \"a\"," + NEWLINE
          + "          \"schema\" : {" + NEWLINE
          + "            \"type\" : \"DOUBLE\"," + NEWLINE
          + "            \"fields\" : null," + NEWLINE
          + "            \"memberSchema\" : null" + NEWLINE
          + "          }" + NEWLINE
          + "        } ]," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"type\" : \"TABLE\"," + NEWLINE
          + "    \"timestamp\" : \"2000-01-01\"," + NEWLINE
          + "    \"statistics\" : \"The statistics field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use statisticsMap instead.\\nstats\"," + NEWLINE
          + "    \"errorStats\" : \"The errorStats field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use errorStatsMap instead.\\nerrors\\n\"," + NEWLINE
          + "    \"statisticsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"errorStatsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"extended\" : false," + NEWLINE
          + "    \"keyFormat\" : \"kafka\"," + NEWLINE
          + "    \"valueFormat\" : \"avro\"," + NEWLINE
          + "    \"topic\" : \"kafka-topic\"," + NEWLINE
          + "    \"partitions\" : 1," + NEWLINE
          + "    \"replication\" : 1," + NEWLINE
          + "    \"statement\" : \"sql statement\"," + NEWLINE
          + "    \"queryOffsetSummaries\" : [ ]," + NEWLINE
          + "    \"sourceConstraints\" : [ ]" + NEWLINE
          + "  }," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + "Name                 : TestSource" + NEWLINE
          + " Field  | Type                           " + NEWLINE
          + "-----------------------------------------" + NEWLINE
          + " ROWKEY | VARCHAR(STRING)  (primary key) " + NEWLINE
          + " f_0    | BOOLEAN                        " + NEWLINE
          + " f_1    | INTEGER                        " + NEWLINE
          + " f_2    | BIGINT                         " + NEWLINE
          + " f_3    | DOUBLE                         " + NEWLINE
          + " f_4    | VARCHAR(STRING)                " + NEWLINE
          + " f_5    | ARRAY<VARCHAR(STRING)>         " + NEWLINE
          + " f_6    | MAP<STRING, BIGINT>            " + NEWLINE
          + " f_7    | STRUCT<a DOUBLE>               " + NEWLINE
          + "-----------------------------------------" + NEWLINE
          + "For runtime statistics and query details run: DESCRIBE <Stream,Table> EXTENDED;"
          + NEWLINE));
    }
  }

  @Test
  public void testPrintTopicDescription() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new TopicDescription("e", "TestTopic", "TestKafkaTopic", "AVRO", "schemaString")
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"topicDescription\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"name\" : \"TestTopic\"," + NEWLINE
          + "  \"format\" : \"AVRO\"," + NEWLINE
          + "  \"schemaString\" : \"schemaString\"," + NEWLINE
          + "  \"warnings\" : [ ]," + NEWLINE
          + "  \"kafkaTopic\" : \"TestKafkaTopic\"" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Table Name | Kafka Topic    | Type | Schema       " + NEWLINE
          + "---------------------------------------------------" + NEWLINE
          + " TestTopic  | TestKafkaTopic | AVRO | schemaString " + NEWLINE
          + "---------------------------------------------------" + NEWLINE));
    }
  }

  @Test
  public void testPrintConnectorDescription() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new ConnectorDescription(
            "STATEMENT",
            "io.confluent.Connector",
            new ConnectorStateInfo(
                "name",
                new ConnectorState("state", "worker", "msg"),
                ImmutableList.of(
                    new TaskState(0, "task", "worker", "task_msg")
                ),
                ConnectorType.SOURCE),
            ImmutableList.of(sourceDescription),
            ImmutableList.of("a-jdbc-topic"),
            ImmutableList.of()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"connector_description\"," + NEWLINE
          + "  \"statementText\" : \"STATEMENT\"," + NEWLINE
          + "  \"connectorClass\" : \"io.confluent.Connector\"," + NEWLINE
          + "  \"status\" : {" + NEWLINE
          + "    \"name\" : \"name\"," + NEWLINE
          + "    \"connector\" : {" + NEWLINE
          + "      \"state\" : \"state\"," + NEWLINE
          + "      \"worker_id\" : \"worker\"," + NEWLINE
          + "      \"trace\" : \"msg\"" + NEWLINE
          + "    }," + NEWLINE
          + "    \"tasks\" : [ {" + NEWLINE
          + "      \"id\" : 0," + NEWLINE
          + "      \"state\" : \"task\"," + NEWLINE
          + "      \"worker_id\" : \"worker\"," + NEWLINE
          + "      \"trace\" : \"task_msg\"" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"type\" : \"source\"" + NEWLINE
          + "  }," + NEWLINE
          + "  \"sources\" : [ {" + NEWLINE
          + "    \"name\" : \"TestSource\"," + NEWLINE
          + "    \"windowType\" : null," + NEWLINE
          + "    \"readQueries\" : [ ]," + NEWLINE
          + "    \"writeQueries\" : [ ]," + NEWLINE
          + "    \"fields\" : [ {" + NEWLINE
          + "      \"name\" : \"ROWKEY\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }," + NEWLINE
          + "      \"type\" : \"KEY\"" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_0\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"INTEGER\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_1\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"type\" : \"TABLE\"," + NEWLINE
          + "    \"timestamp\" : \"2000-01-01\"," + NEWLINE
          + "    \"statistics\" : \"The statistics field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use statisticsMap instead.\\nstats\"," + NEWLINE
          + "    \"errorStats\" : \"The errorStats field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use errorStatsMap instead.\\nerrors\\n\"," + NEWLINE
          + "    \"statisticsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"errorStatsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"extended\" : true," + NEWLINE
          + "    \"keyFormat\" : \"kafka\"," + NEWLINE
          + "    \"valueFormat\" : \"avro\"," + NEWLINE
          + "    \"topic\" : \"kafka-topic\"," + NEWLINE
          + "    \"partitions\" : 2," + NEWLINE
          + "    \"replication\" : 1," + NEWLINE
          + "    \"statement\" : \"statement\"," + NEWLINE
          + "    \"queryOffsetSummaries\" : [ ]," + NEWLINE
          + "    \"sourceConstraints\" : [ ]" + NEWLINE
          + "  } ]," + NEWLINE
          + "  \"topics\" : [ \"a-jdbc-topic\" ]," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + "Name                 : name" + NEWLINE
          + "Class                : io.confluent.Connector" + NEWLINE
          + "Type                 : source" + NEWLINE
          + "State                : state" + NEWLINE
          + "WorkerId             : worker" + NEWLINE
          + "Trace                : msg" + NEWLINE
          + "" + NEWLINE
          + " Task ID | State | Error Trace " + NEWLINE
          + "-------------------------------" + NEWLINE
          + " 0       | task  | task_msg    " + NEWLINE
          + "-------------------------------" + NEWLINE
          + "" + NEWLINE
          + " KSQL Source Name | Kafka Topic | Type  " + NEWLINE
          + "----------------------------------------" + NEWLINE
          + " TestSource       | kafka-topic | TABLE " + NEWLINE
          + "----------------------------------------" + NEWLINE
          + "" + NEWLINE
          + " Related Topics " + NEWLINE
          + "----------------" + NEWLINE
          + " a-jdbc-topic   " + NEWLINE
          + "----------------" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintStreamsList() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new StreamsList("e", ImmutableList.of(
            new SourceInfo.Stream("B", "t2", "KAFKA", "AVRO", false),
            new SourceInfo.Stream("A", "t1", "JSON", "JSON", true)
        ))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"streams\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"streams\" : [ {" + NEWLINE
          + "    \"type\" : \"STREAM\"," + NEWLINE
          + "    \"name\" : \"B\"," + NEWLINE
          + "    \"topic\" : \"t2\"," + NEWLINE
          + "    \"keyFormat\" : \"KAFKA\"," + NEWLINE
          + "    \"valueFormat\" : \"AVRO\"," + NEWLINE
          + "    \"isWindowed\" : false" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"type\" : \"STREAM\"," + NEWLINE
          + "    \"name\" : \"A\"," + NEWLINE
          + "    \"topic\" : \"t1\"," + NEWLINE
          + "    \"keyFormat\" : \"JSON\"," + NEWLINE
          + "    \"valueFormat\" : \"JSON\"," + NEWLINE
          + "    \"isWindowed\" : true" + NEWLINE
          + "  } ]," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Stream Name | Kafka Topic | Key Format | Value Format | Windowed " + NEWLINE
          + "------------------------------------------------------------------" + NEWLINE
          + " A           | t1          | JSON       | JSON         | true     " + NEWLINE
          + " B           | t2          | KAFKA      | AVRO         | false    " + NEWLINE
          + "------------------------------------------------------------------" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintTablesList() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new TablesList("e", ImmutableList.of(
            new SourceInfo.Table("B", "t2", "JSON", "JSON", true),
            new SourceInfo.Table("A", "t1", "KAFKA", "AVRO", false)
        ))
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"tables\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"tables\" : [ {" + NEWLINE
          + "    \"type\" : \"TABLE\"," + NEWLINE
          + "    \"name\" : \"B\"," + NEWLINE
          + "    \"topic\" : \"t2\"," + NEWLINE
          + "    \"keyFormat\" : \"JSON\"," + NEWLINE
          + "    \"valueFormat\" : \"JSON\"," + NEWLINE
          + "    \"isWindowed\" : true" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"type\" : \"TABLE\"," + NEWLINE
          + "    \"name\" : \"A\"," + NEWLINE
          + "    \"topic\" : \"t1\"," + NEWLINE
          + "    \"keyFormat\" : \"KAFKA\"," + NEWLINE
          + "    \"valueFormat\" : \"AVRO\"," + NEWLINE
          + "    \"isWindowed\" : false" + NEWLINE
          + "  } ]," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Table Name | Kafka Topic | Key Format | Value Format | Windowed " + NEWLINE
          + "-----------------------------------------------------------------" + NEWLINE
          + " A          | t1          | KAFKA      | AVRO         | false    " + NEWLINE
          + " B          | t2          | JSON       | JSON         | true     " + NEWLINE
          + "-----------------------------------------------------------------" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintConnectorsList() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new ConnectorList(
            "statement",
            ImmutableList.of(),
            ImmutableList.of(
                new SimpleConnectorInfo("foo", ConnectorType.SOURCE, "clazz", "STATUS"),
                new SimpleConnectorInfo("bar", null, null, null)
        ))
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is(""
          + "[ {" + NEWLINE
          + "  \"@type\" : \"connector_list\"," + NEWLINE
          + "  \"statementText\" : \"statement\"," + NEWLINE
          + "  \"warnings\" : [ ]," + NEWLINE
          + "  \"connectors\" : [ {" + NEWLINE
          + "    \"name\" : \"foo\"," + NEWLINE
          + "    \"type\" : \"source\"," + NEWLINE
          + "    \"className\" : \"clazz\"," + NEWLINE
          + "    \"state\" : \"STATUS\"" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"name\" : \"bar\"" + NEWLINE
          + "  } ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Connector Name | Type    | Class | Status " + NEWLINE
          + "-------------------------------------------" + NEWLINE
          + " foo            | SOURCE  | clazz | STATUS " + NEWLINE
          + " bar            | UNKNOWN |       |        " + NEWLINE
          + "-------------------------------------------" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintTypesList() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new TypeList("statement", ImmutableMap.of(
            "typeB", new SchemaInfo(
                SqlBaseType.ARRAY,
                null,
                new SchemaInfo(SqlBaseType.STRING, null, null)),
            "typeA", new SchemaInfo(
                SqlBaseType.STRUCT,
                ImmutableList.of(
                    new FieldInfo("f1", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.empty())),
                null),
            "typeC", new SchemaInfo(
                    SqlBaseType.DECIMAL,
                    null,
                    null,
                    ImmutableMap.of("precision", 10, "scale", 9)
                )
        ))
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"type_list\"," + NEWLINE
          + "  \"statementText\" : \"statement\"," + NEWLINE
          + "  \"types\" : {" + NEWLINE
          + "    \"typeB\" : {" + NEWLINE
          + "      \"type\" : \"ARRAY\"," + NEWLINE
          + "      \"fields\" : null," + NEWLINE
          + "      \"memberSchema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    }," + NEWLINE
          + "    \"typeA\" : {" + NEWLINE
          + "      \"type\" : \"STRUCT\"," + NEWLINE
          + "      \"fields\" : [ {" + NEWLINE
          + "        \"name\" : \"f1\"," + NEWLINE
          + "        \"schema\" : {" + NEWLINE
          + "          \"type\" : \"STRING\"," + NEWLINE
          + "          \"fields\" : null," + NEWLINE
          + "          \"memberSchema\" : null" + NEWLINE
          + "        }" + NEWLINE
          + "      } ]," + NEWLINE
          + "      \"memberSchema\" : null" + NEWLINE
          + "    }," + NEWLINE
          + "    \"typeC\" : {" + NEWLINE
          + "      \"type\" : \"DECIMAL\"," + NEWLINE
          + "      \"fields\" : null," + NEWLINE
          + "      \"memberSchema\" : null," + NEWLINE
          + "      \"parameters\" : {" + NEWLINE
          + "        \"precision\" : 10," + NEWLINE
          + "        \"scale\" : 9" + NEWLINE
          + "      }" + NEWLINE
          + "    }" + NEWLINE
          + "  }," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Type Name | Schema                     " + NEWLINE
          + "----------------------------------------" + NEWLINE
          + " typeA     | STRUCT<f1 VARCHAR(STRING)> " + NEWLINE
          + " typeB     | ARRAY<VARCHAR(STRING)>     " + NEWLINE
          + " typeC     | DECIMAL(10, 9)             " + NEWLINE
          + "----------------------------------------" + NEWLINE));
    }
  }

  @Test
  public void testPrintExecuptionPlan() {
    // Given:
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new ExecutionPlan("Test Execution Plan")
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"executionPlan\"," + NEWLINE
          + "  \"statementText\" : \"Test Execution Plan\"," + NEWLINE
          + "  \"warnings\" : [ ]," + NEWLINE
          + "  \"executionPlan\" : \"Test Execution Plan\"" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Execution Plan      " + NEWLINE
          + "---------------------" + NEWLINE
          + " Test Execution Plan " + NEWLINE
          + "---------------------" + NEWLINE));
    }
  }

  @Test
  public void shouldPrintTopicDescribeExtended() {
    // Given:
    final List<RunningQuery> readQueries = ImmutableList.of(
        new RunningQuery("read query", ImmutableSet.of("sink1"), ImmutableSet.of("sink1 topic"), new QueryId("readId"), queryStatusCount, KsqlConstants.KsqlQueryType.PERSISTENT)
    );
    final List<RunningQuery> writeQueries = ImmutableList.of(
        new RunningQuery("write query", ImmutableSet.of("sink2"), ImmutableSet.of("sink2 topic"), new QueryId("writeId"), queryStatusCount, KsqlConstants.KsqlQueryType.PERSISTENT)
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new SourceDescriptionEntity(
            "e",
            new SourceDescription(
                "TestSource",
                Optional.empty(),
                readQueries,
                writeQueries,
                buildTestSchema(SqlTypes.STRING),
                DataSourceType.KTABLE.getKsqlType(),
                "2000-01-01",
                "stats",
                "errors",
                IMMUTABLE_MAP,
                IMMUTABLE_MAP,
                true,
                "json",
                "avro",
                "kafka-topic",
                2, 1,
                "sql statement text",
                ImmutableList.of(
                    new QueryOffsetSummary(
                        "consumer1",
                        ImmutableList.of(
                            new QueryTopicOffsetSummary(
                                "kafka-topic",
                                ImmutableList.of(
                                    new ConsumerPartitionOffsets(0, 100, 900, 800),
                                    new ConsumerPartitionOffsets(1, 50, 900, 900)
                                )),
                            new QueryTopicOffsetSummary(
                                "kafka-topic-2",
                                ImmutableList.of(
                                    new ConsumerPartitionOffsets(0, 0, 90, 80),
                                    new ConsumerPartitionOffsets(1, 10, 90, 90)
                                ))
                        )),
                    new QueryOffsetSummary(
                        "consumer2",
                        ImmutableList.of())
                ),
                ImmutableList.of("S1", "S2")),
            Collections.emptyList()
        ))
    );

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is("[ {" + NEWLINE
          + "  \"@type\" : \"sourceDescription\"," + NEWLINE
          + "  \"statementText\" : \"e\"," + NEWLINE
          + "  \"sourceDescription\" : {" + NEWLINE
          + "    \"name\" : \"TestSource\"," + NEWLINE
          + "    \"windowType\" : null," + NEWLINE
          + "    \"readQueries\" : [ {" + NEWLINE
          + "      \"queryString\" : \"read query\"," + NEWLINE
          + "      \"sinks\" : [ \"sink1\" ]," + NEWLINE
          + "      \"sinkKafkaTopics\" : [ \"sink1 topic\" ]," + NEWLINE
          + "      \"id\" : \"readId\"," + NEWLINE
          + "      \"statusCount\" : {" + NEWLINE
          + "        \"RUNNING\" : 1," + NEWLINE
          + "        \"ERROR\" : 2" + NEWLINE
          + "      }," + NEWLINE
          + "      \"queryType\" : \"PERSISTENT\"," + NEWLINE
          + "      \"state\" : \"" + AGGREGATE_STATUS +"\"" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"writeQueries\" : [ {" + NEWLINE
          + "      \"queryString\" : \"write query\"," + NEWLINE
          + "      \"sinks\" : [ \"sink2\" ]," + NEWLINE
          + "      \"sinkKafkaTopics\" : [ \"sink2 topic\" ]," + NEWLINE
          + "      \"id\" : \"writeId\"," + NEWLINE
          + "      \"statusCount\" : {" + NEWLINE
          + "        \"RUNNING\" : 1," + NEWLINE
          + "        \"ERROR\" : 2" + NEWLINE
          + "      }," + NEWLINE
          + "      \"queryType\" : \"PERSISTENT\"," + NEWLINE
          + "      \"state\" : \"" + AGGREGATE_STATUS +"\"" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"fields\" : [ {" + NEWLINE
          + "      \"name\" : \"ROWKEY\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }," + NEWLINE
          + "      \"type\" : \"KEY\"" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"name\" : \"f_0\"," + NEWLINE
          + "      \"schema\" : {" + NEWLINE
          + "        \"type\" : \"STRING\"," + NEWLINE
          + "        \"fields\" : null," + NEWLINE
          + "        \"memberSchema\" : null" + NEWLINE
          + "      }" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"type\" : \"TABLE\"," + NEWLINE
          + "    \"timestamp\" : \"2000-01-01\"," + NEWLINE
          + "    \"statistics\" : \"The statistics field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use statisticsMap instead.\\nstats\"," + NEWLINE
          + "    \"errorStats\" : \"The errorStats field is deprecated and will be removed in a future version of ksql. Please update your client to the latest version and use errorStatsMap instead.\\nerrors\\n\"," + NEWLINE
          + "    \"statisticsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"errorStatsMap\" : {" + NEWLINE + "      \"TEST\" : {" + NEWLINE + "        \"name\" : \"TEST\"," + NEWLINE + "        \"value\" : 0.0," + NEWLINE + "        \"timestamp\" : 0" + NEWLINE + "      }" + NEWLINE + "    }," + NEWLINE
          + "    \"extended\" : true," + NEWLINE
          + "    \"keyFormat\" : \"json\"," + NEWLINE
          + "    \"valueFormat\" : \"avro\"," + NEWLINE
          + "    \"topic\" : \"kafka-topic\"," + NEWLINE
          + "    \"partitions\" : 2," + NEWLINE
          + "    \"replication\" : 1," + NEWLINE
          + "    \"statement\" : \"sql statement text\"," + NEWLINE
          + "    \"queryOffsetSummaries\" : [ {" + NEWLINE
          + "      \"groupId\" : \"consumer1\"," + NEWLINE
          + "      \"topicSummaries\" : [ {" + NEWLINE
          + "        \"kafkaTopic\" : \"kafka-topic\"," + NEWLINE
          + "        \"offsets\" : [ {" + NEWLINE
          + "          \"partition\" : 0," + NEWLINE
          + "          \"logStartOffset\" : 100," + NEWLINE
          + "          \"logEndOffset\" : 900," + NEWLINE
          + "          \"consumerOffset\" : 800" + NEWLINE
          + "        }, {" + NEWLINE
          + "          \"partition\" : 1," + NEWLINE
          + "          \"logStartOffset\" : 50," + NEWLINE
          + "          \"logEndOffset\" : 900," + NEWLINE
          + "          \"consumerOffset\" : 900" + NEWLINE
          + "        } ]" + NEWLINE
          + "      }, {" + NEWLINE
          + "        \"kafkaTopic\" : \"kafka-topic-2\"," + NEWLINE
          + "        \"offsets\" : [ {" + NEWLINE
          + "          \"partition\" : 0," + NEWLINE
          + "          \"logStartOffset\" : 0," + NEWLINE
          + "          \"logEndOffset\" : 90," + NEWLINE
          + "          \"consumerOffset\" : 80" + NEWLINE
          + "        }, {" + NEWLINE
          + "          \"partition\" : 1," + NEWLINE
          + "          \"logStartOffset\" : 10," + NEWLINE
          + "          \"logEndOffset\" : 90," + NEWLINE
          + "          \"consumerOffset\" : 90" + NEWLINE
          + "        } ]" + NEWLINE
          + "      } ]" + NEWLINE
          + "    }, {" + NEWLINE
          + "      \"groupId\" : \"consumer2\"," + NEWLINE
          + "      \"topicSummaries\" : [ ]" + NEWLINE
          + "    } ]," + NEWLINE
          + "    \"sourceConstraints\" : [ \"S1\", \"S2\" ]" + NEWLINE
          + "  }," + NEWLINE
          + "  \"warnings\" : [ ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + "Name                 : TestSource" + NEWLINE
          + "Type                 : TABLE" + NEWLINE
          + "Timestamp field      : 2000-01-01" + NEWLINE
          + "Key format           : json" + NEWLINE
          + "Value format         : avro" + NEWLINE
          + "Kafka topic          : kafka-topic (partitions: 2, replication: 1)" + NEWLINE
          + "Statement            : sql statement text" + NEWLINE
          + "" + NEWLINE
          + " Field  | Type                           " + NEWLINE
          + "-----------------------------------------" + NEWLINE
          + " ROWKEY | VARCHAR(STRING)  (primary key) " + NEWLINE
          + " f_0    | VARCHAR(STRING)                " + NEWLINE
          + "-----------------------------------------" + NEWLINE
          + "" + NEWLINE
          + "Sources that have a DROP constraint on this source" + NEWLINE
          + "--------------------------------------------------" + NEWLINE
          + "S1" + NEWLINE
          + "S2" + NEWLINE
          + "" + NEWLINE
          + "Queries that read from this TABLE" + NEWLINE
          + "-----------------------------------" + NEWLINE
          + "readId (" + AGGREGATE_STATUS +") : read query" + NEWLINE
          + "\n"
          + "For query topology and execution plan please run: EXPLAIN <QueryId>" + NEWLINE
          + "" + NEWLINE
          + "Queries that write from this TABLE" + NEWLINE
          + "-----------------------------------" + NEWLINE
          + "writeId (" + AGGREGATE_STATUS + ") : write query" + NEWLINE
          + "\n"
          + "For query topology and execution plan please run: EXPLAIN <QueryId>" + NEWLINE
          + "" + NEWLINE
          + "Local runtime statistics" + NEWLINE
          + "------------------------" + NEWLINE
          + "            TEST:         0     last-message:       n/a" + NEWLINE
          + "            TEST:         0     last-message:       n/a" + NEWLINE
          + "(Statistics of the local KSQL server interaction with the Kafka topic kafka-topic)"
          + NEWLINE
          + NEWLINE
          + "Consumer Groups summary:" + NEWLINE
          + NEWLINE
          + "Consumer Group       : consumer1" + NEWLINE
          + NEWLINE
          + "Kafka topic          : kafka-topic" + NEWLINE
          + "Max lag              : 100" + NEWLINE
          + NEWLINE
          + " Partition | Start Offset | End Offset | Offset | Lag " + NEWLINE
          + "------------------------------------------------------" + NEWLINE
          + " 0         | 100          | 900        | 800    | 100 " + NEWLINE
          + " 1         | 50           | 900        | 900    | 0   " + NEWLINE
          + "------------------------------------------------------" + NEWLINE
          + NEWLINE
          + "Kafka topic          : kafka-topic-2" + NEWLINE
          + "Max lag              : 10" + NEWLINE
          + NEWLINE
          + " Partition | Start Offset | End Offset | Offset | Lag " + NEWLINE
          + "------------------------------------------------------" + NEWLINE
          + " 0         | 0            | 90         | 80     | 10  " + NEWLINE
          + " 1         | 10           | 90         | 90     | 0   " + NEWLINE
          + "------------------------------------------------------" + NEWLINE
          + NEWLINE
          + "Consumer Group       : consumer2" + NEWLINE
          + "<no offsets committed by this group yet>" + NEWLINE
      ));
    }
  }

  @Test
  public void shouldPrintWarnings() {
    // Given:
    final KsqlEntity entity = new SourceDescriptionEntity(
        "e",
        sourceDescription,
        ImmutableList.of(new KsqlWarning("oops"), new KsqlWarning("doh!"))
    );

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(
          output,
          containsString("WARNING: oops" + NEWLINE + "WARNING: doh")
      );
    } else {
      assertThat(output, containsString("\"message\" : \"oops\""));
      assertThat(output, containsString("\"message\" : \"doh!\""));
    }
  }

  @Test
  public void shouldPrintDropConnector() {
    // Given:
    final KsqlEntity entity = new DropConnectorEntity("statementText", "connectorName");

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(
          output,
          is("" + NEWLINE
              + " Message                           " + NEWLINE
              + "-----------------------------------" + NEWLINE
              + " Dropped connector \"connectorName\" " + NEWLINE
              + "-----------------------------------" + NEWLINE)
      );
    } else {
      assertThat(
          output,
          is("[ {" + NEWLINE
              + "  \"statementText\" : \"statementText\"," + NEWLINE
              + "  \"connectorName\" : \"connectorName\"," + NEWLINE
              + "  \"warnings\" : [ ]" + NEWLINE
              + "} ]" + NEWLINE)
      );
    }
  }

  @Test
  public void shouldPrintErrorEntityLongNonJson() {
    // Given:
    final KsqlEntity entity = new ErrorEntity(
        "statementText",
        Strings.repeat("Not a JSON value! ", 10));

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(
          output,
          is("" + NEWLINE
              + " Error                                                        " + NEWLINE
              + "--------------------------------------------------------------" + NEWLINE
              + " Not a JSON value! Not a JSON value! Not a JSON value! Not a " + NEWLINE
              + "JSON value! Not a JSON value! Not a JSON value! Not a JSON v" + NEWLINE
              + "alue! Not a JSON value! Not a JSON value! Not a JSON value!  " + NEWLINE
              + "--------------------------------------------------------------" + NEWLINE)
      );
    }
  }

  @Test
  public void shouldPrintErrorEntityLongJson() throws IOException {
    // Given:
    final KsqlEntity entity = new ErrorEntity(
        "statementText",
        new ObjectMapper().writeValueAsString(ImmutableMap.of(
            "foo", "bar",
            "message", "a " + StringUtils.repeat("really ", 20) + " long message"
        )));

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.TABULAR) {
      assertThat(
          output,
          containsString(""
              + "----------------------------------------------------------------------------------------------------"
              + NEWLINE
              + " {" + NEWLINE
              + "  \"foo\" : \"bar\"," + NEWLINE
              + "  \"message\" : \"a really really really really really really really really really really really really really really really really really really really really  long message\""
              + NEWLINE
              + "} " + NEWLINE
              + "----------------------------------------------------------------------------------------------------")
      );
    }
  }

  @Test
  public void shouldPrintFunctionDescription() {
    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(
        new FunctionDescriptionList(
            "DESCRIBE FUNCTION foo;",
            "FOO",
            "Description that is very, very, very, very, very, very, very, very, very, "
                + "very, very, very, very, very, very, very, very, very, very, very, very long\n"
                + "and containing new lines\n"
                + "\tAND TABS\n"
                + "too!",
            "Andy",
            "v1.1.0",
            "some.jar",
            ImmutableList.of(new FunctionInfo(
                ImmutableList.of(
                    new ArgumentInfo(
                        "arg1",
                        "INT",
                        "Another really, really, really, really, really, really, really,"
                            + "really, really, really, really, really, really, really, really "
                            + " really, really, really, really, really, really, really, long\n"
                            + "description\n"
                            + "\tContaining Tabs\n"
                            + "and stuff",
                        true)
                ),
                "LONG",
                "The function description, which too can be really, really, really, "
                    + "really, really, really, really, really, really, really, really, really, "
                    + "really, really, really, really, really, really, really, really, long\n"
                    + "and contains\n\ttabs and stuff"
            )), FunctionType.SCALAR
        )));

    console.printKsqlEntityList(entityList);

    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, containsString("\"name\" : \"FOO\""));
    } else {
      final String expected = "" + NEWLINE
          + "Name        : FOO" + NEWLINE
          + "Author      : Andy" + NEWLINE
          + "Version     : v1.1.0" + NEWLINE
          + "Overview    : Description that is very, very, very, very, very, very, very, very, very, very, very, "
          + NEWLINE
          + "              very, very, very, very, very, very, very, very, very, very long"
          + NEWLINE
          + "              and containing new lines" + NEWLINE
          + "              \tAND TABS" + NEWLINE
          + "              too!" + NEWLINE
          + "Type        : SCALAR" + NEWLINE
          + "Jar         : some.jar" + NEWLINE
          + "Variations  : " + NEWLINE
          + "" + NEWLINE
          + "\tVariation   : FOO(arg1 INT[])" + NEWLINE
          + "\tReturns     : LONG" + NEWLINE
          + "\tDescription : The function description, which too can be really, really, really, really, really, "
          + NEWLINE
          + "                really, really, really, really, really, really, really, really, really, really, "
          + NEWLINE
          + "                really, really, really, really, really, long" + NEWLINE
          + "                and contains" + NEWLINE
          + "                \ttabs and stuff" + NEWLINE
          + "\targ1        : Another really, really, really, really, really, really, really,really, really, "
          + NEWLINE
          + "                really, really, really, really, really, really  really, really, really, really, "
          + NEWLINE
          + "                really, really, really, long" + NEWLINE
          + "                description" + NEWLINE
          + "                \tContaining Tabs" + NEWLINE
          + "                and stuff";

      assertThat(output, containsString(expected));
    }
  }

  @Test
  public void shouldExecuteCliCommands() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME)
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of()), any());
  }

  @Test
  public void shouldExecuteCliCommandWithArgsTrimmingWhiteSpace() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0" + WHITE_SPACE + "Arg1" + WHITE_SPACE)
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg1")), any());
  }

  @Test
  public void shouldExecuteCliCommandWithQuotedArgsContainingSpaces() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg 1")), any());
  }

  @Test
  public void shouldSupportOtherWhitespaceBetweenCliCommandAndArgs() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + "\tArg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0", "Arg 1")), any());
  }

  @Test
  public void shouldSupportCmdBeingTerminatedWithSemiColon() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE  + "Arg0;")
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0")), any());
  }

  @Test
  public void shouldSupportCmdBeingTerminatedWithSemiColonAndWhitespace() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0; " + NEWLINE)
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0")), any());
  }

  @Test
  public void shouldSupportCmdWithQuotedArgBeingTerminatedWithSemiColon() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE  + "'Arg0';")
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand).execute(eq(ImmutableList.of("Arg0")), any());
  }

  @Test
  public void shouldFailIfCommandNameIsQuoted() {
    // Given:
    when(lineSupplier.get())
        .thenReturn("'some' 'command' " + "Arg0" + WHITE_SPACE + "'Arg 1'")
        .thenReturn("not a CLI command;");

    // When:
    console.nextNonCliCommand();

    // Then:
    verify(cliCommand, never()).execute(any(), any());
  }

  @Test
  public void shouldSwallowCliCommandLines() {
    // Given:
    when(lineSupplier.get())
        .thenReturn(CLI_CMD_NAME)
        .thenReturn("not a CLI command;");

    // When:
    final String result = console.nextNonCliCommand();

    // Then:
    assertThat(result, is("not a CLI command;"));
  }

  @Test
  public void shouldSwallowCliCommandLinesEvenWithWhiteSpace() {
    // Given:
    when(lineSupplier.get())
        .thenReturn("   \t   " + CLI_CMD_NAME + "   \t   ")
        .thenReturn("not a CLI command;");

    // When:
    final String result = console.nextNonCliCommand();

    // Then:
    assertThat(result, is("not a CLI command;"));
  }

  @Test
  public void shouldThrowOnInvalidCliProperty() {
    // When:
    console.setCliProperty("FOO", "BAR");

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Undefined property: FOO. Valid properties are"));
  }

  @Test
  public void shouldThrowOnInvalidCliPropertyValue() {
    // When:
    console.setCliProperty(CliConfig.WRAP_CONFIG, "BURRITO");

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Invalid value BURRITO for configuration WRAP: String must be one of: ON, OFF, null"));
  }

  private static List<FieldInfo> buildTestSchema(final SqlType... fieldTypes) {
    final Builder schemaBuilder = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING);

    for (int idx = 0; idx < fieldTypes.length; idx++) {
      schemaBuilder.valueColumn(ColumnName.of("f_" + idx), fieldTypes[idx]);
    }

    final LogicalSchema schema = schemaBuilder.build();

    return EntityUtil.buildSourceSchemaEntity(schema);
  }
}
