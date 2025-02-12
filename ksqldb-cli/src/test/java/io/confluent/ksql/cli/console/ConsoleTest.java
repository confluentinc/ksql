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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import io.confluent.ksql.rest.entity.AssertSchemaEntity;
import io.confluent.ksql.rest.entity.AssertTopicEntity;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.ConnectorPluginsList;
import io.confluent.ksql.rest.entity.ConsumerPartitionOffsets;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
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
import io.confluent.ksql.rest.entity.QueryHostStat;
import io.confluent.ksql.rest.entity.QueryOffsetSummary;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.QueryTopicOffsetSummary;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.rest.entity.SimpleConnectorInfo;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.entity.TypeList;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.test.util.TimezoneRule;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.ConnectorState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class ConsoleTest {
  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  @ClassRule
  public static final TimezoneRule tzRule = new TimezoneRule(TimeZone.getTimeZone("UTC"));

  private static final String CLI_CMD_NAME = "some command";
  private static final String WHITE_SPACE = " \t ";
  private static final String NEWLINE = System.lineSeparator();
  private static final String STATUS_COUNT_STRING = "RUNNING:1,ERROR:2";
  private static final String AGGREGATE_STATUS = "ERROR";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("foo"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
      .build().withPseudoAndKeyColsInValue(false);
  private final TestTerminal terminal;
  private final Console console;
  private final Supplier<String> lineSupplier;
  private final CliSpecificCommand cliCommand;
  private final Options approvalOptions;
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

  @SuppressWarnings("unchecked")
  public ConsoleTest(final OutputFormat outputFormat) {
    this.lineSupplier = mock(Supplier.class);
    this.cliCommand = mock(CliSpecificCommand.class);
    this.terminal = new TestTerminal(lineSupplier);
    this.console = new Console(outputFormat, terminal, new NoOpRowCaptor());

    this.approvalOptions = new Options().forFile().withExtension(outputFormat.toString().toLowerCase());

    when(cliCommand.getName()).thenReturn(CLI_CMD_NAME);
    when(cliCommand.matches(any()))
        .thenAnswer(i -> ((String) i.getArgument(0)).toLowerCase().startsWith(CLI_CMD_NAME.toLowerCase()));
    console.registerCliSpecificCommand(cliCommand);

  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<OutputFormat> data() {
    return ImmutableList.of(OutputFormat.JSON, OutputFormat.TABULAR);
  }

  private static List<FieldInfo> buildTestSchema(final Optional<String> headerKey, final SqlType... fieldTypes) {
    final LogicalSchema schema = builderWithoutHeaders(fieldTypes)
        .headerColumn(ColumnName.of("HEAD"), headerKey)
        .build();

    return EntityUtil.buildSourceSchemaEntity(schema);
  }

  private static List<FieldInfo> buildTestSchema(final SqlType... fieldTypes) {
    final LogicalSchema schema = builderWithoutHeaders(fieldTypes).build();

    return EntityUtil.buildSourceSchemaEntity(schema);
  }

  private static Builder builderWithoutHeaders(final SqlType... fieldTypes) {
    final Builder schemaBuilder = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING);

    for (int idx = 0; idx < fieldTypes.length; idx++) {
      schemaBuilder.valueColumn(ColumnName.of("f_" + idx), fieldTypes[idx]);
    }

    return schemaBuilder;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
            ),
            "consumerGroupId"
        )
    );

    final KsqlEntityList entityList = new KsqlEntityList(ImmutableList.of(queryEntity));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  private SourceDescription buildSourceDescription(
      final List<RunningQuery> readQueries,
      final List<RunningQuery> writeQueries,
      final List<FieldInfo> fields,
      final boolean withClusterStats) {

    final Stat STAT = new Stat("TEST", 0, 1596644936314L);
    final Stat ERROR_STAT = new Stat("ERROR", 0, 1596644936314L);
    List<QueryHostStat> statistics = IntStream.range(1, 5)
        .boxed()
        .map((i) -> new KsqlHostInfoEntity("host" + i, 8000 + i))
        .map((host) -> QueryHostStat.fromStat(STAT, host)).collect(Collectors.toList());
    List<QueryHostStat> errors = IntStream.range(1, 5)
        .boxed()
        .map((i) -> new KsqlHostInfoEntity("host" + i, 8000 + i))
        .map((host) -> QueryHostStat.fromStat(ERROR_STAT, host)).collect(Collectors.toList());


    return new SourceDescription(
        "TestSource",
        Optional.empty(),
        readQueries,
        writeQueries,
        fields,
        DataSourceType.KTABLE.getKsqlType(),
        "2000-01-01",
        "stats",
        "errors",
        true,
        "kafka",
        "avro",
        "kafka-topic",
        1,
        1,
        "sql statement",
        Collections.emptyList(),
        Collections.emptyList(),
        withClusterStats ? statistics : ImmutableList.of(),
        withClusterStats ? errors : ImmutableList.of()
    );
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
            buildSourceDescription(readQueries, writeQueries, fields, false),
            Collections.emptyList()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void testPrintSourceDescriptionWithHeaders() {
    // Given:
    final List<FieldInfo> fields = buildTestSchema(
        Optional.empty(),
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
            buildSourceDescription(readQueries, writeQueries, fields, false),
            Collections.emptyList()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void testPrintSourceDescriptionWithExtractedHeader() {
    // Given:
    final List<FieldInfo> fields = buildTestSchema(
        Optional.of("abc"),
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
            buildSourceDescription(readQueries, writeQueries, fields, false),
            Collections.emptyList()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void testPrintSourceDescriptionWithClusterStats() {
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
            buildSourceDescription(readQueries, writeQueries, fields, true),
            Collections.emptyList()
        )
    ));

    // When:
    console.printKsqlEntityList(entityList);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintConnectorPluginsList() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new ConnectorPluginsList(
            "statement",
            ImmutableList.of(),
            ImmutableList.of(
                new SimpleConnectorPluginInfo("clazz1", ConnectorType.SOURCE, "v1"),
                new SimpleConnectorPluginInfo("clazz2", ConnectorType.SINK, "v2")
            ))
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    if (console.getOutputFormat() == OutputFormat.JSON) {
      assertThat(output, is(""
          + "[ {" + NEWLINE
          + "  \"@type\" : \"connector_plugins_list\"," + NEWLINE
          + "  \"statementText\" : \"statement\"," + NEWLINE
          + "  \"warnings\" : [ ]," + NEWLINE
          + "  \"connectorsPlugins\" : [ {" + NEWLINE
          + "    \"className\" : \"clazz1\"," + NEWLINE
          + "    \"type\" : \"source\"," + NEWLINE
          + "    \"version\" : \"v1\"" + NEWLINE
          + "  }, {" + NEWLINE
          + "    \"className\" : \"clazz2\"," + NEWLINE
          + "    \"type\" : \"sink\"," + NEWLINE
          + "    \"version\" : \"v2\"" + NEWLINE
          + "  } ]" + NEWLINE
          + "} ]" + NEWLINE));
    } else {
      assertThat(output, is("" + NEWLINE
          + " Class  | Type   | Version " + NEWLINE
          + "---------------------------" + NEWLINE
          + " clazz1 | SOURCE | v1      " + NEWLINE
          + " clazz2 | SINK   | v2      " + NEWLINE
          + "---------------------------" + NEWLINE));
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintDropConnector() {
    // Given:
    final KsqlEntity entity = new DropConnectorEntity("statementText", "connectorName");

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintTerminateQuery() {
    // Given:
    final KsqlEntity entity = new TerminateQueryEntity("statementText", "queryId", true);

    // When:
    console.printKsqlEntityList(ImmutableList.of(entity));

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
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
    Approvals.verify(output, approvalOptions);
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
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "Arg0;")
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
        .thenReturn(CLI_CMD_NAME + WHITE_SPACE + "'Arg0';")
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

  @Test
  public void shouldPrintAssertTopicResult() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new AssertTopicEntity("statement", "name", true)
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintAssertNotExistsTopicResult() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new AssertTopicEntity("statement", "name", false)
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintAssertSchemaResult() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new AssertSchemaEntity("statement", Optional.of("abc"), Optional.of(55), true)
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }

  @Test
  public void shouldPrintAssertNotExistsSchemaResult() {
    // Given:
    final KsqlEntityList entities = new KsqlEntityList(ImmutableList.of(
        new AssertSchemaEntity("statement", Optional.of("abc"), Optional.of(55), false)
    ));

    // When:
    console.printKsqlEntityList(entities);

    // Then:
    final String output = terminal.getOutputString();
    Approvals.verify(output, approvalOptions);
  }
}
