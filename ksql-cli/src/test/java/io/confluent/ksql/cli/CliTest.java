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

package io.confluent.ksql.cli;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static javax.ws.rs.core.Response.Status.NOT_ACCEPTABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.Console.RowCaptor;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommand;
import io.confluent.ksql.cli.console.cmd.RequestPipeliningCommand;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.ProcessingException;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Most tests in CliTest are end-to-end integration tests, so it may expect a long running time.
 */
@SuppressWarnings("SameParameterValue")
@RunWith(MockitoJUnitRunner.class)
@Category({IntegrationTest.class})
public class CliTest {

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster.build();
  private static final String SERVER_OVERRIDE = "SERVER";
  private static final String SESSION_OVERRIDE = "SESSION";

  private static final Pattern WRITE_QUERIES = Pattern
      .compile(".*The following queries write into this source: \\[(.+)].*", Pattern.DOTALL);

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(CLUSTER::bootstrapServers)
      .withProperty(KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
          KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention + 1)
      .build();

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(CLUSTER)
      .around(REST_APP);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(30, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  private static final String COMMANDS_KSQL_TOPIC_NAME = KsqlRestApplication.COMMANDS_STREAM_NAME;
  private static final OutputFormat CLI_OUTPUT_FORMAT = OutputFormat.TABULAR;

  private static final long STREAMED_QUERY_ROW_LIMIT = 10000;
  private static final long STREAMED_QUERY_TIMEOUT_MS = 10000;

  private static final List<List<String>> EMPTY_RESULT = ImmutableList.of();

  private static TopicProducer topicProducer;
  private static TopicConsumer topicConsumer;
  private static KsqlRestClient restClient;
  private static OrderDataProvider orderDataProvider;

  private Console console;
  private TestTerminal terminal;
  private TestRowCaptor rowCaptor;
  @Mock
  private Supplier<String> lineSupplier;
  private Cli localCli;
  private String streamName;

  @BeforeClass
  public static void classSetUp() throws Exception {
    restClient = new KsqlRestClient(REST_APP.getHttpListener().toString());

    orderDataProvider = new OrderDataProvider();
    CLUSTER.createTopic(orderDataProvider.topicName());

    topicProducer = new TopicProducer(CLUSTER);
    topicConsumer = new TopicConsumer(CLUSTER);

    produceInputStream(orderDataProvider);

    try (Cli cli = Cli.build(1L, 1000L, OutputFormat.JSON, restClient)) {
      createKStream(orderDataProvider, cli);
    }
  }

  @Before
  public void setUp() {
    streamName = KsqlIdentifierTestUtil.uniqueIdentifierName();
    terminal = new TestTerminal(lineSupplier);
    rowCaptor = new TestRowCaptor();
    console = new Console(CLI_OUTPUT_FORMAT, terminal, rowCaptor);

    localCli = new Cli(
        STREAMED_QUERY_ROW_LIMIT,
        STREAMED_QUERY_TIMEOUT_MS,
        restClient,
        console
    );

    maybeDropStream("SHOULDRUNSCRIPT");
  }

  private void assertRunListCommand(
      final String commandSuffix,
      final Matcher<Iterable<? extends Iterable<? extends String>>> matcher
  ) {
    assertRunCommand("list " + commandSuffix, matcher);
    assertRunCommand("show " + commandSuffix, matcher);
  }

  private void assertRunCommand(
      final String command,
      final Matcher<Iterable<? extends Iterable<? extends String>>> matcher
  ) {
    rowCaptor.resetTestResult();
    run(command, localCli);
    assertThat(rowCaptor.getRows(), matcher);
  }

  private static void run(String command, final Cli localCli) {
    try {
      if (!command.endsWith(";")) {
        command += ";";
      }
      System.out.println("[Run Command] " + command);
      localCli.handleLine(command);
    } catch (final Exception e) {
      throw new AssertionError("Failed to run command: " + command, e);
    }
  }

  private static void produceInputStream(final TestDataProvider dataProvider) throws Exception {
    topicProducer.produceInputData(dataProvider);
  }

  private static void createKStream(final TestDataProvider dataProvider, final Cli cli) {
    run(String.format(
        "CREATE STREAM %s %s WITH (value_format = 'json', kafka_topic = '%s' , key='%s')",
        dataProvider.kstreamName(), dataProvider.ksqlSchemaString(), dataProvider.topicName(),
        dataProvider.key()),
        cli);
  }

  @After
  public void tearDown() {
    System.out.println("[Terminal Output]");
    System.out.println(terminal.getOutputString());

    localCli.close();
    console.close();
  }

  @AfterClass
  public static void classTearDown() {
    restClient.close();
  }

  private void testCreateStreamAsSelect(
      String selectQuery,
      final PhysicalSchema resultSchema,
      final Map<String, GenericRow> expectedResults
  ) {
    if (!selectQuery.endsWith(";")) {
      selectQuery += ";";
    }
    final String queryString = "CREATE STREAM " + streamName + " AS " + selectQuery;

    /* Start Stream Query */
    assertRunCommand(
        queryString,
        anyOf(
            isRow(containsString("Stream created and running")),
            isRow(is("Parsing statement")),
            isRow(is("Executing statement"))));

    /* Assert Results */
    final Map<String, GenericRow> results = topicConsumer
        .readResults(streamName, resultSchema, expectedResults.size(), new StringDeserializer());

    dropStream(streamName);

    assertThat(results, equalTo(expectedResults));
  }

  private static void runStatement(final String statement, final KsqlRestClient restClient) {
    final RestResponse<?> response = restClient.makeKsqlRequest(statement, null);
    assertThat(response.isSuccessful(), is(true));
    final KsqlEntityList entityList = ((KsqlEntityList) response.get());
    assertThat(entityList.size(), equalTo(1));
    assertThat(entityList.get(0), instanceOf(CommandStatusEntity.class));
    final CommandStatusEntity entity = (CommandStatusEntity) entityList.get(0);
    final CommandStatus status = entity.getCommandStatus();
    assertThat(status, not(CommandStatus.Status.ERROR));

    if (status.getStatus() != Status.SUCCESS) {
      assertThatEventually(
          "",
          () -> {
            final RestResponse<?> statusResponse = restClient
                .makeStatusRequest(entity.getCommandId().toString());
            assertThat(statusResponse.isSuccessful(), is(true));
            assertThat(statusResponse.get(), instanceOf(CommandStatus.class));
            return ((CommandStatus) statusResponse.get()).getStatus();
          },
          anyOf(
              is(CommandStatus.Status.SUCCESS),
              is(CommandStatus.Status.TERMINATED),
              is(CommandStatus.Status.ERROR)),
          120,
          TimeUnit.SECONDS
      );
    }
  }

  private static void terminateQuery(final String queryId) {
    runStatement(String.format("terminate %s;", queryId), restClient);
  }

  private static void dropStream(final String name) {
    final String dropStatement = String.format("drop stream %s;", name);

    final RestResponse<?> response = restClient.makeKsqlRequest(dropStatement, null);
    if (response.isSuccessful()) {
      return;
    }

    final java.util.regex.Matcher matcher = WRITE_QUERIES
        .matcher(response.getErrorMessage().getMessage());

    if (!matcher.matches()) {
      throw new RuntimeException("Failed to drop stream: " + response.getErrorMessage());
    }

    Arrays.stream(matcher.group(1).split("/w*,/w*"))
        .forEach(CliTest::terminateQuery);

    runStatement(dropStatement, restClient);
  }

  private static void maybeDropStream(final String name) {
    final String dropStatement = String.format("drop stream %s;", name);

    final RestResponse<?> response = restClient.makeKsqlRequest(dropStatement, null);
    if (response.isSuccessful()
        || response.getErrorMessage().toString().contains("does not exist")) {
      return;
    }

    dropStream(name);
  }

  private void selectWithLimit(
      final String selectQuery,
      final int limit,
      final Matcher<Iterable<? extends Iterable<? extends String>>> expectedResults
  ) {
    final String query = selectQuery + " LIMIT " + limit + ";";
    assertRunCommand(query, expectedResults);
  }

  @Test
  public void shouldPrintResultsForListOrShowCommands() {
    assertRunListCommand(
        "topics",
        hasRow(
            equalTo(orderDataProvider.topicName()),
            equalTo("1"),
            equalTo("1")
        )
    );

    assertRunListCommand(
        "topics extended",
        hasRow(
            equalTo(orderDataProvider.topicName()),
            equalTo("1"),
            equalTo("1"),
            any(String.class),
            any(String.class)
        )
    );

    assertRunListCommand(
        "streams",
        isRow(orderDataProvider.kstreamName(), orderDataProvider.topicName(), "JSON")
    );

    assertRunListCommand("tables", is(EMPTY_RESULT));
    assertRunListCommand("queries", is(EMPTY_RESULT));
  }

  @Test
  public void testPrint() {
    final Thread thread =
        new Thread(() -> run("print 'ORDER_TOPIC' FROM BEGINNING INTERVAL 2;", localCli));
    thread.start();

    try {
      assertThatEventually(() -> terminal.getOutputString(), containsString("Format:JSON"));
    } finally {
      thread.interrupt();

      try {
        thread.join(0);
      } catch (InterruptedException e) {
        //
      }
    }
  }

  @Test
  public void testPropertySetUnset() {
    assertRunCommand("set 'auto.offset.reset' = 'latest'", is(EMPTY_RESULT));
    assertRunCommand("set 'application.id' = 'Test_App'", is(EMPTY_RESULT));
    assertRunCommand("set 'producer.batch.size' = '16384'", is(EMPTY_RESULT));
    assertRunCommand("set 'max.request.size' = '1048576'", is(EMPTY_RESULT));
    assertRunCommand("set 'consumer.max.poll.records' = '500'", is(EMPTY_RESULT));
    assertRunCommand("set 'enable.auto.commit' = 'true'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.application.id' = 'Test_App'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.producer.batch.size' = '16384'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.max.request.size' = '1048576'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.consumer.max.poll.records' = '500'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.enable.auto.commit' = 'true'", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.service.id' = 'assertPrint'", is(EMPTY_RESULT));

    assertRunCommand("unset 'application.id'", is(EMPTY_RESULT));
    assertRunCommand("unset 'producer.batch.size'", is(EMPTY_RESULT));
    assertRunCommand("unset 'max.request.size'", is(EMPTY_RESULT));
    assertRunCommand("unset 'consumer.max.poll.records'", is(EMPTY_RESULT));
    assertRunCommand("unset 'enable.auto.commit'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.application.id'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.producer.batch.size'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.max.request.size'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.consumer.max.poll.records'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.enable.auto.commit'", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.service.id'", is(EMPTY_RESULT));

    assertRunListCommand("properties", hasRows(
        // SERVER OVERRIDES:
        row(
            KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG,
            SERVER_OVERRIDE,
            "4"
        ),
        row(
            KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
            SERVER_OVERRIDE,
            "" + (KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention + 1)
        ),
        // SESSION OVERRIDES:
        row(
            KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            SESSION_OVERRIDE,
            "latest"
        )
    ));

    assertRunCommand("unset 'auto.offset.reset'", is(EMPTY_RESULT));
  }

  @Test
  public void shouldPrintCorrectSchemaForDescribeStream() {
    assertRunCommand(
        "describe " + orderDataProvider.kstreamName(),
        containsRows(
            row("ROWTIME", "BIGINT           (system)"),
            row("ROWKEY", "VARCHAR(STRING)  (system)"),
            row("ORDERTIME", "BIGINT"),
            row("ORDERID", "VARCHAR(STRING)"),
            row("ITEMID", "VARCHAR(STRING)"),
            row("ORDERUNITS", "DOUBLE"),
            row("TIMESTAMP", "VARCHAR(STRING)"),
            row("PRICEARRAY", "ARRAY<DOUBLE>"),
            row("KEYVALUEMAP", "MAP<STRING, DOUBLE>")
        ));
  }

  @Test
  public void testPersistentSelectStar() {
    testCreateStreamAsSelect(
        "SELECT * FROM " + orderDataProvider.kstreamName(),
        orderDataProvider.schema(),
        orderDataProvider.data()
    );
  }

  @Test
  public void testSelectProject() {
    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(
        ImmutableList.of(
            "ITEM_1",
            10.0,
            new Double[]{100.0, 110.99, 90.0})));
    expectedResults.put("2", new GenericRow(
        ImmutableList.of(
            "ITEM_2",
            20.0,
            new Double[]{10.0, 10.99, 9.0})));

    expectedResults.put("3", new GenericRow(
        ImmutableList.of(
            "ITEM_3",
            30.0,
            new Double[]{10.0, 10.99, 91.0})));

    expectedResults.put("4", new GenericRow(
        ImmutableList.of(
            "ITEM_4",
            40.0,
            new Double[]{10.0, 140.99, 94.0})));

    expectedResults.put("5", new GenericRow(
        ImmutableList.of(
            "ITEM_5",
            50.0,
            new Double[]{160.0, 160.99, 98.0})));

    expectedResults.put("6", new GenericRow(
        ImmutableList.of(
            "ITEM_6",
            60.0,
            new Double[]{1000.0, 1100.99, 900.0})));

    expectedResults.put("7", new GenericRow(
        ImmutableList.of(
            "ITEM_7",
            70.0,
            new Double[]{1100.0, 1110.99, 190.0})));

    expectedResults.put("8", new GenericRow(
        ImmutableList.of(
            "ITEM_8",
            80.0,
            new Double[]{1100.0, 1110.99, 970.0})));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        LogicalSchema.of(SchemaBuilder.struct()
            .field("ITEMID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("ORDERUNITS", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
            .field("PRICEARRAY", SchemaBuilder
                .array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                .optional()
                .build())
            .build()),
        SerdeOption.none()
    );

    testCreateStreamAsSelect(
        "SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM " + orderDataProvider.kstreamName(),
        resultSchema,
        expectedResults
    );
  }

  @Test
  public void testSelectFilter() {
    final Map<String, GenericRow> expectedResults = new HashMap<>();
    final Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);
    expectedResults.put("8", new GenericRow(
        ImmutableList.of(
            8,
            "ORDER_6",
            "ITEM_8",
            80.0,
            "2018-01-08",
            new Double[]{1100.0, 1110.99, 970.0},
            mapField)));

    testCreateStreamAsSelect(
        "SELECT * FROM " + orderDataProvider.kstreamName() + " WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'",
        orderDataProvider.schema(),
        expectedResults
    );
  }

  @Test
  public void testTransientSelect() {
    final Map<String, GenericRow> streamData = orderDataProvider.data();
    final List<Object> row1 = streamData.get("1").getColumns();
    final List<Object> row2 = streamData.get("2").getColumns();
    final List<Object> row3 = streamData.get("3").getColumns();

    selectWithLimit(
        "SELECT ORDERID, ITEMID FROM " + orderDataProvider.kstreamName(),
        3,
        containsRows(
            row(row1.get(1).toString(), row1.get(2).toString()),
            row(row2.get(1).toString(), row2.get(2).toString()),
            row(row3.get(1).toString(), row3.get(2).toString())
        ));
  }

  @Test
  public void testTransientSelectStar() {
    final Map<String, GenericRow> streamData = orderDataProvider.data();
    final List<Object> row1 = streamData.get("1").getColumns();
    final List<Object> row2 = streamData.get("2").getColumns();
    final List<Object> row3 = streamData.get("3").getColumns();

    selectWithLimit(
        "SELECT * FROM " + orderDataProvider.kstreamName(),
        3,
        containsRows(
            row(prependWithRowTimeAndKey(row1)),
            row(prependWithRowTimeAndKey(row2)),
            row(prependWithRowTimeAndKey(row3))
        ));
  }

  @Test
  public void testTransientHeader() {
    // When:
    rowCaptor.resetTestResult();
    run("SELECT * FROM " + orderDataProvider.kstreamName() + " LIMIT 1", localCli);

    // Then: (note that some of these are truncated because of header wrapping)
    assertThat(terminal.getOutputString(), containsString("ROWTIME"));
    assertThat(terminal.getOutputString(), containsString("ROWKEY"));
    assertThat(terminal.getOutputString(), containsString("ITEMID"));
    assertThat(terminal.getOutputString(), containsString("ORDERID"));
    assertThat(terminal.getOutputString(), containsString("ORDERUNIT"));
    assertThat(terminal.getOutputString(), containsString("TIMESTAMP"));
    assertThat(terminal.getOutputString(), containsString("PRICEARRA"));
    assertThat(terminal.getOutputString(), containsString("KEYVALUEM"));
  }

  @Test
  public void testSelectUDFs() {
    final String queryString = String.format(
        "SELECT ITEMID, "
            + "ORDERUNITS*10 AS Col1, "
            + "PRICEARRAY[0]+10 AS Col2, "
            + "KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10 AS Col3, "
            + "PRICEARRAY[1]>1000 AS Col4 "
            + "FROM %s "
            + "WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        orderDataProvider.kstreamName()
    );

    final Schema sourceSchema = orderDataProvider.schema().logicalSchema().valueSchema();
    final PhysicalSchema resultSchema = PhysicalSchema.from(
        LogicalSchema.of(SchemaBuilder.struct()
            .field("ITEMID", sourceSchema.field("ITEMID").schema())
            .field("COL1", sourceSchema.field("ORDERUNITS").schema())
            .field("COL2", sourceSchema.field("PRICEARRAY").schema().valueSchema())
            .field("COL3", sourceSchema.field("KEYVALUEMAP").schema().valueSchema())
            .field("COL4", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .build()),
        SerdeOption.none()
    );

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(ImmutableList.of("ITEM_8", 800.0, 1110.0, 12.0, true)));

    testCreateStreamAsSelect(queryString, resultSchema, expectedResults);
  }

  // ===================================================================
  // Below Tests are only used for coverage, not for results validation.
  // ===================================================================

  @Test
  public void testRunInteractively() {
    // Given:
    givenRunInteractivelyWillExit();

    // When:
    localCli.runInteractively();
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServer() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.makeRootRequest())
        .thenThrow(new KsqlRestClientException("Boom", new ProcessingException("")));

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(terminal.getOutputString(),
               containsString("Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldRegisterRemoteCommand() {
    assertThat(console.getCliSpecificCommands().get("server"),
        instanceOf(RemoteServerSpecificCommand.class));
  }

  @Test
  public void shouldRegisterRequestPipeliningCommand() {
    assertThat(console.getCliSpecificCommands().get(RequestPipeliningCommand.NAME),
        instanceOf(RequestPipeliningCommand.class));
  }

  @Test
  public void shouldPrintErrorOnUnsupportedAPI() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.makeRootRequest()).thenReturn(
        RestResponse.erroneous(
            new KsqlErrorMessage(
                Errors.toErrorCode(NOT_ACCEPTABLE.getStatusCode()),
                "Minimum supported client version: 1.0")));

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(
        terminal.getOutputString(),
        containsString("This CLI version no longer supported"));
    assertThat(
        terminal.getOutputString(),
        containsString("Minimum supported client version: 1.0"));
  }

  @Test
  public void shouldListFunctions() {
    assertRunListCommand("functions", hasRows(
        row("TIMESTAMPTOSTRING", "SCALAR"),
        row("EXTRACTJSONFIELD", "SCALAR"),
        row("TOPK", "AGGREGATE")
    ));
  }

  @Test
  public void shouldDescribeScalarFunction() throws Exception {
    final String expectedOutput =
        "Name        : TIMESTAMPTOSTRING\n"
            + "Author      : Confluent\n"
            + "Overview    : Converts a BIGINT millisecond timestamp value into the string"
            + " representation of the \n"
            + "              timestamp in the given format.\n"
            + "Type        : scalar\n"
            + "Jar         : internal\n"
            + "Variations  :";

    localCli.handleLine("describe function timestamptostring;");
    final String outputString = terminal.getOutputString();
    assertThat(outputString, containsString(expectedOutput));

    // variations for Udfs are loaded non-deterministically. Don't assume which variation is first
    final String expectedVariation =
        "\tVariation   : TIMESTAMPTOSTRING(epochMilli BIGINT, formatPattern VARCHAR)\n" +
                "\tReturns     : VARCHAR\n" +
                "\tDescription : Converts a BIGINT millisecond timestamp value into the string" +
                " representation of the \n" +
                "                timestamp in the given format. Single quotes in the timestamp" +
                " format can be escaped \n" +
                "                with '', for example: 'yyyy-MM-dd''T''HH:mm:ssX' The system" +
                " default time zone is \n" +
                "                used when no time zone is explicitly provided. The format" +
                " pattern should be in the \n" +
                "                format expected by java.time.format.DateTimeFormatter\n" +
                "\tepochMilli  : Milliseconds since January 1, 1970, 00:00:00 GMT.\n" +
                "\tformatPattern: The format pattern should be in the format expected by \n" +
                "                 java.time.format.DateTimeFormatter.";

    assertThat(outputString, containsString(expectedVariation));
  }

  @Test
  public void shouldDescribeOverloadedScalarFunction() throws Exception {
    // Given:
    localCli.handleLine("describe function substring;");

    // Then:
    final String output = terminal.getOutputString();

    // Summary output:
    assertThat(output, containsString(
        "Name        : SUBSTRING\n"
        + "Author      : Confluent\n"
        + "Overview    : Returns a substring of the passed in value.\n"
    ));
    assertThat(output, containsString(
        "Type        : scalar\n"
        + "Jar         : internal\n"
        + "Variations  :"
    ));

    // Variant output:
    assertThat(output, containsString(
        "\tVariation   : SUBSTRING(str VARCHAR, pos INT)\n"
        + "\tReturns     : VARCHAR\n"
        + "\tDescription : Returns a substring of str that starts at pos and continues to the end"
    ));
    assertThat(output, containsString(
        "\tstr         : The source string. If null, then function returns null.\n"
        + "\tpos         : The base-one position the substring starts from."
    ));
  }

  @Test
  public void shouldDescribeAggregateFunction() throws Exception {
    final String expectedSummary =
            "Name        : TOPK\n" +
            "Author      : confluent\n" +
            "Type        : aggregate\n" +
            "Jar         : internal\n" +
            "Variations  : \n";

    final String expectedVariant =
        "\tVariation   : TOPK(INT)\n"
        + "\tReturns     : ARRAY<INT>\n"
        + "\tDescription : Calculates the TopK value for a column, per key.";

    localCli.handleLine("describe function topk;");

    final String output = terminal.getOutputString();
    assertThat(output, containsString(expectedSummary));
    assertThat(output, containsString(expectedVariant));
  }

  @Test
  public void shouldPrintErrorIfCantFindFunction() throws Exception {
    localCli.handleLine("describe function foobar;");

    assertThat(terminal.getOutputString(),
        containsString("Can't find any functions with the name 'foobar'"));
  }

  @Test
  public void shouldHandleSetPropertyAsPartOfMultiStatementLine() throws Exception {
    // Given:
    final String csas =
        "CREATE STREAM " + streamName + " "
            + "AS SELECT * FROM " + orderDataProvider.kstreamName() + ";";

    // When:
    localCli
        .handleLine("set 'auto.offset.reset'='earliest'; " + csas);

    // Then:
    dropStream(streamName);

    assertThat(terminal.getOutputString(),
        containsString("Successfully changed local property 'auto.offset.reset' to 'earliest'"));
  }

  @Test
  public void shouldRunScript() throws Exception {
    // Given:
    final File scriptFile = TMP.newFile("script.sql");
    Files.write(scriptFile.toPath(), (""
        + "CREATE STREAM shouldRunScript AS SELECT * FROM " + orderDataProvider.kstreamName() + ";"
        + "").getBytes(StandardCharsets.UTF_8));

    when(lineSupplier.get())
        .thenReturn("run script '" + scriptFile.getAbsolutePath() + "'")
        .thenReturn("exit");

    // When:
    localCli.runInteractively();

    // Then:
    assertThat(terminal.getOutputString(), containsString("Stream created and running"));
  }

  @Test
  public void shouldUpdateCommandSequenceNumber() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    final CommandStatusEntity stubEntity = stubCommandStatusEntityWithSeqNum(12L);
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(new KsqlEntityList(
            Collections.singletonList(stubEntity))));

    // When:
    localCli.handleLine(statementText);

    // Then:
    assertLastCommandSequenceNumber(mockRestClient, 12L);
  }

  @Test
  public void shouldUpdateCommandSequenceNumberOnMultipleCommandStatusEntities() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    final CommandStatusEntity firstEntity = stubCommandStatusEntityWithSeqNum(12L);
    final CommandStatusEntity secondEntity = stubCommandStatusEntityWithSeqNum(14L);
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(new KsqlEntityList(
            ImmutableList.of(firstEntity, secondEntity))));

    // When:
    localCli.handleLine(statementText);

    // Then:
    assertLastCommandSequenceNumber(mockRestClient, 14L);
  }

  @Test
  public void shouldNotUpdateCommandSequenceNumberIfNoCommandStatusEntities() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(new KsqlEntityList()));

    // When:
    localCli.handleLine(statementText);

    // Then:
    assertLastCommandSequenceNumber(mockRestClient, -1L);
  }

  @Test
  public void shouldIssueRequestWithoutCommandSequenceNumberIfRequestPipeliningOn() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.makeKsqlRequest(anyString(), eq(null)))
        .thenReturn(RestResponse.successful(new KsqlEntityList()));

    givenRequestPipelining("ON");

    // When:
    localCli.handleLine(statementText);

    // Then:
    verify(mockRestClient).makeKsqlRequest(statementText, null);
  }

  @Test
  public void shouldUpdateCommandSequenceNumberEvenIfRequestPipeliningOn() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    final CommandStatusEntity stubEntity = stubCommandStatusEntityWithSeqNum(12L);
    when(mockRestClient.makeKsqlRequest(anyString(), eq(null)))
        .thenReturn(RestResponse.successful(new KsqlEntityList(
            Collections.singletonList(stubEntity))));

    givenRequestPipelining("ON");

    // When:
    localCli.handleLine(statementText);

    // Then:
    givenRequestPipelining("OFF");
    assertLastCommandSequenceNumber(mockRestClient, 12L);
  }

  @Test
  public void shouldDefaultRequestPipeliningToOff() {
    // When:
    runCliSpecificCommand(RequestPipeliningCommand.NAME);

    // Then:
    assertThat(terminal.getOutputString(),
        containsString(String.format("Current %s configuration: OFF", RequestPipeliningCommand.NAME)));
  }

  @Test
  public void shouldUpdateRequestPipelining() {
    // When:
    runCliSpecificCommand(RequestPipeliningCommand.NAME + " ON");
    runCliSpecificCommand(RequestPipeliningCommand.NAME);

    // Then:
    assertThat(terminal.getOutputString(),
        containsString(String.format("Current %s configuration: ON", RequestPipeliningCommand.NAME)));
  }

  @Test
  public void shouldResetStateWhenServerChanges() throws Exception {
    // Given:
    final KsqlRestClient mockRestClient = givenMockRestClient();
    givenCommandSequenceNumber(mockRestClient, 5L);
    givenRequestPipelining("ON");
    when(mockRestClient.makeRootRequest()).thenReturn(
        RestResponse.successful(new ServerInfo("version", "clusterId", "serviceId")));

    // When:
    runCliSpecificCommand("server foo");

    // Then:
    assertLastCommandSequenceNumber(mockRestClient, -1L);
  }

  private void givenRequestPipelining(final String setting) {
    runCliSpecificCommand(RequestPipeliningCommand.NAME + " " + setting);
  }

  private void runCliSpecificCommand(final String command) {
    when(lineSupplier.get()).thenReturn(command).thenReturn("");
    console.readLine();
  }

  private void givenRunInteractivelyWillExit() {
    when(lineSupplier.get()).thenReturn("eXiT");
  }

  private KsqlRestClient givenMockRestClient() throws Exception {
    final KsqlRestClient mockRestClient = mock(KsqlRestClient.class);

    when(mockRestClient.getServerInfo()).thenReturn(
        RestResponse.of(new ServerInfo("1.x", "testClusterId", "testServiceId")));
    when(mockRestClient.getServerAddress()).thenReturn(new URI("http://someserver:8008"));

    localCli = new Cli(
        STREAMED_QUERY_ROW_LIMIT, STREAMED_QUERY_TIMEOUT_MS, mockRestClient, console);

    return mockRestClient;
  }

  private static CommandStatusEntity stubCommandStatusEntityWithSeqNum(final long seqNum) {
    return new CommandStatusEntity(
        "stub",
        new CommandId(CommandId.Type.STREAM, "stub", CommandId.Action.CREATE),
        new CommandStatus(CommandStatus.Status.SUCCESS, "stub"),
        seqNum
    );
  }

  private void givenCommandSequenceNumber(
      final KsqlRestClient mockRestClient, final long seqNum) throws Exception {
    final CommandStatusEntity stubEntity = stubCommandStatusEntityWithSeqNum(seqNum);
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(new KsqlEntityList(
            Collections.singletonList(stubEntity))));
    localCli.handleLine("create stream foo;");
  }

  private void assertLastCommandSequenceNumber(
      final KsqlRestClient mockRestClient, final long seqNum) throws Exception {
    // Given:
    reset(mockRestClient);
    final String statementText = "list streams;";
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(new KsqlEntityList()));

    // When:
    localCli.handleLine(statementText);

    // Then:
    verify(mockRestClient).makeKsqlRequest(statementText, seqNum);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Matcher<String>[] prependWithRowTimeAndKey(final List<?> values) {

    final Matcher<String>[] allMatchers = new Matcher[values.size() + 2];
    allMatchers[0] = any(String.class);            // ROWTIME
    allMatchers[1] = is(values.get(0).toString()); // ROWKEY

    for (int idx = 0; idx != values.size(); ++idx) {
      allMatchers[idx + 2] = is(values.get(idx).toString());
    }

    return allMatchers;
  }

  private static Matcher<Iterable<? extends String>> row(final String... expected) {
    return Matchers.contains(expected);
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static Matcher<Iterable<? extends String>> row(final Matcher<String>... expected) {
    return Matchers.contains(expected);
  }

  private static Matcher<Iterable<? extends Iterable<? extends String>>> isRow(
      final String... expected
  ) {
    return Matchers.contains(row(expected));
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static Matcher<Iterable<? extends Iterable<? extends String>>> isRow(
      final Matcher<String>... expected
  ) {
    return Matchers.contains(row(expected));
  }

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked"})
  private static Matcher<Iterable<? extends Iterable<? extends String>>> hasRow(
      final Matcher<String>... expected
  ) {
    return (Matcher) Matchers.hasItems(row(expected));
  }

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked"})
  private static Matcher<Iterable<? extends Iterable<? extends String>>> hasRows(
      final Matcher<Iterable<? extends String>>... rows
  ) {
    return (Matcher) Matchers.hasItems(rows);
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static Matcher<Iterable<? extends Iterable<? extends String>>> containsRows(
      final Matcher<Iterable<? extends String>>... rows
  ) {
    return Matchers.contains(rows);
  }

  private static class TestRowCaptor implements RowCaptor {

    private ImmutableList.Builder<List<String>> rows = ImmutableList.builder();

    @Override
    public void addRows(final List<List<String>> rows) {
      rows.forEach(this::addRow);
    }

    @Override
    public void addRow(final GenericRow row) {
      addRow(row.getColumns());
    }

    private void addRow(final List<?> row) {
      final List<String> strings = row.stream()
          .map(Object::toString)
          .collect(Collectors.toList());
      rows.add(ImmutableList.copyOf(strings));
    }

    private void resetTestResult() {
      rows = ImmutableList.builder();
    }

    private List<List<String>> getRows() {
      return rows.build();
    }
  }
}
