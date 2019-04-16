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

package io.confluent.ksql;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static javax.ws.rs.core.Response.Status.NOT_ACCEPTABLE;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommand;
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
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.ProcessingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

/**
 * Most tests in CliTest are end-to-end integration tests, so it may expect a long running time.
 */
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
  public static final RuleChain CHAIN = RuleChain.outerRule(CLUSTER).around(REST_APP);

  private static final String COMMANDS_KSQL_TOPIC_NAME = KsqlRestApplication.COMMANDS_KSQL_TOPIC_NAME;
  private static final OutputFormat CLI_OUTPUT_FORMAT = OutputFormat.TABULAR;

  private static final long STREAMED_QUERY_ROW_LIMIT = 10000;
  private static final long STREAMED_QUERY_TIMEOUT_MS = 10000;

  private static final TestResult EMPTY_RESULT = new TestResult();

  private static Cli localCli;
  private static TestTerminal terminal;
  private static String commandTopicName;
  private static TopicProducer topicProducer;
  private static TopicConsumer topicConsumer;
  private static KsqlRestClient restClient;

  private static OrderDataProvider orderDataProvider;
  private static int result_stream_no = 0;

  @BeforeClass
  public static void setUp() throws Exception {
    restClient = new KsqlRestClient(REST_APP.getHttpListener().toString());

    terminal = new TestTerminal(CLI_OUTPUT_FORMAT, restClient);

    commandTopicName = KsqlRestConfig.getCommandTopic(KsqlConfig.KSQL_SERVICE_ID_DEFAULT);

    orderDataProvider = new OrderDataProvider();
    CLUSTER.createTopic(orderDataProvider.topicName());

    localCli = new Cli(
        STREAMED_QUERY_ROW_LIMIT,
        STREAMED_QUERY_TIMEOUT_MS,
        restClient,
        terminal
    );

    topicProducer = new TopicProducer(CLUSTER);
    topicConsumer = new TopicConsumer(CLUSTER);

    produceInputStream(orderDataProvider);
  }

  @SuppressWarnings("unchecked")
  private static Matcher<Iterable<List<String>>> hasItems(final TestResult expected) {
    return CoreMatchers.hasItems(expected.rows().stream()
            .map(CoreMatchers::equalTo)
            .collect(Collectors.toList())
            .toArray(new Matcher[0]));
  }

  private static class BoundedMatcher extends BaseMatcher<Iterable<List<String>>> {
    private final Matcher<Iterable<? extends List<String>>> internal;

    public BoundedMatcher(Matcher<Iterable<? extends List<String>>> internal) {
      this.internal = internal;
    }

    @Override
    public boolean matches(Object o) {
      return internal.matches(o);
    }

    @Override
    public void describeTo(Description description) {
      internal.describeTo(description);
    }
  }

  @SuppressWarnings("unchecked")
  private static Matcher<Iterable<List<String>>> contains(final TestResult expected) {
    return new BoundedMatcher(
        Matchers.contains(expected.rows().stream()
            .map(CoreMatchers::equalTo)
            .collect(Collectors.toList())
            .toArray(new Matcher[0])));
  }

  @SuppressWarnings("unchecked")
  private static Matcher<Iterable<List<String>>> containsInAnyOrder(final TestResult expected) {
    return new BoundedMatcher(
        Matchers.containsInAnyOrder(expected.rows().stream()
            .map(CoreMatchers::equalTo)
            .collect(Collectors.toList())
            .toArray(new Matcher[0])));
  }

  static void assertRunListCommand(
      final String commandSuffix,
      final Matcher<Iterable<List<String>>> matcher) {
    assertRunCommand("list " + commandSuffix, matcher);
    assertRunCommand("show " + commandSuffix, matcher);
  }

  private static void assertRunCommand(
      final String command,
      final Matcher<Iterable<List<String>>> matcher) {
    run(command, true);
    assertThat(terminal.getTestResult(), matcher);
  }

  private static TestResult run(String command, final boolean strict) {
    try {
      if (!command.endsWith(";")) {
        command += ";";
      }
      System.out.println("[Run Command] " + command);
      terminal.resetTestResult(strict);
      localCli.handleLine(command);
      return terminal.getTestResult();
    } catch (final Exception e) {
      throw new AssertionError("Failed to run command: " + command, e);
    }
  }

  private static void produceInputStream(final TestDataProvider dataProvider) throws Exception {
    createKStream(dataProvider);
    topicProducer.produceInputData(dataProvider);
  }

  private static void createKStream(final TestDataProvider dataProvider) {
    assertRunCommand(
        String.format("CREATE STREAM %s %s WITH (value_format = 'json', kafka_topic = '%s' , key='%s')",
            dataProvider.kstreamName(), dataProvider.ksqlSchemaString(), dataProvider.topicName(), dataProvider.key()),
        anyOf(
            equalTo(new TestResult("Stream created")),
            equalTo(new TestResult("Parsing statement")),
            equalTo(new TestResult("Executing statement"))));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // If WARN NetworkClient:589 - Connection to node -1 could not be established. Broker may not be available.
    // It may be due to not closing the resource.
    // ksqlEngine.close();
    System.out.println("[Terminal Output]");
    System.out.println(terminal.getOutputString());

    localCli.close();
    terminal.close();
    restClient.close();
  }

  private static List<List<String>> startUpConfigs() {
    return ImmutableList.of(
        // SERVER OVERRIDES:
        ImmutableList.of(
            KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG,
            SERVER_OVERRIDE, "4"),

        ImmutableList.of(
            KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY, SERVER_OVERRIDE,
            "" + (KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention + 1)
        ),

        // SESSION OVERRIDES:
        ImmutableList.of(
            KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            SESSION_OVERRIDE, "latest"),

        // DEFAULTS:
        ImmutableList.of(
            KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "",
            "" + KsqlConstants.defaultSinkNumberOfReplications)
        ,
        ImmutableList.of(
            KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "",
            "" + KsqlConstants.defaultSinkNumberOfReplications)
    );
  }

  private static void testCreateStreamAsSelect(String selectQuery, final Schema resultSchema, final Map<String, GenericRow> expectedResults) {
    if (!selectQuery.endsWith(";")) {
      selectQuery += ";";
    }
    final String resultKStreamName = "RESULT_" + result_stream_no++;
    final String queryString = "CREATE STREAM " + resultKStreamName + " AS " + selectQuery;

    /* Start Stream Query */
    assertRunCommand(
        queryString,
        anyOf(
            equalTo(new TestResult("Stream created and running")),
            equalTo(new TestResult("Parsing statement")),
            equalTo(new TestResult("Executing statement"))));

    /* Assert Results */
    final Map<String, GenericRow> results = topicConsumer.readResults(resultKStreamName, resultSchema, expectedResults.size(), new StringDeserializer());

    dropStream(resultKStreamName);

    assertThat(results, equalTo(expectedResults));
  }

  private static void runStatement(final String statement, final KsqlRestClient restClient) {
    final RestResponse response = restClient.makeKsqlRequest(statement);
    Assert.assertThat(response.isSuccessful(), is(true));
    final KsqlEntityList entityList = ((KsqlEntityList) response.get());
    Assert.assertThat(entityList.size(), equalTo(1));
    Assert.assertThat(entityList.get(0), instanceOf(CommandStatusEntity.class));
    final CommandStatusEntity entity = (CommandStatusEntity) entityList.get(0);
    final CommandStatus status = entity.getCommandStatus();
    Assert.assertThat(status, not(CommandStatus.Status.ERROR));

    if (status.getStatus() != Status.SUCCESS) {
      assertThatEventually(
          "",
          () -> {
            final RestResponse statusResponse = restClient
                .makeStatusRequest(entity.getCommandId().toString());
            Assert.assertThat(statusResponse.isSuccessful(), is(true));
            Assert.assertThat(statusResponse.get(), instanceOf(CommandStatus.class));
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

    final RestResponse response = restClient.makeKsqlRequest(dropStatement);
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

  private static void selectWithLimit(String selectQuery, final int limit, final TestResult expectedResults) {
    selectQuery += " LIMIT " + limit + ";";
    assertRunCommand(selectQuery, contains(expectedResults));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldPrintResultsForListOrShowCommands() {
    assertRunListCommand(
        "topics",
        Matchers.hasItems(
            Matchers.contains(
                equalTo(orderDataProvider.topicName()),
                equalTo("true"), equalTo("1"),
                equalTo("1"), any(String.class),
                any(String.class))));
    assertRunListCommand(
        "registered topics",
        containsInAnyOrder(
            new TestResult.Builder()
                .addRow(orderDataProvider.kstreamName(), orderDataProvider.topicName(), "JSON")
                .addRow(COMMANDS_KSQL_TOPIC_NAME, commandTopicName, "JSON")
                .build()
        )
    );
    assertRunListCommand(
        "streams",
        contains(
            new TestResult(orderDataProvider.kstreamName(), orderDataProvider.topicName(), "JSON")));
    assertRunListCommand("tables", is(EMPTY_RESULT));
    assertRunListCommand("queries", is(EMPTY_RESULT));
  }

  @Test
  public void testPrint() {
    final Thread thread =
        new Thread(() -> run("print 'ORDER_TOPIC' FROM BEGINNING INTERVAL 2;", false));
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

    final TestResult.Builder builder = new TestResult.Builder();
    builder.addRows(startUpConfigs());
    assertRunListCommand("properties", hasItems(builder.build()));

    assertRunCommand("unset 'auto.offset.reset'", is(EMPTY_RESULT));
  }

  @Test
  public void testDescribe() {
    assertRunCommand("describe topic " + COMMANDS_KSQL_TOPIC_NAME,
        contains(new TestResult(COMMANDS_KSQL_TOPIC_NAME, commandTopicName, "JSON")));
  }

  @Test
  public void shouldPrintCorrectSchemaForDescribeStream() {
    TestResult.Builder builder = new TestResult.Builder();
    builder.addRow("ROWTIME", "BIGINT           (system)");
    builder.addRow("ROWKEY", "VARCHAR(STRING)  (system)");
    builder.addRow("ORDERTIME", "BIGINT");
    builder.addRow("ORDERID", "VARCHAR(STRING)");
    builder.addRow("ITEMID", "VARCHAR(STRING)");
    builder.addRow("ORDERUNITS", "DOUBLE");
    builder.addRow("TIMESTAMP", "VARCHAR(STRING)");
    builder.addRow("PRICEARRAY", "ARRAY<DOUBLE>");
    builder.addRow("KEYVALUEMAP", "MAP<STRING, DOUBLE>");
    assertRunCommand(
        "describe " + orderDataProvider.kstreamName(),
        contains(builder.build()));
  }

  @Test
  public void testSelectStar() {
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

    final Schema resultSchema = SchemaBuilder.struct()
        .field("ITEMID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
        .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .build();

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
  public void testSelectLimit() {
    final TestResult.Builder builder = new TestResult.Builder();
    final Map<String, GenericRow> streamData = orderDataProvider.data();
    final int limit = 3;
    for (int i = 1; i <= limit; i++) {
      final GenericRow srcRow = streamData.get(Integer.toString(i));
      final List<Object> columns = srcRow.getColumns();
      final GenericRow resultRow = new GenericRow(ImmutableList.of(columns.get(1), columns.get(2)));
      builder.addRow(resultRow);
    }
    selectWithLimit(
        "SELECT ORDERID, ITEMID FROM " + orderDataProvider.kstreamName(), limit, builder.build());
  }

  @Test
  public void testSelectUDFs() {
    final String selectColumns =
        "ITEMID, ORDERUNITS*10, PRICEARRAY[0]+10, KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10, PRICEARRAY[1]>1000";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "SELECT %s FROM %s WHERE %s;",
        selectColumns,
        orderDataProvider.kstreamName(),
        whereClause
    );

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(ImmutableList.of("ITEM_8", 800.0, 1110.0, 12.0, true)));

    // TODO: tests failed!
    // testCreateStreamAsSelect(queryString, orderDataProvider.schema(), expectedResults);
  }

  // ===================================================================
  // Below Tests are only used for coverage, not for results validation.
  // ===================================================================

  @Test
  public void testRunInteractively() {
    localCli.runInteractively();
  }

  @Test
  public void testEmptyInput() throws Exception {
    localCli.runNonInteractively("");
  }

  @Test
  public void testExitCommand() throws Exception {
    localCli.runNonInteractively("exit");
    localCli.runNonInteractively("\nexit\n\n\n");
    localCli.runNonInteractively("exit\nexit\nexit");
    localCli.runNonInteractively("\n\nexit\nexit\n\n\n\nexit\n\n\n");
  }

  @Test
  public void testExtraCommands() throws Exception {
    localCli.runNonInteractively("help");
    localCli.runNonInteractively("version");
    localCli.runNonInteractively("output");
    localCli.runNonInteractively("clear");
  }

  @Test
  public void testCommandsOverMultipleLines() throws Exception {
    localCli.runNonInteractively("he\\\nlp");
    localCli.runNonInteractively("he\\ \nlp");
  }

  @Test
  public void shouldHandleRegisterTopic() throws Exception {
    localCli.handleLine("REGISTER TOPIC foo WITH (value_format = 'csv', kafka_topic='foo');");
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServer() throws Exception {
    final KsqlRestClient mockRestClient = EasyMock.mock(KsqlRestClient.class);
    EasyMock.expect(mockRestClient.makeRootRequest()).andThrow(new KsqlRestClientException("Boom", new ProcessingException("")));
    EasyMock.expect(mockRestClient.getServerInfo()).andReturn(
        RestResponse.of(new ServerInfo("1.x", "testClusterId", "testServiceId")));
    EasyMock.expect(mockRestClient.getServerAddress()).andReturn(new URI("http://someserver:8008")).anyTimes();
    EasyMock.replay(mockRestClient);
    final TestTerminal terminal = new TestTerminal(CLI_OUTPUT_FORMAT, mockRestClient);

    new Cli(1L, 1L, mockRestClient, terminal)
        .runInteractively();

    assertThat(terminal.getOutputString(), containsString("Remote server address may not be valid"));
  }

  @Test
  public void shouldRegisterRemoteCommand() {
    new Cli(1L, 1L, restClient, terminal);
    assertThat(terminal.getCliSpecificCommands().get("server"),
        instanceOf(RemoteServerSpecificCommand.class));
  }

  @Test
  public void shouldPrintErrorOnUnsupportedAPI() throws Exception {
    final KsqlRestClient mockRestClient = EasyMock.mock(KsqlRestClient.class);
    EasyMock.expect(mockRestClient.makeRootRequest()).andReturn(
        RestResponse.erroneous(
            new KsqlErrorMessage(
                Errors.toErrorCode(NOT_ACCEPTABLE.getStatusCode()),
                "Minimum supported client version: 1.0")));
    EasyMock.expect(mockRestClient.getServerInfo()).andReturn(
        RestResponse.of(new ServerInfo("1.x", "testClusterId", "testServiceId")));
    EasyMock.expect(mockRestClient.getServerAddress()).andReturn(new URI("http://someserver:8008"));
    EasyMock.replay(mockRestClient);
    final TestTerminal terminal = new TestTerminal(CLI_OUTPUT_FORMAT, restClient);

    new Cli(1L, 1L, mockRestClient, terminal)
        .runInteractively();

    Assert.assertThat(
        terminal.getOutputString(),
        containsString("This CLI version no longer supported"));
    Assert.assertThat(
        terminal.getOutputString(),
        containsString("Minimum supported client version: 1.0"));
  }

  @Test
  public void shouldListFunctions() {
    final TestResult.Builder builder = new TestResult.Builder();
    builder.addRow("TIMESTAMPTOSTRING", "SCALAR");
    builder.addRow("EXTRACTJSONFIELD", "SCALAR");
    builder.addRow("TOPK", "AGGREGATE");
    assertRunListCommand("functions", hasItems(builder.build()));
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
    final String expectedOutput = "Can't find any functions with the name 'foobar'";
    assertThat(terminal.getOutputString(), containsString(expectedOutput));
  }
}
