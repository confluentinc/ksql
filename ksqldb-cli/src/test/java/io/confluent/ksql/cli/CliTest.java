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

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.TestTerminal;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.Console.RowCaptor;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.cli.console.cmd.RemoteServerSpecificCommand;
import io.confluent.ksql.cli.console.cmd.RequestPipeliningCommand;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlMissingCredentialsException;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.client.exception.KsqlUnsupportedServerException;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow.DataRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
@SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
@RunWith(MockitoJUnitRunner.class)
@Category(IntegrationTest.class)
public class CliTest {

  private static final OrderDataProvider ORDER_DATA_PROVIDER = new OrderDataProvider();

  private static final String SERVER_OVERRIDE = "SERVER";
  private static final String SESSION_OVERRIDE = "SESSION";

  private static final String JSON_TOPIC = ORDER_DATA_PROVIDER.topicName();
  private static final String DELIMITED_TOPIC = "Delimited_Topic";

  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query with ID (\\S+)");

  private static final Pattern WRITE_QUERIES = Pattern
      .compile(".*The following queries write into this source: \\[(.+)].*", Pattern.DOTALL);

  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
          KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention + 1)
      .withProperty(KsqlRestConfig.DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG, 30000L)
      .build();

  @Rule
  public final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(1, TimeUnit.MINUTES)
      .withLookingForStuckThread(true)
      .build();

  private static final OutputFormat CLI_OUTPUT_FORMAT = OutputFormat.TABULAR;

  private static final long STREAMED_QUERY_ROW_LIMIT = 10000;
  private static final long STREAMED_QUERY_TIMEOUT_MS = 10000;

  private static final List<List<String>> EMPTY_RESULT = ImmutableList.of();

  private static KsqlRestClient restClient;

  private Console console;
  private TestTerminal terminal;
  private TestRowCaptor rowCaptor;
  @Mock
  private Supplier<String> lineSupplier;
  private Cli localCli;
  private String streamName;
  private String tableName;

  @BeforeClass
  public static void classSetUp() {
    restClient = KsqlRestClient.create(
        REST_APP.getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty(),
        Optional.empty()
    );

    TEST_HARNESS.getKafkaCluster().createTopics(
        JSON_TOPIC,
        DELIMITED_TOPIC
    );

    TEST_HARNESS.produceRows(JSON_TOPIC, ORDER_DATA_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    TEST_HARNESS.produceRecord(DELIMITED_TOPIC, null, null);
    TEST_HARNESS.produceRows(DELIMITED_TOPIC, new ItemDataProvider(), FormatFactory.KAFKA, FormatFactory.DELIMITED);

    try (Cli cli = Cli.build(1L, 1000L, OutputFormat.JSON, restClient)) {
      createKStream(ORDER_DATA_PROVIDER, cli);
    }
  }

  @Before
  public void setUp() {
    streamName = KsqlIdentifierTestUtil.uniqueIdentifierName();
    tableName = KsqlIdentifierTestUtil.uniqueIdentifierName();
    terminal = new TestTerminal(lineSupplier);
    rowCaptor = new TestRowCaptor();
    console = new Console(CLI_OUTPUT_FORMAT, terminal, rowCaptor);

    localCli = new Cli(
        STREAMED_QUERY_ROW_LIMIT,
        STREAMED_QUERY_TIMEOUT_MS,
        restClient,
        console
    );
  }

  private void assertRunListCommand(
      final String commandSuffix,
      final Matcher<Iterable<? extends Iterable<? extends String>>> matcher
  ) {
    assertRunCommand("list " + commandSuffix + ";", matcher);
    assertRunCommand("show " + commandSuffix + ";", matcher);
  }

  private void assertRunCommand(
      final String command,
      final Matcher<Iterable<? extends Iterable<? extends String>>> matcher
  ) {
    rowCaptor.resetTestResult();
    run(command, localCli);
    assertThat(rowCaptor.getRows(), matcher);
  }

  private static void run(final String command, final Cli localCli) {
    try {
      localCli.handleLine(command);
    } catch (final Exception e) {
      throw new AssertionError("Failed to run command: " + command, e);
    }
  }

  private static void createKStream(final TestDataProvider dataProvider, final Cli cli) {
    run("CREATE STREAM " + dataProvider.sourceName()
            + " (" + dataProvider.ksqlSchemaString(false) + ")"
            + " WITH (value_format = 'json', kafka_topic = '" + dataProvider.topicName() + "');",
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
      final String selectQuery,
      final PhysicalSchema resultSchema,
      final Map<GenericKey, GenericRow> expectedResults
  ) {
    final String queryString = "CREATE STREAM " + streamName + " AS " + selectQuery;

    assertRunCommand(
        queryString,
        anyOf(
            isRow(containsString("Created query with ID CSAS_" + streamName)),
            isRow(is("Parsing statement")),
            isRow(is("Executing statement"))));

    try {
      TEST_HARNESS.verifyAvailableUniqueRows(
          streamName,
          is(expectedResults),
          FormatFactory.KAFKA,
          FormatFactory.JSON,
          resultSchema
      );
    } finally {
      dropStream(streamName);
    }
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
    dropStreamOrTable(true, name);
  }

  private static void dropTable(final String name) {
    dropStreamOrTable(false, name);
  }

  private static void dropStreamOrTable(final boolean stream, final String name) {

    final String streamOrTable = stream ? "stream" : "table";

    final String dropStatement = String.format("drop " + streamOrTable + " %s;", name);

    final RestResponse<?> response = restClient.makeKsqlRequest(dropStatement, null);
    if (response.isSuccessful()) {
      return;
    }

    final java.util.regex.Matcher matcher = WRITE_QUERIES
        .matcher(response.getErrorMessage().getMessage());

    if (!matcher.matches()) {
      throw new RuntimeException("Failed to drop " + streamOrTable + ": " + response.getErrorMessage());
    }

    Arrays.stream(matcher.group(1).split("/w*,/w*"))
        .forEach(CliTest::terminateQuery);

    runStatement(dropStatement, restClient);
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
            equalTo(ORDER_DATA_PROVIDER.topicName()),
            equalTo("1"),
            equalTo("1")
        )
    );

    assertRunListCommand(
        "topics extended",
        hasRow(
            equalTo(ORDER_DATA_PROVIDER.topicName()),
            equalTo("1"),
            equalTo("1"),
            any(String.class),
            any(String.class)
        )
    );

    assertRunListCommand(
        "streams",
        hasRow(
            equalTo(ORDER_DATA_PROVIDER.sourceName()),
            equalTo(ORDER_DATA_PROVIDER.topicName()),
            equalTo("KAFKA"),
            equalTo("JSON"),
            equalTo("false")
        )
    );
  }

  @Test
  public void shouldPrintTopicWithJsonValue() {
    // When:
    run("print " + JSON_TOPIC + " FROM BEGINNING INTERVAL 1 LIMIT 1;", localCli);

    // Then:
    assertThatEventually(() -> terminal.getOutputString(), containsString("Value format: JSON"));
    assertThatEventually(() -> terminal.getOutputString(),
        containsString("Key format: KAFKA_STRING"));
    assertThatEventually(() -> terminal.getOutputString(), containsString(","
        + " key: ORDER_1, "
        + "value: {"
        + "\"ORDERTIME\":1,"
        + "\"ITEMID\":\"ITEM_1\","
        + "\"ORDERUNITS\":10.0,"
        + "\"TIMESTAMP\":\"2018-01-01\","
        + "\"PRICEARRAY\":[100.0,110.99,90.0],"
        + "\"KEYVALUEMAP\":{\"key1\":1.0,\"key2\":2.0,\"key3\":3.0}"
        + "}"));
  }

  @Test
  public void shouldPrintTopicWithDelimitedValue() {
    // When:
    run("print " + DELIMITED_TOPIC + " FROM BEGINNING INTERVAL 1 LIMIT 2;", localCli);

    // Then:
    assertThatEventually(() -> terminal.getOutputString(),
        containsString("Value format: KAFKA_STRING"));
    assertThat(terminal.getOutputString(), containsString("Key format: KAFKA_STRING"));
    assertThat(terminal.getOutputString(), containsString(", key: <null>, value: <null>"));
    assertThat(terminal.getOutputString(),
        containsString(", key: ITEM_1, value: home cinema"));
  }

  @Test
  public void testDisableVariableSubstitution() {
    // Given:
    assertRunCommand(
        "set '" + KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE + "' = 'false';", is(EMPTY_RESULT));
    assertRunCommand("define topicName = '" + DELIMITED_TOPIC + "';", is(EMPTY_RESULT));

    // When:
    run("PRINT ${topicName} FROM BEGINNING INTERVAL 1 LIMIT 2;", localCli);

    // Then:
    assertThatEventually(() -> terminal.getOutputString(),
        containsString("Failed to Describe Kafka Topic(s): [${topicName}]"));
    assertThatEventually(() -> terminal.getOutputString(),
        containsString("Caused by: The request attempted to perform an operation on an invalid topic."));
  }

  @Test
  public void testVariableSubstitution() {
    // Given:
    assertRunCommand(
        "set '" + KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE + "' = 'true';", is(EMPTY_RESULT));
    assertRunCommand("define topicName = '" + DELIMITED_TOPIC + "';", is(EMPTY_RESULT));

    // When:
    run("PRINT ${topicName} FROM BEGINNING INTERVAL 1 LIMIT 2;", localCli);

    // Then:
    assertThatEventually(() -> terminal.getOutputString(),
        containsString("Value format: KAFKA_STRING"));
    assertThat(terminal.getOutputString(), containsString("Key format: KAFKA_STRING"));
    assertThat(terminal.getOutputString(), containsString(", key: <null>, value: <null>"));
    assertThat(terminal.getOutputString(),
        containsString(", key: ITEM_1, value: home cinema"));
  }

  @Test
  public void testVariableDefineUndefine() {
    assertRunCommand("define var1 = '1';", is(EMPTY_RESULT));
    assertRunCommand("define var2 = '2';", is(EMPTY_RESULT));
    assertRunCommand("define var3 = '3';", is(EMPTY_RESULT));

    assertRunCommand("undefine var1;", is(EMPTY_RESULT));

    assertRunListCommand("variables", hasRows(
        row(
            "var2",
            "2"
        ),
        row(
            "var3",
            "3"
        )
    ));
  }

  @Test
  public void testAddVariablesToCli() {
    // Given
    localCli.addSessionVariables(ImmutableMap.of("env", "qa"));

    // Then
    assertRunListCommand("variables", hasRows(
        row(
            "env",
            "qa"
        )
    ));
  }

  @Test
  public void testPropertySetUnset() {
    assertRunCommand("set 'auto.offset.reset' = 'latest';", is(EMPTY_RESULT));
    assertRunCommand("set 'application.id' = 'Test_App';", is(EMPTY_RESULT));
    assertRunCommand("set 'producer.batch.size' = '16384';", is(EMPTY_RESULT));
    assertRunCommand("set 'max.request.size' = '1048576';", is(EMPTY_RESULT));
    assertRunCommand("set 'consumer.max.poll.records' = '500';", is(EMPTY_RESULT));
    assertRunCommand("set 'enable.auto.commit' = 'true';", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.application.id' = 'Test_App';", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.producer.batch.size' = '16384';", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.max.request.size' = '1048576';", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.consumer.max.poll.records' = '500';", is(EMPTY_RESULT));
    assertRunCommand("set 'ksql.streams.enable.auto.commit' = 'true';", is(EMPTY_RESULT));

    assertRunCommand("unset 'application.id';", is(EMPTY_RESULT));
    assertRunCommand("unset 'producer.batch.size';", is(EMPTY_RESULT));
    assertRunCommand("unset 'max.request.size';", is(EMPTY_RESULT));
    assertRunCommand("unset 'consumer.max.poll.records';", is(EMPTY_RESULT));
    assertRunCommand("unset 'enable.auto.commit';", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.application.id';", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.producer.batch.size';", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.max.request.size';", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.consumer.max.poll.records';", is(EMPTY_RESULT));
    assertRunCommand("unset 'ksql.streams.enable.auto.commit';", is(EMPTY_RESULT));

    assertRunListCommand("properties", hasRows(
        // SERVER OVERRIDES:
        row(
            KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG,
            "KSQL",
            SERVER_OVERRIDE,
            "4"
        ),
        row(
            KsqlConfig.SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY,
            "KSQL",
            SERVER_OVERRIDE,
            "" + (KsqlConstants.defaultSinkWindowChangeLogAdditionalRetention + 1)
        ),
        // SESSION OVERRIDES:
        row(
            KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "KSQL",
            SESSION_OVERRIDE,
            "latest"
        )
    ));

    assertRunCommand("unset 'auto.offset.reset';", is(EMPTY_RESULT));
  }

  @Test
  public void shouldPrintCorrectSchemaForDescribeStream() {
    assertRunCommand(
        "describe " + ORDER_DATA_PROVIDER.sourceName() + ";",
        containsRows(
            row("ORDERID", "VARCHAR(STRING)  (key)"),
            row("ORDERTIME", "BIGINT"),
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
        "SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + ";",
        ORDER_DATA_PROVIDER.schema(),
        ORDER_DATA_PROVIDER.finalData()
    );
  }

  @Test
  public void testSelectProject() {
    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap
        .<GenericKey, GenericRow>builder()
        .put(genericKey("ORDER_1"), genericRow(
            "ITEM_1",
            10.0,
            ImmutableList.of(100.0, 110.99, 90.0)))
        .put(genericKey("ORDER_2"), genericRow(
            "ITEM_2",
            20.0,
            ImmutableList.of(10.0, 10.99, 9.0)))
        .put(genericKey("ORDER_3"), genericRow(
            "ITEM_3",
            30.0,
            ImmutableList.of(10.0, 10.99, 91.0)))
        .put(genericKey("ORDER_4"), genericRow(
            "ITEM_4",
            40.0,
            ImmutableList.of(10.0, 140.99, 94.0)))
        .put(genericKey("ORDER_5"), genericRow(
            "ITEM_5",
            50.0,
            ImmutableList.of(160.0, 160.99, 98.0)))
        .put(genericKey("ORDER_6"), genericRow(
            "ITEM_8",
            80.0,
            ImmutableList.of(1100.0, 1110.99, 970.0)))
        .build();

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        LogicalSchema.builder()
            .keyColumn(ColumnName.of("ORDERID"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.DOUBLE)
            .valueColumn(ColumnName.of("PRICEARRAY"), SqlTypes.array(SqlTypes.DOUBLE))
            .build(),
        SerdeFeatures.of(),
        SerdeFeatures.of()
    );

    testCreateStreamAsSelect(
        "SELECT ORDERID, ITEMID, ORDERUNITS, PRICEARRAY "
            + "FROM " + ORDER_DATA_PROVIDER.sourceName() + ";",
        resultSchema,
        expectedResults
    );
  }

  @Test
  public void testSelectFilter() {
    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap.of(
        genericKey("ORDER_6"),
        genericRow(
            8L,
            "ITEM_8",
            80.0,
            "2018-01-08",
            ImmutableList.of(1100.0, 1110.99, 970.0),
            ImmutableMap.of("key1", 1.0, "key2", 2.0, "key3", 3.0)
        )
    );

    testCreateStreamAsSelect(
        "SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName()
            + " WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';",
        ORDER_DATA_PROVIDER.schema(),
        expectedResults
    );
  }

  @Test
  public void testTransientSelect() {
    final Multimap<GenericKey, GenericRow> streamData = ORDER_DATA_PROVIDER.data();
    final List<Object> row1 = Iterables.getFirst(streamData.get(genericKey("ORDER_1")), genericRow()).values();
    final List<Object> row2 = Iterables.getFirst(streamData.get(genericKey("ORDER_2")), genericRow()).values();
    final List<Object> row3 = Iterables.getFirst(streamData.get(genericKey("ORDER_3")), genericRow()).values();

    selectWithLimit(
        "SELECT ORDERID, ITEMID FROM " + ORDER_DATA_PROVIDER.sourceName() + " EMIT CHANGES",
        3,
        containsRows(
            row("ORDER_1", row1.get(1).toString()),
            row("ORDER_2", row2.get(1).toString()),
            row("ORDER_3", row3.get(1).toString())
        ));
  }

  @Test
  public void shouldHandlePullQuery() {
    // Given:
    run("CREATE TABLE " + tableName + " AS SELECT ITEMID, COUNT(1) AS COUNT "
            + "FROM " + ORDER_DATA_PROVIDER.sourceName()
            + " GROUP BY ITEMID;",
        localCli
    );

    // When:
    final Supplier<String> runner = () -> {
      // It's possible that the state store is not warm on the first invocation, hence the retry
      run("SELECT ITEMID, COUNT FROM " + tableName + " WHERE ITEMID='ITEM_1';", localCli);
      return terminal.getOutputString();
    };

    // Wait for warm store:
    assertThatEventually(runner, containsString("|ITEM_1"));
    assertRunCommand(
        "SELECT ITEMID, COUNT FROM " + tableName + " WHERE ITEMID='ITEM_1';",
        containsRows(
            row(equalTo("ITEM_1"), any(String.class))
        )
    );
  }

  @Test
  public void shouldOutputPullQueryHeader() {
    // Given:
    run("CREATE TABLE Y AS SELECT ITEMID, COUNT(1) AS COUNT "
            + "FROM " + ORDER_DATA_PROVIDER.sourceName()
            + " GROUP BY ITEMID;",
        localCli
    );

    // When:
    final Supplier<String> runner = () -> {
      // It's possible that the state store is not warm on the first invocation, hence the retry
      run("SELECT * FROM Y WHERE ITEMID='ITEM_1';", localCli);
      return terminal.getOutputString();
    };

    assertThatEventually(runner, containsString("ITEMID"));
    assertThatEventually(runner, containsString("COUNT"));
  }

  @Test
  public void testTransientContinuousSelectStar() {
    final Multimap<GenericKey, GenericRow> streamData = ORDER_DATA_PROVIDER.data();
    final List<Object> row1 = Iterables.getFirst(streamData.get(genericKey("ORDER_1")), genericRow()).values();
    final List<Object> row2 = Iterables.getFirst(streamData.get(genericKey("ORDER_2")), genericRow()).values();
    final List<Object> row3 = Iterables.getFirst(streamData.get(genericKey("ORDER_3")), genericRow()).values();

    selectWithLimit(
        "SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + " EMIT CHANGES",
        3,
        containsRows(
            row(prependWithKey("ORDER_1", row1)),
            row(prependWithKey("ORDER_2", row2)),
            row(prependWithKey("ORDER_3", row3))
        ));
  }

  @Test
  public void shouldOutputPushQueryHeader() {
    // When:
    run("SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + " EMIT CHANGES LIMIT 1;", localCli);

    // Then: (note that some of these are truncated because of header wrapping)
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
        "SELECT "
            + "ORDERID, "
            + "ITEMID, "
            + "ORDERUNITS*10 AS Col1, "
            + "PRICEARRAY[1]+10 AS Col2, "
            + "KEYVALUEMAP['key1']*KEYVALUEMAP['key2']+10 AS Col3, "
            + "PRICEARRAY[2]>1000 AS Col4 "
            + "FROM %s "
            + "WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        ORDER_DATA_PROVIDER.sourceName()
    );

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        LogicalSchema.builder()
            .keyColumn(ColumnName.of("ORDERID"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("COL1"), SqlTypes.DOUBLE)
            .valueColumn(ColumnName.of("COL2"), SqlTypes.DOUBLE)
            .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
            .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
            .build(),
        SerdeFeatures.of(),
        SerdeFeatures.of()
    );

    final Map<GenericKey, GenericRow> expectedResults = ImmutableMap.of(
        genericKey("ORDER_6"),
        genericRow("ITEM_8", 800.0, 1110.0, 12.0, true)
    );

    testCreateStreamAsSelect(queryString, resultSchema, expectedResults);
  }

  @Test
  public void testCreateTable() {
    final String queryString = "CREATE TABLE " + tableName + " AS " +
        " SELECT ITEMID, COUNT(*) FROM " + ORDER_DATA_PROVIDER.sourceName() +
        " GROUP BY ITEMID;";

    assertRunCommand(
        queryString,
        anyOf(
            isRow(containsString("Created query with ID CTAS_" + tableName.toUpperCase())),
            isRow(is("Parsing statement")),
            isRow(is("Executing statement"))));
    assertRunListCommand("tables",
        hasRow(
            equalTo(tableName),
            equalTo(tableName.toUpperCase(Locale.ROOT)),
            equalTo("KAFKA"),
            equalTo("JSON"),
            equalTo("false")
    ));

    assertRunListCommand("queries",
        hasRow(
            containsString("CTAS_" + tableName),
            equalTo("PERSISTENT"),
            equalTo("RUNNING:1"),
            equalTo(tableName),
            equalTo(tableName),
            containsString("CREATE TABLE " + tableName)
        )
    );

    dropTable(tableName);
  }

  @Test
  public void testRunInteractively() {
    // Given:
    givenRunInteractivelyWillExit();

    // When:
    localCli.runInteractively();
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServerOnRunInteractively() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getServerInfo())
        .thenThrow(new KsqlRestClientException("Boom", new IOException("")));

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(terminal.getOutputString(),
               containsString("Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServerOnRunCommand() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getServerInfo())
        .thenThrow(new KsqlRestClientException("Boom", new IOException("")));

    final int error_code = new Cli(1L, 1L, mockRestClient, console)
        .runCommand("this is a command");

    assertThat(error_code, is(-1));
    assertThat(terminal.getOutputString(),
        containsString("Please ensure that the URL provided is for an active KSQL server."));
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServerOnRunScript() throws Exception {
    // Given
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getServerInfo())
        .thenThrow(new KsqlRestClientException("Boom", new IOException("")));

    new Cli(1L, 1L, mockRestClient, console)
        .runScript("script_file_ignored");

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
    when(mockRestClient.getServerInfo()).thenReturn(
        RestResponse.erroneous(
            NOT_ACCEPTABLE.code(),
            new KsqlErrorMessage(
                Errors.toErrorCode(NOT_ACCEPTABLE.code()),
                "Minimum supported client version: 1.0")
        ));

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
  public void shouldFailOnUnsupportedCpServerVersion() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient("5.5.0-0");

    assertThrows(
        KsqlUnsupportedServerException.class,
        () -> new Cli(1L, 1L, mockRestClient, console)
            .runInteractively()
    );
  }

  @Test
  public void shouldFailOnUnsupportedStandaloneServerVersion() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient("0.9.0-0");

    assertThrows(
        KsqlUnsupportedServerException.class,
        () -> new Cli(1L, 1L, mockRestClient, console)
            .runInteractively()
    );
  }

  @Test
  public void shouldPrintWarningOnDifferentCpServerVersion() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient("6.0.0-0");

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(
        terminal.getOutputString(),
        containsString("WARNING: CLI and server version don't match."
            + " This may lead to unexpected errors.")
    );
  }

  @Test
  public void shouldPrintWarningOnDifferentStandaloneServerVersion() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient("0.10.0-0");

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(
        terminal.getOutputString(),
        containsString("WARNING: CLI and server version don't match."
            + " This may lead to unexpected errors.")
    );
  }

  @Test
  public void shouldPrintWarningOnUnknownServerVersion() throws Exception {
    givenRunInteractivelyWillExit();

    final KsqlRestClient mockRestClient = givenMockRestClient("bad-version");

    new Cli(1L, 1L, mockRestClient, console)
        .runInteractively();

    assertThat(
        terminal.getOutputString(),
        containsString("WARNING: Could not identify server version.")
    );
  }

  @Test
  public void shouldListFunctions() {
    assertRunListCommand("functions", hasRows(
        row("TIMESTAMPTOSTRING", "DATE / TIME"),
        row("EXTRACTJSONFIELD", "JSON"),
        row("TOPK", "AGGREGATE")
    ));
  }

  @Test
  public void shouldDescribeScalarFunction() {
    final String expectedOutput =
        "Name        : TIMESTAMPTOSTRING\n"
            + "Author      : Confluent\n"
            + "Overview    : Converts a number of milliseconds since 1970-01-01 00:00:00 UTC/GMT "
            + "into the string \n"
            + "              representation of the timestamp in the given format. The system "
            + "default time zone is \n"
            + "              used when no time zone is explicitly provided.\n"
            + "Type        : SCALAR\n"
            + "Jar         : internal\n"
            + "Variations  :";

    localCli.handleLine("describe function timestamptostring;");
    final String outputString = terminal.getOutputString();
    assertThat(outputString, containsString(expectedOutput));

    // variations for Udfs are loaded non-deterministically. Don't assume which variation is first
    final String expectedVariation =
        "\tVariation   : TIMESTAMPTOSTRING(epochMilli BIGINT, formatPattern VARCHAR)\n" +
                "\tReturns     : VARCHAR\n" +
                "\tDescription : Converts a number of milliseconds since 1970-01-01 00:00:00 "
              + "UTC/GMT into the string \n" +
                "                representation of the timestamp in the given format. Single "
              + "quotes in the timestamp \n" +
                "                format can be escaped with '', for example: "
              + "'yyyy-MM-dd''T''HH:mm:ssX'. The system \n" +
                "                default time zone is used when no time zone is explicitly "
              + "provided. The format \n" +
                "                pattern should be in the format expected by "
              + "java.time.format.DateTimeFormatter\n" +
                "\tepochMilli  : Milliseconds since January 1, 1970, 00:00:00 UTC/GMT.\n" +
                "\tformatPattern: The format pattern should be in the format expected by \n" +
                "                 java.time.format.DateTimeFormatter.";

    assertThat(outputString, containsString(expectedVariation));
  }

  @Test
  public void shouldDescribeOverloadedScalarFunction() {
    // Given:
    localCli.handleLine("describe function substring;");

    // Then:
    final String output = terminal.getOutputString();

    // Summary output:
    assertThat(output, containsString(
        "Name        : SUBSTRING\n"
        + "Author      : Confluent\n"
        + "Overview    : Returns the portion of the string or bytes passed in value.\n"
    ));
    assertThat(output, containsString(
        "Type        : SCALAR\n"
        + "Jar         : internal\n"
        + "Variations  :"
    ));

    // Variant output:
    assertThat(output, containsString(
        "\tVariation   : SUBSTRING(str VARCHAR, pos INT)\n"
        + "\tReturns     : VARCHAR\n"
        + "\tDescription : Returns the portion of str from pos to the end of str"
    ));
    assertThat(output, containsString(
        "\tstr         : The source string.\n"
        + "\tpos         : The base-one position to start from."
    ));
  }

  @Test
  public void shouldDescribeAggregateFunction() {
    final String expectedSummary =
            "Name        : TOPK\n"
            + "Author      : Confluent\n"
            + "Overview    : Computes the top k values for a column, per key.\n"
            + "Type        : AGGREGATE\n"
            + "Jar         : internal\n"
            + "Variations  : \n";

    final String expectedVariant =
        "\tVariation   : TOPK(val1 INT, k INT)\n"
        + "\tReturns     : ARRAY<INT>\n"
        + "\tDescription : Calculates the top k values for an integer column, per key.";

    localCli.handleLine("describe function topk;");

    final String output = terminal.getOutputString();
    assertThat(output, containsString(expectedSummary));
    assertThat(output, containsString(expectedVariant));
  }

  @Test
  public void shouldDescribeTableFunction() {
    final String expectedOutput =
        "Name        : EXPLODE\n"
            + "Author      : Confluent\n"
            + "Overview    : Explodes an array. This function outputs one value for each element of the array.\n"
            + "Type        : TABLE\n"
            + "Jar         : internal\n"
            + "Variations  : ";

    localCli.handleLine("describe function explode;");
    final String outputString = terminal.getOutputString();
    assertThat(outputString, containsString(expectedOutput));

    // variations for Udfs are loaded non-deterministically. Don't assume which variation is first
    String expectedVariation =
        "\tVariation   : EXPLODE(list ARRAY<T>)\n"
            + "\tReturns     : T";
    assertThat(outputString, containsString(expectedVariation));

    expectedVariation = "\tVariation   : EXPLODE(input ARRAY<DECIMAL>)\n"
        + "\tReturns     : DECIMAL";
    assertThat(outputString, containsString(expectedVariation));
  }

  @Test
  public void shouldExplainQueryId() {
    // Given:
    localCli.handleLine("CREATE STREAM " + streamName + " "
        + "AS SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + ";");

    final String queryId = extractQueryId(terminal.getOutputString());

    final String explain = "EXPLAIN " + queryId + ";";

    // When:
    localCli.handleLine(explain);

    // Then:
    assertThat(terminal.getOutputString(), containsString(queryId));
    assertThat(terminal.getOutputString(), containsString("Status"));
    assertThat(terminal.getOutputString(),
        either(containsString("REBALANCING"))
            .or(containsString("RUNNING")));

    dropStream(streamName);
  }

  @Test
  public void shouldPrintErrorIfCantFindFunction() {
    localCli.handleLine("describe function foobar;");

    assertThat(terminal.getOutputString(),
        containsString("Can't find any functions with the name 'foobar'"));
  }

  @Test
  public void shouldHandleSetPropertyAsPartOfMultiStatementLine() {
    // When:
    localCli.handleLine("set 'auto.offset.reset'='earliest';");

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Successfully changed local property 'auto.offset.reset' to 'earliest'"));
  }

  @Test
  public void shouldSubstituteVariablesOnRunCommand()  {
    // Given:
    final StringBuilder builder = new StringBuilder();
    builder.append("SET '" + KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE + "' = 'true';");
    builder.append("DEFINE var = '" + ORDER_DATA_PROVIDER.sourceName() + "';");
    builder.append("CREATE STREAM shouldRunCommand AS SELECT * FROM ${var};");

    // When:
    localCli.runCommand(builder.toString());

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Created query with ID CSAS_SHOULDRUNCOMMAND"));
  }

  @Test
  public void shouldThrowWhenTryingToSetDeniedProperty() throws Exception {
    // Given
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.makeIsValidRequest("ksql.service.id"))
        .thenReturn(RestResponse.erroneous(
            NOT_ACCEPTABLE.code(),
            new KsqlErrorMessage(Errors.toErrorCode(NOT_ACCEPTABLE.code()),
                "Property cannot be set")));

    // When:
    assertThrows(IllegalArgumentException.class, () ->
        localCli.handleLine("set 'ksql.service.id'='test';"));
  }

  @Test
  public void shouldRunScriptOnRunInteractively() throws Exception {
    // Given:
    final File scriptFile = TMP.newFile("script.sql");
    Files.write(scriptFile.toPath(), (""
        + "CREATE STREAM " + streamName + " AS SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + ";"
        + "").getBytes(StandardCharsets.UTF_8));

    when(lineSupplier.get())
        .thenReturn("run script '" + scriptFile.getAbsolutePath() + "'")
        .thenReturn("exit");

    // When:
    localCli.runInteractively();

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Created query with ID CSAS_" + streamName));
  }

  @Test
  public void shouldRunScriptOnRunScript() throws Exception {
    // Given:
    final File scriptFile = TMP.newFile("script.sql");
    Files.write(scriptFile.toPath(), (""
        + "CREATE STREAM shouldRunScript AS SELECT * FROM " + ORDER_DATA_PROVIDER.sourceName() + ";"
        + "").getBytes(StandardCharsets.UTF_8));

    // When:
    localCli.runScript(scriptFile.getPath());

    // Then:
    assertThat(terminal.getOutputString(),
        containsString("Created query with ID CSAS_SHOULDRUNSCRIPT"));
  }

  @Test
  public void shouldPrintOnlyFailedStatementFromScriptFile() throws Exception {
    // Given:
    final File scriptFile = TMP.newFile("script.sql");
    Files.write(scriptFile.toPath(), (""
        + "drop stream if exists s1;\n"
        + "create stream if not exist s1(id int) with (kafka_topic='s1', value_format='json', "
        + "partitions=1);\n").getBytes(StandardCharsets.UTF_8));

    // When:
    final int error_code = localCli.runScript(scriptFile.getPath());

    // Then:
    final String out = terminal.getOutputString();
    final String expected = "line 2:22: " +
        "no viable alternative at input 'create stream if not exist'\n" +
        "Statement: create stream if not exist s1(id int) with " +
        "(kafka_topic='s1', value_format='json', partitions=1);\n" +
        "Caused by: line 2:22: Syntax error at line 2:22\n";
    assertThat(error_code, is(-1));
    assertThat(out, is(expected));
    assertThat(out, not(containsString("drop stream if exists")));
  }

  @Test
  public void shouldUpdateCommandSequenceNumber() throws Exception {
    // Given:
    final String statementText = "create stream foo;";
    final KsqlRestClient mockRestClient = givenMockRestClient();
    final CommandStatusEntity stubEntity = stubCommandStatusEntityWithSeqNum(12L);
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(
            OK.code(),
            new KsqlEntityList(Collections.singletonList(stubEntity))
        ));

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
        .thenReturn(RestResponse.successful(
            OK.code(),
            new KsqlEntityList(ImmutableList.of(firstEntity, secondEntity))
        ));

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
        .thenReturn(RestResponse.successful(OK.code(), new KsqlEntityList()));

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
        .thenReturn(RestResponse.successful(OK.code(), new KsqlEntityList()));

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
        .thenReturn(RestResponse.successful(
            OK.code(),
            new KsqlEntityList(Collections.singletonList(stubEntity))
        ));

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
    when(mockRestClient.getServerInfo()).thenReturn(
        RestResponse.successful(OK.code(),
            new ServerInfo("version", "kafkaClusterId", "ksqlServiceId", "serverStatus")));

    // When:
    runCliSpecificCommand("server foo");

    // Then:
    assertLastCommandSequenceNumber(mockRestClient, -1L);
  }

  @Test
  public void shouldIssueCCloudConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getIsCCloudServer()).thenReturn(true);
    when(mockRestClient.getHasCCloudApiKey()).thenReturn(true);
    when(mockRestClient.makeConnectorRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(
            OK.code(),
            new KsqlEntityList(Collections.singletonList(
                new ConnectorList("list connectors;", Collections.emptyList(), Collections.emptyList())))
        ));

    // When:
    localCli.handleLine("list connectors;");

    // Then:
    verify(mockRestClient).makeConnectorRequest(anyString(), anyLong());
  }

  @Test
  public void shouldIssueNonCCloudConnectorRequest() throws Exception {
    // Given:
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getIsCCloudServer()).thenReturn(false);
    when(mockRestClient.makeConnectorRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(
            OK.code(),
            new KsqlEntityList(Collections.singletonList(
                new ConnectorList("list connectors;", Collections.emptyList(), Collections.emptyList())))
        ));

    // When:
    localCli.handleLine("list connectors;");

    // Then:
    verify(mockRestClient).makeConnectorRequest(anyString(), anyLong());
  }

  @Test
  public void shouldThrowOnCCloudConnectorRequestWithoutApiKey() throws Exception {
    // Given:
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getIsCCloudServer()).thenReturn(true);
    when(mockRestClient.getHasCCloudApiKey()).thenReturn(false);

    // When:
    final Exception e = assertThrows(
        KsqlMissingCredentialsException.class,
        () -> localCli.handleLine("list connectors;")
    );

    // Then:
    assertThat(e.getMessage(), containsString("In order to use ksqlDB's connector "
        + "management capabilities with a Confluent Cloud ksqlDB server, launch the "
        + "ksqlDB command line with the additional flags '--confluent-api-key' and "
        + "'--confluent-api-secret' to pass a Confluent Cloud API key."));
    verify(mockRestClient, never()).makeConnectorRequest(anyString(), anyLong());
  }

  @Test
  public void shouldNotExecuteAnyStatementsOnFailedValidation() throws Exception {
    // Given:
    final KsqlRestClient mockRestClient = givenMockRestClient();
    when(mockRestClient.getIsCCloudServer()).thenReturn(true);
    when(mockRestClient.getHasCCloudApiKey()).thenReturn(false);

    // When: "show streams;" is valid but "list connectors;" will fail validation
    assertThrows(
        KsqlMissingCredentialsException.class,
        () -> localCli.handleLine("show streams; list connectors;")
    );

    // Then:
    verify(mockRestClient, never()).makeConnectorRequest(anyString(), anyLong());
    // "show streams;" should not have been executed either
    verify(mockRestClient, never()).makeKsqlRequest(anyString(), anyLong());
  }

  private void givenRequestPipelining(final String setting) {
    runCliSpecificCommand(RequestPipeliningCommand.NAME + " " + setting);
  }

  private void runCliSpecificCommand(final String command) {
    when(lineSupplier.get()).thenReturn(command).thenReturn("");
    console.nextNonCliCommand();
  }

  private void givenRunInteractivelyWillExit() {
    when(lineSupplier.get()).thenReturn("eXiT");
  }

  private KsqlRestClient givenMockRestClient() throws Exception {
    return givenMockRestClient("100.0.0-0");
  }

  private KsqlRestClient givenMockRestClient(final String serverVersion) throws Exception {
    final KsqlRestClient mockRestClient = mock(KsqlRestClient.class);

    when(mockRestClient.getServerInfo()).thenReturn(RestResponse.successful(
        OK.code(),
        new ServerInfo(serverVersion, "testClusterId", "testServiceId", "status")
    ));

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
      final KsqlRestClient mockRestClient,
      final long seqNum
  ) {
    final CommandStatusEntity stubEntity = stubCommandStatusEntityWithSeqNum(seqNum);
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong())).thenReturn(
        RestResponse.successful(
            OK.code(),
            new KsqlEntityList(Collections.singletonList(stubEntity))
        ));
    localCli.handleLine("create stream foo;");
  }

  private void assertLastCommandSequenceNumber(
      final KsqlRestClient mockRestClient,
      final long seqNum
  ) {
    // Given:
    reset(mockRestClient);
    final String statementText = "list streams;";
    when(mockRestClient.makeKsqlRequest(anyString(), anyLong()))
        .thenReturn(RestResponse.successful(OK.code(), new KsqlEntityList()));

    // When:
    localCli.handleLine(statementText);

    // Then:
    verify(mockRestClient).makeKsqlRequest(statementText, seqNum);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Matcher<String>[] prependWithKey(final String key, final List<?> values) {

    final Matcher[] allMatchers = new Matcher[values.size() + 1];
    allMatchers[0] = is(key);

    for (int idx = 0; idx != values.size(); ++idx) {
      allMatchers[idx + 1] = is(values.get(idx).toString());
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

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static Matcher<Iterable<? extends Iterable<? extends String>>> isRow(
      final Matcher<String>... expected
  ) {
    return Matchers.contains(row(expected));
  }

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked", "rawtypes"})
  private static Matcher<Iterable<? extends Iterable<? extends String>>> hasRow(
      final Matcher<String>... expected
  ) {
    return (Matcher) Matchers.hasItems(row(expected));
  }

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked", "rawtypes"})
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

  private static String extractQueryId(final String outputString) {
    final java.util.regex.Matcher matcher = QUERY_ID_PATTERN.matcher(outputString);
    assertThat("Could not find query id in: " + outputString, matcher.find());
    return matcher.group(1);
  }

  private static class TestRowCaptor implements RowCaptor {

    private ImmutableList.Builder<List<String>> rows = ImmutableList.builder();

    @Override
    public void addRows(final List<List<String>> rows) {
      rows.forEach(this::addRow);
    }

    @Override
    public void addRow(final DataRow row) {
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
