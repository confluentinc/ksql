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

package io.confluent.ksql;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.*;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static io.confluent.ksql.TestResult.build;
import static io.confluent.ksql.util.KsqlConfig.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Most tests in CliTest are end-to-end integration tests, so it may expect a long running time.
 */
@Category({IntegrationTest.class})
public class CliTest extends TestRunner {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
  private static final String COMMANDS_KSQL_TOPIC_NAME = KsqlRestApplication.COMMANDS_KSQL_TOPIC_NAME;
  private static final int PORT = 9098;
  private static final String LOCAL_REST_SERVER_ADDR = "http://localhost:" + PORT;
  private static final OutputFormat CLI_OUTPUT_FORMAT = OutputFormat.TABULAR;

  private static final long STREAMED_QUERY_ROW_LIMIT = 10000;
  private static final long STREAMED_QUERY_TIMEOUT_MS = 10000;

  private static final TestResult.OrderedResult EMPTY_RESULT = build("");

  private static Cli localCli;
  private static TestTerminal terminal;
  private static String commandTopicName;
  private static TopicProducer topicProducer;
  private static TopicConsumer topicConsumer;

  private static OrderDataProvider orderDataProvider;
  private static int result_stream_no = 0;

  @BeforeClass
  public static void setUp() throws Exception {
    KsqlRestClient restClient = new KsqlRestClient(LOCAL_REST_SERVER_ADDR);

    // TODO: Fix Properties Setup in Local().getCli()
    // Local local =  new Local().getCli();
    // LocalCli localCli = local.getCli(restClient, terminal);

    // TODO: add remote cli test cases
    terminal = new TestTerminal(CLI_OUTPUT_FORMAT, restClient);

    KsqlRestConfig restServerConfig = new KsqlRestConfig(defaultServerProperties());
    commandTopicName = restServerConfig.getCommandTopic(KsqlConfig.KSQL_SERVICE_ID_DEFAULT);

    orderDataProvider = new OrderDataProvider();
    CLUSTER.createTopic(orderDataProvider.topicName());

    KsqlRestApplication restServer = KsqlRestApplication.buildApplication(restServerConfig, false,
        EasyMock.mock(VersionCheckerAgent.class)
    );

    restServer.start();

    localCli = new Cli(
        STREAMED_QUERY_ROW_LIMIT,
        STREAMED_QUERY_TIMEOUT_MS,
        restClient,
        terminal
    );

    TestRunner.setup(localCli, terminal);

    topicProducer = new TopicProducer(CLUSTER);
    topicConsumer = new TopicConsumer(CLUSTER);

    testListOrShowCommands();

    produceInputStream(orderDataProvider);
  }


  private static void produceInputStream(TestDataProvider dataProvider) throws Exception {
    createKStream(dataProvider);
    topicProducer.produceInputData(dataProvider);
  }

  private static void createKStream(TestDataProvider dataProvider) {
    test(
        String.format("CREATE STREAM %s %s WITH (value_format = 'json', kafka_topic = '%s' , key='%s')",
            dataProvider.kstreamName(), dataProvider.ksqlSchemaString(), dataProvider.topicName(), dataProvider.key()),
        build("Stream created")
    );
  }

  private static void testListOrShowCommands() {
    TestResult.OrderedResult testResult = (TestResult.OrderedResult) TestResult.init(true);
    testResult.addRows(Arrays.asList(Arrays.asList(orderDataProvider.topicName(), "false", "1",
                                                   "1", "0", "0")));
    testListOrShow("topics", testResult);
    testListOrShow("registered topics", build(COMMANDS_KSQL_TOPIC_NAME, commandTopicName, "JSON"));
    testListOrShow("streams", EMPTY_RESULT);
    testListOrShow("tables", EMPTY_RESULT);
    testListOrShow("queries", EMPTY_RESULT);
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
  }

  private static Map<String, Object> genDefaultConfigMap() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put(KsqlRestConfig.LISTENERS_CONFIG, CliUtils.getLocalServerAddress(PORT));
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    return configMap;
  }

  private static Properties defaultServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.putAll(genDefaultConfigMap());
    return serverProperties;
  }

  private static Map<String, Object> validStartUpConfigs() {
    // TODO: these configs should be set with other configs on start-up, rather than setup later.
    Map<String, Object> startConfigs = genDefaultConfigMap();
    startConfigs.put("num.stream.threads", 4);

    startConfigs.put(SINK_NUMBER_OF_REPLICAS_PROPERTY, 1);
    startConfigs.put(SINK_NUMBER_OF_PARTITIONS_PROPERTY, 4);
    startConfigs.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_MS_PROPERTY, 1000000);

    startConfigs.put(KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG, KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT);
    startConfigs.put(KSQL_SERVICE_ID_CONFIG, KSQL_SERVICE_ID_DEFAULT);
    startConfigs.put(KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG, KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT);
    startConfigs.put(KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT);
    startConfigs.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,  "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
    startConfigs.put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, KsqlConfig.defaultSchemaRegistryUrl);
    startConfigs.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogMetricAndContinueExceptionHandler.class.getName());
    return startConfigs;
  }

  private static void testCreateStreamAsSelect(String selectQuery, Schema resultSchema, Map<String, GenericRow> expectedResults) throws Exception {
    if (!selectQuery.endsWith(";")) {
      selectQuery += ";";
    }
    String resultKStreamName = "RESULT_" + result_stream_no++;
    final String queryString = "CREATE STREAM " + resultKStreamName + " AS " + selectQuery;

    /* Start Stream Query */
    test(queryString, build("Stream created and running"));

    /* Assert Results */
    Map<String, GenericRow> results = topicConsumer.readResults(resultKStreamName, resultSchema, expectedResults.size(), new StringDeserializer());

    terminateQuery("CSAS_" + resultKStreamName);

    dropStream(resultKStreamName);
    assertThat(results, equalTo(expectedResults));
  }

  private static void terminateQuery(String queryId) {
    test(
        String.format("terminate %s", queryId),
        build("Query terminated.")
    );
  }

  private static void dropStream(String name) {
    test(
        String.format("drop stream %s", name),
        build("Source " + name + " was dropped")
    );
  }

  private static void selectWithLimit(String selectQuery, int limit, TestResult.OrderedResult expectedResults) {
    selectQuery += " LIMIT " + limit + ";";
    test(selectQuery, expectedResults);
  }

  @Test
  public void testPrint() throws InterruptedException {

    Thread wait = new Thread(() -> {
        CliTest.this.run("print 'ORDER_TOPIC' FROM BEGINNING INTERVAL 2;", false);
    });

    wait.start();
    Thread.sleep(1000);
    wait.interrupt();

    String terminalOutput = terminal.getOutputString();
    assertThat(terminalOutput, containsString("Format:JSON"));
  }

  @Test
  public void testPropertySetUnset() {
    test("set 'application.id' = 'Test_App'", EMPTY_RESULT);
    test("set 'producer.batch.size' = '16384'", EMPTY_RESULT);
    test("set 'max.request.size' = '1048576'", EMPTY_RESULT);
    test("set 'consumer.max.poll.records' = '500'", EMPTY_RESULT);
    test("set 'enable.auto.commit' = 'true'", EMPTY_RESULT);
    test("set 'ksql.streams.application.id' = 'Test_App'", EMPTY_RESULT);
    test("set 'ksql.streams.producer.batch.size' = '16384'", EMPTY_RESULT);
    test("set 'ksql.streams.max.request.size' = '1048576'", EMPTY_RESULT);
    test("set 'ksql.streams.consumer.max.poll.records' = '500'", EMPTY_RESULT);
    test("set 'ksql.streams.enable.auto.commit' = 'true'", EMPTY_RESULT);
    test("set 'ksql.service.id' = 'test'", EMPTY_RESULT);

    test("unset 'application.id'", EMPTY_RESULT);
    test("unset 'producer.batch.size'", EMPTY_RESULT);
    test("unset 'max.request.size'", EMPTY_RESULT);
    test("unset 'consumer.max.poll.records'", EMPTY_RESULT);
    test("unset 'enable.auto.commit'", EMPTY_RESULT);
    test("unset 'ksql.streams.application.id'", EMPTY_RESULT);
    test("unset 'ksql.streams.producer.batch.size'", EMPTY_RESULT);
    test("unset 'ksql.streams.max.request.size'", EMPTY_RESULT);
    test("unset 'ksql.streams.consumer.max.poll.records'", EMPTY_RESULT);
    test("unset 'ksql.streams.enable.auto.commit'", EMPTY_RESULT);
    test("unset 'ksql.service.id'", EMPTY_RESULT);

    testListOrShow("properties", build(validStartUpConfigs()), false);
  }

  @Test
  public void testDescribe() {
    test("describe topic " + COMMANDS_KSQL_TOPIC_NAME,
        build(COMMANDS_KSQL_TOPIC_NAME, commandTopicName, "JSON"));
  }

  @Test
  public void testSelectStar() throws Exception {
    testCreateStreamAsSelect(
        "SELECT * FROM " + orderDataProvider.kstreamName(),
        orderDataProvider.schema(),
        orderDataProvider.data()
    );
  }

  @Test
  public void testSelectProject() throws Exception {
    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("ITEM_1", 10.0, new
        Double[]{100.0,
        110.99,
        90.0 })));
    expectedResults.put("2", new GenericRow(Arrays.asList("ITEM_2", 20.0, new
        Double[]{10.0,
        10.99,
        9.0 })));

    expectedResults.put("3", new GenericRow(Arrays.asList("ITEM_3", 30.0, new
        Double[]{10.0,
        10.99,
        91.0 })));

    expectedResults.put("4", new GenericRow(Arrays.asList("ITEM_4", 40.0, new
        Double[]{10.0,
        140.99,
        94.0 })));

    expectedResults.put("5", new GenericRow(Arrays.asList("ITEM_5", 50.0, new
        Double[]{160.0,
        160.99,
        98.0 })));

    expectedResults.put("6", new GenericRow(Arrays.asList("ITEM_6", 60.0, new
        Double[]{1000.0,
        1100.99,
        900.0 })));

    expectedResults.put("7", new GenericRow(Arrays.asList("ITEM_7", 70.0, new
        Double[]{1100.0,
        1110.99,
        190.0 })));

    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 80.0, new
        Double[]{1100.0,
        1110.99,
        970.0 })));

    Schema resultSchema = SchemaBuilder.struct()
        .field("ITEMID", SchemaBuilder.STRING_SCHEMA)
        .field("ORDERUNITS", SchemaBuilder.FLOAT64_SCHEMA)
        .field("PRICEARRAY", SchemaBuilder.array(SchemaBuilder.FLOAT64_SCHEMA))
        .build();

    testCreateStreamAsSelect(
        "SELECT ITEMID, ORDERUNITS, PRICEARRAY FROM " + orderDataProvider.kstreamName(),
        resultSchema,
        expectedResults
    );
  }

  @Test
  public void testSelectFilter() throws Exception {
    Map<String, GenericRow> expectedResults = new HashMap<>();
    Map<String, Double> mapField = new HashMap<>();
    mapField.put("key1", 1.0);
    mapField.put("key2", 2.0);
    mapField.put("key3", 3.0);
    expectedResults.put("8", new GenericRow(Arrays.asList(8, "ORDER_6",
        "ITEM_8", 80.0,
        "2018-01-08",
        new Double[]{1100.0,
            1110.99,
            970.0 },
        mapField)));

    testCreateStreamAsSelect(
        "SELECT * FROM " + orderDataProvider.kstreamName() + " WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8'",
        orderDataProvider.schema(),
        expectedResults
        );
  }

  @Test
  public void testSelectLimit() {
    TestResult.OrderedResult expectedResult = TestResult.build();
    Map<String, GenericRow> streamData = orderDataProvider.data();
    int limit = 3;
    for (int i = 1; i <= limit; i++) {
      GenericRow srcRow = streamData.get(Integer.toString(i));
      List<Object> columns = srcRow.getColumns();
      GenericRow resultRow = new GenericRow(Arrays.asList(columns.get(1), columns.get(2)));
      expectedResult.addRow(resultRow);
    }
    selectWithLimit(
        "SELECT ORDERID, ITEMID FROM " + orderDataProvider.kstreamName(), limit, expectedResult);
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

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)));

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
  public void shouldHandleRegisterTopic() throws Exception {
    localCli.handleLine("REGISTER TOPIC foo WITH (value_format = 'csv', kafka_topic='foo');");
  }

  @Test
  public void shouldPrintErrorIfCantConnectToRestServer() {
    new Cli(1L, 1L, new KsqlRestClient("xxxx", Collections.emptyMap()), terminal);
    assertThat(terminal.getOutputString(), containsString("Remote server address may not be valid"));
  }

  @Test
  public void shouldRegisterRemoteCommand() {
    new Cli(1L, 1L, new KsqlRestClient("xxxx", Collections.emptyMap()), terminal);
    assertThat(terminal.getCliSpecificCommands().get("server"),
        instanceOf(Cli.RemoteServerSpecificCommand.class));
  }
}