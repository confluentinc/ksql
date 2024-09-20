/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.integration;

import static io.confluent.ksql.api.client.util.ClientTestUtil.shouldReceiveRows;
import static io.confluent.ksql.api.client.util.ClientTestUtil.subscribeAndWait;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Client.HttpResponse;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.ConnectorDescription;
import io.confluent.ksql.api.client.ConnectorInfo;
import io.confluent.ksql.api.client.ConnectorType;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.api.client.impl.ConnectorTypeImpl;
import io.confluent.ksql.api.client.util.ClientTestUtil.TestSubscriber;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.ConnectExecutable;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlTimeTypes;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.reactivestreams.Publisher;

@Category({IntegrationTest.class})
public class ClientIntegrationTest {

  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER = new StructuredTypesDataProvider();
  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();
  private static final int TEST_NUM_ROWS = TEST_DATA_PROVIDER.data().size();
  private static final List<String> TEST_COLUMN_NAMES =
      ImmutableList.of("K", "STR", "LONG", "DEC", "BYTES_", "ARRAY", "MAP", "STRUCT", "COMPLEX",
          "TIMESTAMP", "DATE", "TIME", "HEAD");
  private static final List<ColumnType> TEST_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRUCT", "STRING", "BIGINT", "DECIMAL",
          "BYTES", "ARRAY", "MAP", "STRUCT", "STRUCT", "TIMESTAMP", "DATE", "TIME", "BYTES"));
  private static final List<KsqlArray> TEST_EXPECTED_ROWS =
      convertToClientRows(TEST_DATA_PROVIDER.data());

  private static final Format KEY_FORMAT = FormatFactory.JSON;
  private static final Format VALUE_FORMAT = FormatFactory.JSON;
  private static final Supplier<Long> TS_SUPPLIER = () -> 0L;
  private static final Supplier<List<Header>> HEADERS_SUPPLIER = () -> ImmutableList.of(new RecordHeader("h0", new byte[] {23}));

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String AN_AGG_KEY = "STRUCT(F1 := ARRAY['a'])";
  private static final PhysicalSchema AGG_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("K"), SqlTypes.struct()
              .field("F1", SqlTypes.array(SqlTypes.STRING))
              .build())
          .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
          .build(),
      SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
      SerdeFeatures.of()
  );

  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER = new TestDataProvider(
      "EMPTY_STRUCTURED_TYPES", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
  private static final String EMPTY_TEST_TOPIC = EMPTY_TEST_DATA_PROVIDER.topicName();
  private static final String EMPTY_TEST_STREAM = EMPTY_TEST_DATA_PROVIDER.sourceName();

  private static final String PUSH_QUERY = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES;";
  private static final String PULL_QUERY_ON_STREAM = "SELECT * FROM " + TEST_STREAM + ";";
  private static final String PULL_QUERY_ON_TABLE =
      "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";
  private static final int PUSH_QUERY_LIMIT_NUM_ROWS = 2;
  private static final String PUSH_QUERY_WITH_LIMIT =
      "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT " + PUSH_QUERY_LIMIT_NUM_ROWS + ";";

  private static final List<String> PULL_QUERY_COLUMN_NAMES = ImmutableList.of("K", "LONG");
  private static final List<ColumnType> PULL_QUERY_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRUCT", "BIGINT"));
  private static final KsqlArray PULL_QUERY_EXPECTED_ROW = new KsqlArray()
      .add(new KsqlObject().put("F1", new KsqlArray().add("a")))
      .add(1);

  protected static final String EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC =
      "The ksqlDB server accepted the statement issued via executeStatement(), but the response "
          + "received is of an unexpected format. ";
  protected static final String EXECUTE_STATEMENT_USAGE_DOC = "The executeStatement() method is only "
      + "for 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS "
      + "SELECT' statements. ";

  private static final String TEST_CONNECTOR = "TEST_CONNECTOR";
  private static final String MOCK_SOURCE_CLASS = "org.apache.kafka.connect.tools.MockSourceConnector";
  private static final ConnectorType SOURCE_TYPE = new ConnectorTypeImpl("SOURCE");

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  // these properties are set together to allow us to verify that we can handle push queries
  // in the worker pool without blocking the event loop.
  private static final int EVENT_LOOP_POOL_SIZE = 1;
  private static final int NUM_CONCURRENT_REQUESTS_TO_TEST = 5;
  private static final int WORKER_POOL_SIZE = 10;

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty("ksql.verticle.instances", EVENT_LOOP_POOL_SIZE)
      .withProperty("ksql.worker.pool.size", WORKER_POOL_SIZE)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private static ConnectExecutable CONNECT;
  private static ClassLoader initClassLoader;

  @BeforeClass
  public static void setUpClass() throws Exception {
    TEST_HARNESS.ensureTopics(TEST_TOPIC, EMPTY_TEST_TOPIC);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT, TS_SUPPLIER, HEADERS_SUPPLIER);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT K, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM + " GROUP BY K;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        4, // Only unique keys are counted
        KEY_FORMAT,
        VALUE_FORMAT,
        AGG_SCHEMA
    );

    final String testDir = Paths.get(TestUtils.tempDirectory().getAbsolutePath(), "client_integ_test").toString();
    final String connectFilePath = Paths.get(testDir, "connect.properties").toString();
    Files.createDirectories(Paths.get(testDir));

    writeConnectConfigs(connectFilePath, ImmutableMap.<String, String>builder()
        .put("bootstrap.servers", TEST_HARNESS.kafkaBootstrapServers())
        .put("group.id", UUID.randomUUID().toString())
        .put("key.converter", StringConverter.class.getName())
        .put("value.converter", JsonConverter.class.getName())
        .put("offset.storage.topic", "connect-offsets")
        .put("status.storage.topic", "connect-status")
        .put("config.storage.topic", "connect-config")
        .put("offset.storage.replication.factor", "1")
        .put("status.storage.replication.factor", "1")
        .put("config.storage.replication.factor", "1")
        .put("value.converter.schemas.enable", "false")
        .build()
    );

    initClassLoader = Thread.currentThread().getContextClassLoader();
    CONNECT = ConnectExecutable.of(connectFilePath);
    CONNECT.startAsync();
  }

  private static void writeConnectConfigs(final String path, final Map<String, String> configs) throws Exception {
    try (final PrintWriter out = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(path, true), StandardCharsets.UTF_8))) {
      for (final Map.Entry<String, String> entry : configs.entrySet()) {
        out.println(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  @AfterClass
  public static void classTearDown() {
    cleanupConnectors();
    CONNECT.shutdown();
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
    Thread.currentThread().setContextClassLoader(initClassLoader);
  }

  private Vertx vertx;
  private Client client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    client = createClient();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test
  public void shouldStreamMultiplePushQueries() throws Exception {
    // When
    final StreamedQueryResult[] streamedQueryResults = new StreamedQueryResult[NUM_CONCURRENT_REQUESTS_TO_TEST];
    for (int i = 0; i < streamedQueryResults.length; i++) {
      streamedQueryResults[i] = client.streamQuery(PUSH_QUERY).get();
    }

    // Then
    for (final StreamedQueryResult streamedQueryResult : streamedQueryResults) {
      assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
      assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
      assertThat(streamedQueryResult.queryID(), is(notNullValue()));
    }

    for (final StreamedQueryResult streamedQueryResult : streamedQueryResults) {
      shouldReceiveStreamRows(streamedQueryResult, false);
    }

    for (final StreamedQueryResult streamedQueryResult : streamedQueryResults) {
      assertThat(streamedQueryResult.isComplete(), is(false));
    }
  }

  @Test
  public void shouldOnlyWarnForDuplicateIfNotExists() throws Exception {
    // When
    ExecuteStatementResult result;
    client.executeStatement("CREATE STREAM FOO (id INT KEY, bar VARCHAR) WITH(value_format='json', kafka_topic='foo', partitions=6);").get();
    client.executeStatement("CREATE STREAM IF NOT EXISTS BAR AS SELECT * FROM FOO EMIT CHANGES;").get();
    result = client.executeStatement("CREATE STREAM IF NOT EXISTS BAR AS SELECT * FROM FOO EMIT CHANGES;").get();

    //Then
    assertThat(result.queryId(), is(Optional.empty()));
  }

  @Test
  public void shouldStreamPushQueryAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceiveStreamRows(streamedQueryResult, false);

    assertThat(streamedQueryResult.isComplete(), is(false));
  }

  @Test
  public void shouldStreamPushQuerySync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    for (int i = 0; i < TEST_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyStreamRowWithIndex(row, i);
    }

    assertThat(streamedQueryResult.isComplete(), is(false));
  }

  @Test
  public void shouldStreamPullQueryOnStreamAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        client.streamQuery(PULL_QUERY_ON_STREAM).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceiveRows(
        streamedQueryResult,
        6,
        rows -> verifyStreamRows(rows, 6),
        true
    );
  }

  @Test
  public void shouldStreamPullQueryOnStreamSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PULL_QUERY_ON_STREAM).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    final List<Row> results = new LinkedList<>();
    Row row;
    while (true) {
      row = streamedQueryResult.poll();
      if (row == null) {
        break;
      } else {
        results.add(row);
      }
    }

    verifyStreamRows(results, 6);

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPullQueryOnEmptyStreamSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(
        "SELECT * FROM " + EMPTY_TEST_STREAM + ";").get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    final List<Row> results = new LinkedList<>();
    Row row;
    while (true) {
      row = streamedQueryResult.poll();
      if (row == null) {
        break;
      } else {
        results.add(row);
      }
    }

    verifyStreamRows(results, 0);

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPullQueryOnTableAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PULL_QUERY_ON_TABLE).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PULL_QUERY_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PULL_QUERY_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceiveRows(
        streamedQueryResult,
        1,
        ClientIntegrationTest::verifyPullQueryRows,
        true
    );

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPullQueryOnTableSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PULL_QUERY_ON_TABLE).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PULL_QUERY_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PULL_QUERY_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    final Row row = streamedQueryResult.poll();
    verifyPullQueryRow(row);
    assertThat(streamedQueryResult.poll(), is(nullValue()));

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPushQueryWithLimitAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceiveStreamRows(streamedQueryResult, true, PUSH_QUERY_LIMIT_NUM_ROWS);

    assertThat(streamedQueryResult.isComplete(), is(true));
  }

  @Test
  public void shouldStreamPushQueryWithLimitSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    for (int i = 0; i < PUSH_QUERY_LIMIT_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyStreamRowWithIndex(row, i);
    }
    assertThat(streamedQueryResult.poll(), is(nullValue()));

    assertThat(streamedQueryResult.isComplete(), is(true));
  }

  @Test
  public void shouldHandleErrorResponseFromStreamQuery() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.streamQuery("SELECT * FROM NONEXISTENT EMIT CHANGES;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("NONEXISTENT does not exist"));
  }

  @Test
  public void shouldDeliverBufferedRowsViaPollIfComplete() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();
    assertThatEventually(streamedQueryResult::isComplete, is(true));

    // When / Then
    for (int i = 0; i < PUSH_QUERY_LIMIT_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyStreamRowWithIndex(row, i);
    }
    assertThat(streamedQueryResult.poll(), is(nullValue()));
  }

  @Test
  public void shouldAllowSubscribeStreamedQueryResultIfComplete() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();
    assertThatEventually(streamedQueryResult::isComplete, is(true));

    // When
    final TestSubscriber<Row> subscriber = subscribeAndWait(streamedQueryResult);
    assertThat(subscriber.getValues(), hasSize(0));
    subscriber.getSub().request(PUSH_QUERY_LIMIT_NUM_ROWS);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(PUSH_QUERY_LIMIT_NUM_ROWS));
    verifyStreamRows(subscriber.getValues(), PUSH_QUERY_LIMIT_NUM_ROWS);
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldExecutePullQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PULL_QUERY_ON_TABLE);

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));

    verifyPullQueryRows(batchedQueryResult.get()); 
  }

  @Test
  public void shouldExecutePullQueryWithVariables() throws Exception {
    // When
    client.define("AGG_TABLE", AGG_TABLE);
    client.define("value", false);
    final BatchedQueryResult batchedQueryResult = client.executeQuery("SELECT ${value} from ${AGG_TABLE} WHERE K=STRUCT(F1 := ARRAY['a']);");

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
    assertThat(batchedQueryResult.get().get(0).getBoolean(1), is(false));
  }

  @Test
  public void shouldExecutePushWithLimitQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PUSH_QUERY_WITH_LIMIT);

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));

    verifyStreamRows(batchedQueryResult.get(), PUSH_QUERY_LIMIT_NUM_ROWS);
  }

  @Test
  public void shouldExecutePushQueryWithVariables() throws Exception {
    // When
    client.define("TEST_STREAM", TEST_STREAM);
    client.define("number", 4567);
    final BatchedQueryResult batchedQueryResult =
        client.executeQuery("SELECT ${number} FROM ${TEST_STREAM} EMIT CHANGES LIMIT " + PUSH_QUERY_LIMIT_NUM_ROWS + ";");

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
    assertThat(batchedQueryResult.get().get(0).getInteger(1), is(4567));
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteQuery() {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery("SELECT * from A_FAKE_TABLE_NAME;");
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        batchedQueryResult::get
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("A_FAKE_TABLE_NAME does not exist."));

    // queryID future should also be completed exceptionally
    final Exception queryIdException = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> batchedQueryResult.queryID().get()
    );
    assertThat(queryIdException.getCause(), instanceOf(KsqlClientException.class));
    assertThat(queryIdException.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(queryIdException.getCause().getMessage(), containsString("A_FAKE_TABLE_NAME does not exist."));
  }

  @Test
  public void shouldTerminatePushQueryIssuedViaStreamQuery() throws Exception {
    // Given: one persistent query for the agg table
    verifyNumActiveQueries(1);

    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY).get();
    final String queryId = streamedQueryResult.queryID();
    assertThat(queryId, is(notNullValue()));

    // Query is running on server, and StreamedQueryResult is not complete
    verifyNumActiveQueries(2);
    assertThat(streamedQueryResult.isComplete(), is(false));

    // When
    client.terminatePushQuery(queryId).get();

    // Then: query is no longer running on server, and StreamedQueryResult is complete
    verifyNumActiveQueries(1);
    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldTerminatePushQueryIssuedViaExecuteQuery() throws Exception {
    // Given: one persistent query for the agg table
    verifyNumActiveQueries(1);

    // Issue non-terminating push query via executeQuery(). This is NOT an expected use case
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PUSH_QUERY);
    final String queryId = batchedQueryResult.queryID().get();
    assertThat(queryId, is(notNullValue()));

    // Query is running on server, and StreamedQueryResult is not complete
    verifyNumActiveQueries(2);
    assertThat(batchedQueryResult.isDone(), is(false));

    // When
    client.terminatePushQuery(queryId).get();

    // Then: query is no longer running on server, and BatchedQueryResult is complete
    verifyNumActiveQueries(1);
    assertThatEventually(batchedQueryResult::isDone, is(true));
    assertThat(batchedQueryResult.isCompletedExceptionally(), is(false));
  }

  @Test
  public void shouldHandleErrorResponseFromTerminatePushQuery() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.terminatePushQuery("NONEXISTENT").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("No query with id NONEXISTENT"));
  }

  @Test
  public void shouldExecutePlainHttpRequests() throws Exception {
    final HttpResponse response = client.buildRequest("GET", "/info").send().get();
    assertThat(response.status(), is(200));
    final Map<String, Map<String, Object>> info = response.bodyAsMap();

    final ServerInfo serverInfo = client.serverInfo().get();
    final Map<String, Object> innerMap = info.get("KsqlServerInfo");

    assertThat(innerMap.get("version"), is(serverInfo.getServerVersion()));
    assertThat(innerMap.get("ksqlServiceId"), is(serverInfo.getKsqlServiceId()));
    assertThat(innerMap.get("kafkaClusterId"), is(serverInfo.getKafkaClusterId()));
    assertThat(innerMap.get("serverStatus"), is("RUNNING"));
  }

  @Test
  public void shouldHandleInvalidSqlInExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("bad sql;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("mismatched input"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 40001"));
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("drop stream NONEXISTENT;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Stream NONEXISTENT does not exist"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 40001"));
  }

  @Test
  public void shouldRejectMultipleRequestsFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("drop stream S1; drop stream S2;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(
        "executeStatement() may only be used to execute one statement at a time"));
  }

  @Test
  public void shouldRejectRequestWithMissingSemicolonFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("sql missing semicolon").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(
        "Missing semicolon in SQL for executeStatement() request"));
  }

  @Test
  public void shouldFailOnNoEntitiesFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("set 'auto.offset.reset' = 'earliest';").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
  }

  @Test
  public void shouldFailToListStreamsViaExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeStatement("list streams;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(), containsString("Use the listStreams() method instead"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldListStreams() throws Exception {
    // When
    final List<StreamInfo> streams = client.listStreams().get();

    // Then
    assertThat("" + streams, streams, containsInAnyOrder(
        streamForProvider(TEST_DATA_PROVIDER),
        streamForProvider(EMPTY_TEST_DATA_PROVIDER)
    ));
  }

  @Test
  public void shouldListTables() throws Exception {
    // When
    final List<TableInfo> tables = client.listTables().get();

    // Then
    assertThat("" + tables, tables, contains(
        tableInfo(AGG_TABLE, AGG_TABLE, KEY_FORMAT.name(), VALUE_FORMAT.name(), false)
    ));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldListTopics() throws Exception {
    // Then:
    assertThatEventually(() -> {
      try {
        return client.listTopics().get();
      } catch (final InterruptedException | ExecutionException e) {
        return null;
      }
    }, containsInAnyOrder(
        topicInfo(TEST_TOPIC),
        topicInfo(EMPTY_TEST_TOPIC),
        topicInfo(AGG_TABLE),
        topicInfo("connect-config")
    ));
  }

  @Test
  public void shouldListQueries() throws ExecutionException, InterruptedException {
    // When
    final List<QueryInfo> queries = client.listQueries().get();

    // Then
    assertThat(queries, hasItem(allOf(
            hasProperty("queryType", is(QueryType.PERSISTENT)),
            hasProperty("id", startsWith("CTAS_" + AGG_TABLE)),
            hasProperty("sql", is(
                    "CREATE TABLE " + AGG_TABLE + " WITH (KAFKA_TOPIC='" + AGG_TABLE + "', PARTITIONS=1, REPLICAS=1) AS SELECT\n"
                            + "  " + TEST_STREAM + ".K K,\n"
                            + "  LATEST_BY_OFFSET(" + TEST_STREAM + ".LONG) LONG\n"
                            + "FROM " + TEST_STREAM + " " + TEST_STREAM + "\n"
                            + "GROUP BY " + TEST_STREAM + ".K\n"
                            + "EMIT CHANGES;")),
            hasProperty("sink", is(Optional.of(AGG_TABLE))),
            hasProperty("sinkTopic", is(Optional.of(AGG_TABLE)))
    )));
  }

  @Test
  public void shouldDescribeSource() throws Exception {
    // When
    final SourceDescription description = client.describeSource(TEST_STREAM).get();

    // Then
    assertThat(description.name(), is(TEST_STREAM));
    assertThat(description.type(), is("STREAM"));
    assertThat(description.fields(), hasSize(TEST_COLUMN_NAMES.size()));
    for (int i = 0; i < TEST_COLUMN_NAMES.size(); i++) {
      assertThat(description.fields().get(i).name(), is(TEST_COLUMN_NAMES.get(i)));
      assertThat(description.fields().get(i).type().getType(), is(TEST_COLUMN_TYPES.get(i).getType()));
      final boolean isKey = TEST_COLUMN_NAMES.get(i).equals(TEST_DATA_PROVIDER.key());
      assertThat(description.fields().get(i).isKey(), is(isKey));
    }
    assertThat(description.topic(), is(TEST_TOPIC));
    assertThat(description.keyFormat(), is("JSON"));
    assertThat(description.valueFormat(), is("JSON"));
    assertThat(description.readQueries(), hasSize(1));
    assertThat(description.readQueries().get(0).getQueryType(), is(QueryType.PERSISTENT));
    assertThat(description.readQueries().get(0).getId(), startsWith("CTAS_" + AGG_TABLE));
    assertThat(description.readQueries().get(0).getSql(), is(
        "CREATE TABLE " + AGG_TABLE + " WITH (KAFKA_TOPIC='" + AGG_TABLE + "', PARTITIONS=1, REPLICAS=1) AS SELECT\n"
            + "  " + TEST_STREAM + ".K K,\n"
            + "  LATEST_BY_OFFSET(" + TEST_STREAM + ".LONG) LONG\n"
            + "FROM " + TEST_STREAM + " " + TEST_STREAM + "\n"
            + "GROUP BY " + TEST_STREAM + ".K\n"
            + "EMIT CHANGES;"));
    assertThat(description.readQueries().get(0).getSink(), is(Optional.of(AGG_TABLE)));
    assertThat(description.readQueries().get(0).getSinkTopic(), is(Optional.of(AGG_TABLE)));
    assertThat(description.writeQueries(), hasSize(0));
    assertThat(description.timestampColumn(), is(Optional.empty()));
    assertThat(description.windowType(), is(Optional.empty()));
    assertThat(description.sqlStatement(), is(
        "CREATE STREAM " + TEST_STREAM + " (K STRUCT<F1 ARRAY<STRING>> KEY, STR STRING, LONG BIGINT, DEC DECIMAL(4, 2), "
            + "BYTES_ BYTES, ARRAY ARRAY<STRING>, MAP MAP<STRING, STRING>, STRUCT STRUCT<F1 INTEGER>, "
            + "COMPLEX STRUCT<`DECIMAL` DECIMAL(2, 1), STRUCT STRUCT<F1 STRING, F2 INTEGER>, "
            + "ARRAY_ARRAY ARRAY<ARRAY<STRING>>, ARRAY_STRUCT ARRAY<STRUCT<F1 STRING>>, "
            + "ARRAY_MAP ARRAY<MAP<STRING, INTEGER>>, MAP_ARRAY MAP<STRING, ARRAY<STRING>>, "
            + "MAP_MAP MAP<STRING, MAP<STRING, INTEGER>>, MAP_STRUCT MAP<STRING, STRUCT<F1 STRING>>>, TIMESTAMP TIMESTAMP, DATE DATE, TIME TIME, HEAD BYTES HEADER('h0')) "
            + "WITH (KAFKA_TOPIC='STRUCTURED_TYPES_TOPIC', KEY_FORMAT='JSON', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldHandleErrorResponseFromDescribeSource() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.describeSource("NONEXISTENT").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Could not find STREAM/TABLE 'NONEXISTENT' in the Metastore"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 40001"));
  }

  @Test
  public void shouldGetServerInfo() throws Exception {
    // Given:
    final String expectedClusterId = REST_APP.getServiceContext().getAdminClient().describeCluster().clusterId().get();

    // When:
    final ServerInfo serverInfo = client.serverInfo().get();

    //Then:
    assertThat(serverInfo.getServerVersion(), is(AppInfo.getVersion()));
    assertThat(serverInfo.getKsqlServiceId(), is("default_"));
    assertThat(serverInfo.getKafkaClusterId(), is(expectedClusterId));
  }

  @Test
  public void shouldListConnectors() throws Exception {
    // Given:
    givenConnectorExists();

    // When:
    final List<ConnectorInfo> connectors = client.listConnectors().get();

    // Then:
    assertThat(connectors.size(), is(1));
    assertThat(connectors.get(0).name(), is(TEST_CONNECTOR));
    assertThat(connectors.get(0).className(), is(MOCK_SOURCE_CLASS));
    assertThat(connectors.get(0).state(), is("RUNNING (1/1 tasks RUNNING)"));
    assertThat(connectors.get(0).type(), is(SOURCE_TYPE));
  }

  @Test
  public void shouldDescribeConnector() throws Exception {
    // Given:
    givenConnectorExists();

    // When:
    final ConnectorDescription connector = client.describeConnector(TEST_CONNECTOR).get();

    // Then:
    assertThat(connector.type(), is(SOURCE_TYPE));
    assertThat(connector.state(), is("RUNNING"));
    assertThat(connector.topics().size(), is(0));
    assertThat(connector.sources().size(), is(0));
    assertThat(connector.className(), is(MOCK_SOURCE_CLASS));
  }

  @Test
  public void shouldDropConnector() throws Exception {
    // Given:
    givenConnectorExists();

    // When:
    client.dropConnector(TEST_CONNECTOR).get();

    // Then:
    assertThatEventually(() -> {
      try {
        return client.listConnectors().get().size();
      } catch (final InterruptedException | ExecutionException e) {
        return null;
      }
    }, is(0));
  }

  @Test
  public void shouldCreateConnector() throws Exception {
    // When:
    client.createConnector("FOO", true, ImmutableMap.of("connector.class", MOCK_SOURCE_CLASS)).get();

    // Then:
    assertThatEventually(
        () -> {
          try {
            return (client.describeConnector("FOO").get()).state();
          } catch (final InterruptedException | ExecutionException e) {
            return null;
          }
        },
        is("RUNNING")
    );
  }

  @Test
  public void shouldCreateConnectorWithVariables() throws Exception {
    // When:
    client.define("class", MOCK_SOURCE_CLASS);
    client.createConnector("FOO", true, ImmutableMap.of("connector.class", "${class}")).get();

    // Then:
    assertThatEventually(
        () -> {
          try {
            return (client.describeConnector("FOO").get()).state();
          } catch (final InterruptedException | ExecutionException e) {
            return null;
          }
        },
        is("RUNNING")
    );
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
  }

  private void givenConnectorExists() {
    // Make sure we are starting from a clean slate before creating a new connector.
    cleanupConnectors();

    makeKsqlRequest("CREATE SOURCE CONNECTOR " + TEST_CONNECTOR + " WITH ('connector.class'='" + MOCK_SOURCE_CLASS + "');");

    assertThatEventually(
        () -> {
          try {
            return ((ConnectorList) makeKsqlRequest("SHOW CONNECTORS;").get(0)).getConnectors().size();
          } catch (final AssertionError e) {
            return 0;
          }},
        is(1)
    );
  }

  private static void cleanupConnectors() {
    ((ConnectorList) makeKsqlRequest("SHOW CONNECTORS;").get(0)).getConnectors()
        .forEach(c -> makeKsqlRequest("DROP CONNECTOR " + c.getName() + ";"));
    assertThatEventually(
        () -> ((ConnectorList) makeKsqlRequest("SHOW CONNECTORS;")
            .get(0)).getConnectors().size(),
        is(0)
    );
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static void verifyNumActiveQueries(final int numQueries) {
    final KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    assertThatEventually(engine::numberOfLiveQueries, is(numQueries));
  }

  private static void shouldReceiveStreamRows(
      final Publisher<Row> publisher,
      final boolean subscriberCompleted
  ) {
    shouldReceiveStreamRows(publisher, subscriberCompleted, TEST_NUM_ROWS);
  }

  private static void shouldReceiveStreamRows(
      final Publisher<Row> publisher,
      final boolean subscriberCompleted,
      final int numRows
  ) {
    shouldReceiveRows(
        publisher,
        numRows,
        rows -> verifyStreamRows(rows, numRows),
        subscriberCompleted
    );
  }

  private static void verifyStreamRows(final List<Row> rows, final int numRows) {
    for (int i = 0; i < Math.min(numRows, rows.size()); i++) {
      verifyStreamRowWithIndex(rows.get(i), i);
    }
    if (rows.size() < numRows) {
      fail("Expected " + numRows + " but only got " + rows.size());
    } else if (rows.size() > numRows) {
      final List<Row> extra = rows.subList(numRows, rows.size());
      fail("Expected " + numRows + " but got " + rows.size() + ". The extra rows were: " + extra);
    }

    // not strictly necessary after the other checks, but just to specify the invariant
    assertThat(rows, hasSize(numRows));

  }

  private static void verifyStreamRowWithIndex(final Row row, final int index) {
    final KsqlArray expectedRow = TEST_EXPECTED_ROWS.get(index);

    // verify metadata
    assertThat(row.values(), equalTo(expectedRow));
    assertThat(row.columnNames(), equalTo(TEST_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(TEST_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getKsqlObject("K"), is(expectedRow.getKsqlObject(0)));
    assertThat(row.getString("STR"), is(expectedRow.getString(1)));
    assertThat(row.getLong("LONG"), is(expectedRow.getLong(2)));
    assertThat(row.getDecimal("DEC"), is(expectedRow.getDecimal(3)));
    assertThat(row.getBytes("BYTES_"), is(expectedRow.getBytes(4)));
    assertThat(row.getKsqlArray("ARRAY"), is(expectedRow.getKsqlArray(5)));
    assertThat(row.getKsqlObject("MAP"), is(expectedRow.getKsqlObject(6)));
    assertThat(row.getKsqlObject("STRUCT"), is(expectedRow.getKsqlObject(7)));
    assertThat(row.getKsqlObject("COMPLEX"), is(expectedRow.getKsqlObject(8)));

    // verify index-based getters are 1-indexed
    assertThat(row.getKsqlObject(1), is(row.getKsqlObject("K")));
    assertThat(row.getString(2), is(row.getString("STR")));
    assertThat(row.getLong(3), is(row.getLong("LONG")));
    assertThat(row.getDecimal(4), is(row.getDecimal("DEC")));
    assertThat(row.getBytes(5), is(row.getBytes("BYTES_")));
    assertThat(row.getKsqlArray(6), is(row.getKsqlArray("ARRAY")));
    assertThat(row.getKsqlObject(7), is(row.getKsqlObject("MAP")));
    assertThat(row.getKsqlObject(8), is(row.getKsqlObject("STRUCT")));
    assertThat(row.getKsqlObject(9), is(row.getKsqlObject("COMPLEX")));

    // verify isNull() evaluation
    assertThat(row.isNull("STR"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("STR"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getKsqlObject(0), is(row.getKsqlObject("K")));
    assertThat(values.getString(1), is(row.getString("STR")));
    assertThat(values.getLong(2), is(row.getLong("LONG")));
    assertThat(values.getDecimal(3), is(row.getDecimal("DEC")));
    assertThat(values.getBytes(4), is(row.getBytes("BYTES_")));
    assertThat(values.getKsqlArray(5), is(row.getKsqlArray("ARRAY")));
    assertThat(values.getKsqlObject(6), is(row.getKsqlObject("MAP")));
    assertThat(values.getKsqlObject(7), is(row.getKsqlObject("STRUCT")));
    assertThat(values.getKsqlObject(8), is(row.getKsqlObject("COMPLEX")));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(TEST_COLUMN_NAMES.toArray()));
    assertThat(obj.getKsqlObject("K"), is(row.getKsqlObject("K")));
    assertThat(obj.getString("STR"), is(row.getString("STR")));
    assertThat(obj.getLong("LONG"), is(row.getLong("LONG")));
    assertThat(obj.getDecimal("DEC"), is(row.getDecimal("DEC")));
    assertThat(obj.getBytes("BYTES_"), is(row.getBytes("BYTES_")));
    assertThat(obj.getKsqlArray("ARRAY"), is(row.getKsqlArray("ARRAY")));
    assertThat(obj.getKsqlObject("MAP"), is(row.getKsqlObject("MAP")));
    assertThat(obj.getKsqlObject("STRUCT"), is(row.getKsqlObject("STRUCT")));
    assertThat(obj.getKsqlObject("COMPLEX"), is(row.getKsqlObject("COMPLEX")));
    assertThat(obj.containsKey("DEC"), is(true));
    assertThat(obj.containsKey("notafield"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static void verifyPullQueryRows(final List<Row> rows) {
    assertThat(rows, hasSize(1));
    verifyPullQueryRow(rows.get(0));
  }

  private static void verifyPullQueryRow(final Row row) {
    // verify metadata
    assertThat(row.values(), equalTo(PULL_QUERY_EXPECTED_ROW));
    assertThat(row.columnNames(), equalTo(PULL_QUERY_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(PULL_QUERY_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getKsqlObject("K"), is(PULL_QUERY_EXPECTED_ROW.getKsqlObject(0)));
    assertThat(row.getLong("LONG"), is(PULL_QUERY_EXPECTED_ROW.getLong(1)));

    // verify index-based getters are 1-indexed
    assertThat(row.getKsqlObject(1), is(row.getKsqlObject("K")));
    assertThat(row.getLong(2), is(row.getLong("LONG")));

    // verify isNull() evaluation
    assertThat(row.isNull("K"), is(false));
    assertThat(row.isNull("LONG"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("K"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getKsqlObject(0), is(row.getKsqlObject("K")));
    assertThat(values.getLong(1), is(row.getLong("LONG")));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(PULL_QUERY_COLUMN_NAMES.toArray()));
    assertThat(obj.getKsqlObject("K"), is(row.getKsqlObject("K")));
    assertThat(obj.getLong("LONG"), is(row.getLong("LONG")));
    assertThat(obj.containsKey("LONG"), is(true));
    assertThat(obj.containsKey("notafield"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static List<KsqlArray> convertToClientRows(
      final Multimap<GenericKey, GenericRow> data
  ) {
    final List<KsqlArray> expectedRows = new ArrayList<>();
    for (final Map.Entry<GenericKey, GenericRow> entry : data.entries()) {
      final KsqlArray expectedRow = new KsqlArray();
      addObjectToKsqlArray(expectedRow, entry.getKey().get(0));

      for (final Object value : entry.getValue().values()) {
        addObjectToKsqlArray(expectedRow, value);
      }

      // Add header column
      expectedRow.add(new byte[] {23});

      expectedRows.add(expectedRow);
    }
    return expectedRows;
  }

  private static void addObjectToKsqlArray(final KsqlArray array, final Object value) {
    if (value instanceof Struct) {
      array.add(StructuredTypesDataProvider.structToMap((Struct) value));
    } else if (value instanceof BigDecimal) {
      // Can't use expectedRow.add((BigDecimal) value) directly since client serializes BigDecimal as string,
      // whereas this method builds up the expected result (unrelated to serialization)
      array.addAll(new KsqlArray(Collections.singletonList(value)));
    } else if (value instanceof Timestamp) {
      array.add(SqlTimeTypes.formatTimestamp((Timestamp) value));
    } else if (value instanceof Date) {
      array.add(SqlTimeTypes.formatDate((Date) value));
    } else if (value instanceof Time) {
      array.add(SqlTimeTypes.formatTime((Time) value));
    } else {
      array.add(value);
    }
  }

  private static Matcher<? super StreamInfo> streamForProvider(
      final TestDataProvider testDataProvider
  ) {
    return streamInfo(
        testDataProvider.sourceName(),
        testDataProvider.topicName(),
        KEY_FORMAT.name(),
        VALUE_FORMAT.name(),
        false
    );
  }

  private static Matcher<? super StreamInfo> streamInfo(
      final String streamName,
      final String topicName,
      final String keyFormat,
      final String valueFormat,
      final boolean isWindowed
  ) {
    return new TypeSafeDiagnosingMatcher<StreamInfo>() {
      @Override
      protected boolean matchesSafely(
          final StreamInfo actual,
          final Description mismatchDescription) {
        if (!streamName.equals(actual.getName())) {
          return false;
        }
        if (!topicName.equals(actual.getTopic())) {
          return false;
        }
        if (!keyFormat.equals(actual.getValueFormat())) {
          return false;
        }
        if (!valueFormat.equals(actual.getValueFormat())) {
          return false;
        }
        if (isWindowed != actual.isWindowed()) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(String.format(
            "streamName: %s. topicName: %s. keyFormat: %s. valueFormat: %s. isWindowed: %s",
            streamName, topicName, keyFormat, valueFormat, isWindowed));
      }
    };
  }

  private static Matcher<? super TableInfo> tableInfo(
      final String tableName,
      final String topicName,
      final String keyFormat,
      final String valueFormat,
      final boolean isWindowed
  ) {
    return new TypeSafeDiagnosingMatcher<TableInfo>() {
      @Override
      protected boolean matchesSafely(
          final TableInfo actual,
          final Description mismatchDescription) {
        if (!tableName.equals(actual.getName())) {
          return false;
        }
        if (!topicName.equals(actual.getTopic())) {
          return false;
        }
        if (!keyFormat.equals(actual.getKeyFormat())) {
          return false;
        }
        if (!valueFormat.equals(actual.getValueFormat())) {
          return false;
        }
        if (isWindowed != actual.isWindowed()) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(String.format(
            "tableName: %s. topicName: %s. keyFormat: %s. valueFormat: %s. isWindowed: %s",
            tableName, topicName, keyFormat, valueFormat, isWindowed));
      }
    };
  }

  // validates topics have 1 partition and 1 replica
  private static Matcher<? super TopicInfo> topicInfo(final String name) {
    return new TypeSafeDiagnosingMatcher<TopicInfo>() {
      @Override
      protected boolean matchesSafely(
          final TopicInfo actual,
          final Description mismatchDescription) {
        if (!name.equals(actual.getName())) {
          return false;
        }
        if (actual.getPartitions() != 1) {
          return false;
        }
        final List<Integer> replicasPerPartition = actual.getReplicasPerPartition();
        if (replicasPerPartition.size() != 1 || replicasPerPartition.get(0) != 1) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("name: " + name);
      }
    };
  }

}