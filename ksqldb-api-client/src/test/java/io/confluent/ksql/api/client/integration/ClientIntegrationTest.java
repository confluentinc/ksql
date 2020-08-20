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
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.api.client.util.ClientTestUtil.TestSubscriber;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsConfig;
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
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.kstreamName();
  private static final int TEST_NUM_ROWS = TEST_DATA_PROVIDER.data().size();
  private static final List<String> TEST_COLUMN_NAMES =
      ImmutableList.of("STR", "LONG", "DEC", "ARRAY", "MAP", "STRUCT", "COMPLEX");
  private static final List<ColumnType> TEST_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "BIGINT", "DECIMAL", "ARRAY", "MAP", "STRUCT", "STRUCT"));
  private static final List<KsqlArray> TEST_EXPECTED_ROWS = convertToClientRows(
      TEST_DATA_PROVIDER.data());

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String AN_AGG_KEY = "FOO";
  private static final PhysicalSchema AGG_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("STR"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final TestDataProvider<String> EMPTY_TEST_DATA_PROVIDER = new TestDataProvider<>(
      "EMPTY_STRUCTURED_TYPES", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
  private static final String EMPTY_TEST_TOPIC = EMPTY_TEST_DATA_PROVIDER.topicName();
  private static final String EMPTY_TEST_STREAM = EMPTY_TEST_DATA_PROVIDER.kstreamName();

  private static final TestDataProvider<String> EMPTY_TEST_DATA_PROVIDER_2 = new TestDataProvider<>(
      "EMPTY_STRUCTURED_TYPES_2", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
  private static final String EMPTY_TEST_TOPIC_2 = EMPTY_TEST_DATA_PROVIDER_2.topicName();
  private static final String EMPTY_TEST_STREAM_2 = EMPTY_TEST_DATA_PROVIDER_2.kstreamName();

  private static final String PUSH_QUERY = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES;";
  private static final String PULL_QUERY = "SELECT * from " + AGG_TABLE + " WHERE STR='" + AN_AGG_KEY + "';";
  private static final int PUSH_QUERY_LIMIT_NUM_ROWS = 2;
  private static final String PUSH_QUERY_WITH_LIMIT =
      "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT " + PUSH_QUERY_LIMIT_NUM_ROWS + ";";

  private static final List<String> PULL_QUERY_COLUMN_NAMES = ImmutableList.of("STR", "LONG");
  private static final List<ColumnType> PULL_QUERY_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "BIGINT"));
  private static final KsqlArray PULL_QUERY_EXPECTED_ROW = new KsqlArray(ImmutableList.of("FOO", 1));

  private static final KsqlObject COMPLEX_FIELD_VALUE = new KsqlObject()
      .put("DECIMAL", new BigDecimal("1.1"))
      .put("STRUCT", new KsqlObject().put("F1", "foo").put("F2", 3))
      .put("ARRAY_ARRAY", new KsqlArray().add(new KsqlArray().add("bar")))
      .put("ARRAY_STRUCT", new KsqlArray().add(new KsqlObject().put("F1", "x")))
      .put("ARRAY_MAP", new KsqlArray().add(new KsqlObject().put("k", 10)))
      .put("MAP_ARRAY", new KsqlObject().put("k", new KsqlArray().add("e1").add("e2")))
      .put("MAP_MAP", new KsqlObject().put("k1", new KsqlObject().put("k2", 5)))
      .put("MAP_STRUCT", new KsqlObject().put("k", new KsqlObject().put("F1", "baz")));
  private static final KsqlObject EXPECTED_COMPLEX_FIELD_VALUE = COMPLEX_FIELD_VALUE.copy()
      .put("DECIMAL", 1.1d); // Expect raw decimal value, whereas put(BigDecimal) serializes as string to avoid loss of precision

  protected static final String EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC =
      "The ksqlDB server accepted the statement issued via executeStatement(), but the response "
          + "received is of an unexpected format. ";
  protected static final String EXECUTE_STATEMENT_USAGE_DOC = "The executeStatement() method is only "
      + "for 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS "
      + "SELECT' statements. ";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(TEST_TOPIC, EMPTY_TEST_TOPIC, EMPTY_TEST_TOPIC_2);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER_2);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT STR, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM + " GROUP BY STR;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        4, // Only unique keys are counted
        FormatFactory.JSON,
        AGG_SCHEMA
    );
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
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
  public void shouldStreamPullQueryAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PULL_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PULL_QUERY_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PULL_QUERY_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(nullValue()));

    shouldReceivePullQueryRow(streamedQueryResult);

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPullQuerySync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PULL_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PULL_QUERY_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PULL_QUERY_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(nullValue()));

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
    TestSubscriber<Row> subscriber = subscribeAndWait(streamedQueryResult);
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
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PULL_QUERY);

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(nullValue()));

    verifyPullQueryRows(batchedQueryResult.get());
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
  public void shouldHandleErrorResponseFromExecuteQuery() {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery("SELECT * from " + AGG_TABLE + ";");
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        batchedQueryResult::get
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Missing WHERE clause"));

    // queryID future should also be completed exceptionally
    final Exception queryIdException = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> batchedQueryResult.queryID().get()
    );
    assertThat(queryIdException.getCause(), instanceOf(KsqlClientException.class));
    assertThat(queryIdException.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(queryIdException.getCause().getMessage(), containsString("Missing WHERE clause"));
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
  public void shouldInsertInto() throws Exception {
    // Given
    final KsqlObject insertRow = new KsqlObject()
        .put("str", "HELLO") // Column names are case-insensitive
        .put("`LONG`", 100L) // Backticks may be used to preserve case-sensitivity
        .put("\"DEC\"", new BigDecimal("13.31")) // Double quotes may also be used to preserve case-sensitivity
        .put("ARRAY", new KsqlArray().add("v1").add("v2"))
        .put("MAP", new KsqlObject().put("some_key", "a_value").put("another_key", ""))
        .put("STRUCT", new KsqlObject().put("f1", 12)) // Nested field names are case-insensitive
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // When
    client.insertInto(EMPTY_TEST_STREAM.toLowerCase(), insertRow).get(); // Stream name is case-insensitive

    // Then: should receive new row
    final String query = "SELECT * FROM " + EMPTY_TEST_STREAM + " EMIT CHANGES LIMIT 1;";
    final List<Row> rows = client.executeQuery(query).get();

    // Verify inserted row is as expected
    assertThat(rows, hasSize(1));
    assertThat(rows.get(0).getString("STR"), is("HELLO"));
    assertThat(rows.get(0).getLong("LONG"), is(100L));
    assertThat(rows.get(0).getDecimal("DEC"), is(new BigDecimal("13.31")));
    assertThat(rows.get(0).getKsqlArray("ARRAY"), is(new KsqlArray().add("v1").add("v2")));
    assertThat(rows.get(0).getKsqlObject("MAP"), is(new KsqlObject().put("some_key", "a_value").put("another_key", "")));
    assertThat(rows.get(0).getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 12)));
    assertThat(rows.get(0).getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
  }

  @Test
  public void shouldHandleErrorResponseFromInsertInto() {
    // Given
    final KsqlObject insertRow = new KsqlObject()
        .put("STR", "BLAH")
        .put("LONG", 11L);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.insertInto(AGG_TABLE, insertRow).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Cannot insert into a table"));
  }

  @Test
  public void shouldStreamQueryWithProperties() throws Exception {
    // Given
    final Map<String, Object> properties = new HashMap<>();
    properties.put("auto.offset.reset", "latest");
    final String sql = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT 1;";

    final KsqlObject insertRow = new KsqlObject()
        .put("STR", "Value_shouldStreamQueryWithProperties")
        .put("LONG", 2000L)
        .put("DEC", new BigDecimal("12.34"))
        .put("ARRAY", new KsqlArray().add("v1_shouldStreamQueryWithProperties").add("v2_shouldStreamQueryWithProperties"))
        .put("MAP", new KsqlObject().put("test_name", "shouldStreamQueryWithProperties"))
        .put("STRUCT", new KsqlObject().put("F1", 4))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // When
    final StreamedQueryResult queryResult = client.streamQuery(sql, properties).get();

    // Then: a newly inserted row arrives
    final Row row = assertThatEventually(() -> {
      // Potentially try inserting multiple times, in case the query wasn't started by the first time
      try {
        client.insertInto(TEST_STREAM, insertRow).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return queryResult.poll(Duration.ofMillis(10));
    }, is(notNullValue()));

    assertThat(row.getString("STR"), is("Value_shouldStreamQueryWithProperties"));
    assertThat(row.getLong("LONG"), is(2000L));
    assertThat(row.getDecimal("DEC"), is(new BigDecimal("12.34")));
    assertThat(row.getKsqlArray("ARRAY"), is(new KsqlArray().add("v1_shouldStreamQueryWithProperties").add("v2_shouldStreamQueryWithProperties")));
    assertThat(row.getKsqlObject("MAP"), is(new KsqlObject().put("test_name", "shouldStreamQueryWithProperties")));
    assertThat(row.getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 4)));
    assertThat(row.getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
  }

  @Test
  public void shouldExecuteQueryWithProperties() {
    // Given
    final Map<String, Object> properties = new HashMap<>();
    properties.put("auto.offset.reset", "latest");
    final String sql = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT 1;";

    final KsqlObject insertRow = new KsqlObject()
        .put("STR", "Value_shouldExecuteQueryWithProperties")
        .put("LONG", 2000L)
        .put("DEC", new BigDecimal("12.34"))
        .put("ARRAY", new KsqlArray().add("v1_shouldExecuteQueryWithProperties").add("v2_shouldExecuteQueryWithProperties"))
        .put("MAP", new KsqlObject().put("test_name", "shouldExecuteQueryWithProperties"))
        .put("STRUCT", new KsqlObject().put("F1", 4))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // When
    final BatchedQueryResult queryResult = client.executeQuery(sql, properties);

    // Then: a newly inserted row arrives

    // Wait for row to arrive
    final AtomicReference<Row> rowRef = new AtomicReference<>();
    new Thread(() -> {
      try {
        final List<Row> rows = queryResult.get();
        assertThat(rows, hasSize(1));
        rowRef.set(rows.get(0));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).start();

    // Insert a new row
    final Row row = assertThatEventually(() -> {
      // Potentially try inserting multiple times, in case the query wasn't started by the first time
      try {
        client.insertInto(TEST_STREAM, insertRow).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return rowRef.get();
    }, is(notNullValue()));

    // Verify received row
    assertThat(row.getString("STR"), is("Value_shouldExecuteQueryWithProperties"));
    assertThat(row.getLong("LONG"), is(2000L));
    assertThat(row.getDecimal("DEC"), is(new BigDecimal("12.34")));
    assertThat(row.getKsqlArray("ARRAY"), is(new KsqlArray().add("v1_shouldExecuteQueryWithProperties").add("v2_shouldExecuteQueryWithProperties")));
    assertThat(row.getKsqlObject("MAP"), is(new KsqlObject().put("test_name", "shouldExecuteQueryWithProperties")));
    assertThat(row.getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 4)));
    assertThat(row.getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
  }

  @Test
  public void shouldStreamInserts() throws Exception {
    // Given
    final InsertsPublisher insertsPublisher = new InsertsPublisher();
    final int numRows = 5;

    // When
    final AcksPublisher acksPublisher = client.streamInserts(EMPTY_TEST_STREAM_2, insertsPublisher).get();

    TestSubscriber<InsertAck> acksSubscriber = subscribeAndWait(acksPublisher);
    assertThat(acksSubscriber.getValues(), hasSize(0));
    acksSubscriber.getSub().request(numRows);

    for (int i = 0; i < numRows; i++) {
      insertsPublisher.accept(new KsqlObject()
          .put("STR", "TEST_" + i)
          .put("LONG", i)
          .put("DEC", new BigDecimal("13.31"))
          .put("ARRAY", new KsqlArray().add("v_" + i))
          .put("MAP", new KsqlObject().put("k_" + i, "v_" + i))
          .put("COMPLEX", COMPLEX_FIELD_VALUE));
    }

    // Then
    assertThatEventually(acksSubscriber::getValues, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      assertThat(acksSubscriber.getValues().get(i).seqNum(), is(Long.valueOf(i)));
    }
    assertThat(acksSubscriber.getError(), is(nullValue()));
    assertThat(acksSubscriber.isCompleted(), is(false));

    assertThat(acksPublisher.isComplete(), is(false));
    assertThat(acksPublisher.isFailed(), is(false));

    // Then: should receive new rows
    final String query = "SELECT * FROM " + EMPTY_TEST_STREAM_2 + " EMIT CHANGES LIMIT " + numRows + ";";
    final List<Row> rows = client.executeQuery(query).get();

    // Verify inserted rows are as expected
    assertThat(rows, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      assertThat(rows.get(i).getString("STR"), is("TEST_" + i));
      assertThat(rows.get(i).getLong("LONG"), is(Long.valueOf(i)));
      assertThat(rows.get(i).getDecimal("DEC"), is(new BigDecimal("13.31")));
      assertThat(rows.get(i).getKsqlArray("ARRAY"), is(new KsqlArray().add("v_" + i)));
      assertThat(rows.get(i).getKsqlObject("MAP"), is(new KsqlObject().put("k_" + i, "v_" + i)));
      assertThat(rows.get(i).getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
    }

    // When: end connection
    insertsPublisher.complete();

    // Then
    assertThatEventually(acksSubscriber::isCompleted, is(true));
    assertThat(acksSubscriber.getError(), is(nullValue()));

    assertThat(acksPublisher.isComplete(), is(true));
    assertThat(acksPublisher.isFailed(), is(false));
  }

  @Test
  public void shouldHandleErrorResponseFromStreamInserts() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.streamInserts(AGG_TABLE, new InsertsPublisher()).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Cannot insert into a table"));
  }

  @Test
  public void shouldExecuteDdlDmlStatements() throws Exception {
    // Given
    final String streamName = TEST_STREAM + "_COPY";
    final String csas = "create stream " + streamName + " as select * from " + TEST_STREAM + " emit changes;";

    final int numInitialStreams = 3;
    final int numInitialQueries = 1;
    verifyNumStreams(numInitialStreams);
    verifyNumQueries(numInitialQueries);

    // When: create stream, start persistent query
    final ExecuteStatementResult csasResult = client.executeStatement(csas).get();

    // Then
    verifyNumStreams(numInitialStreams + 1);
    verifyNumQueries(numInitialQueries + 1);
    assertThat(csasResult.queryId(), is(Optional.of(findQueryIdForSink(streamName))));

    // When: terminate persistent query
    final String queryId = csasResult.queryId().get();
    final ExecuteStatementResult terminateResult =
        client.executeStatement("terminate " + queryId + ";").get();

    // Then
    verifyNumQueries(numInitialQueries);
    assertThat(terminateResult.queryId(), is(Optional.empty()));

    // When: drop stream
    final ExecuteStatementResult dropStreamResult =
        client.executeStatement("drop stream " + streamName + ";").get();

    // Then
    verifyNumStreams(numInitialStreams);
    assertThat(dropStreamResult.queryId(), is(Optional.empty()));
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
    assertThat(e.getCause().getMessage(), containsString("Source NONEXISTENT does not exist"));
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
        streamForProvider(EMPTY_TEST_DATA_PROVIDER),
        streamForProvider(EMPTY_TEST_DATA_PROVIDER_2)
    ));
  }

  @Test
  public void shouldListTables() throws Exception {
    // When
    final List<TableInfo> tables = client.listTables().get();

    // Then
    assertThat("" + tables, tables, contains(tableInfo(AGG_TABLE, AGG_TABLE, "JSON", false)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldListTopics() throws Exception {
    // When
    final List<TopicInfo> topics = client.listTopics().get();

    // Then
    assertThat("" + topics, topics, containsInAnyOrder(
        topicInfo(TEST_TOPIC),
        topicInfo(EMPTY_TEST_TOPIC),
        topicInfo(EMPTY_TEST_TOPIC_2),
        topicInfo(AGG_TABLE)
    ));
  }

  @Test
  public void shouldListQueries() {
    // When
    // Try multiple times to allow time for queries started by the other tests to finish terminating
    final List<QueryInfo> queries = assertThatEventually(() -> {
      try {
        return client.listQueries().get();
      } catch (Exception e) {
        return Collections.emptyList();
      }
    }, hasSize(1));

    // Then
    assertThat(queries.get(0).getQueryType(), is(QueryType.PERSISTENT));
    assertThat(queries.get(0).getId(), is("CTAS_" + AGG_TABLE + "_0"));
    assertThat(queries.get(0).getSql(), is(
        "CREATE TABLE " + AGG_TABLE + " WITH (KAFKA_TOPIC='" + AGG_TABLE + "', PARTITIONS=1, REPLICAS=1) AS SELECT\n"
            + "  " + TEST_STREAM + ".STR STR,\n"
            + "  LATEST_BY_OFFSET(" + TEST_STREAM + ".LONG) LONG\n"
            + "FROM " + TEST_STREAM + " " + TEST_STREAM + "\n"
            + "GROUP BY " + TEST_STREAM + ".STR\n"
            + "EMIT CHANGES;"));
    assertThat(queries.get(0).getSink(), is(Optional.of(AGG_TABLE)));
    assertThat(queries.get(0).getSinkTopic(), is(Optional.of(AGG_TABLE)));
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
    assertThat(description.keyFormat(), is("KAFKA"));
    assertThat(description.valueFormat(), is("JSON"));
    assertThat(description.readQueries(), hasSize(1));
    assertThat(description.readQueries().get(0).getQueryType(), is(QueryType.PERSISTENT));
    assertThat(description.readQueries().get(0).getId(), is("CTAS_" + AGG_TABLE + "_0"));
    assertThat(description.readQueries().get(0).getSql(), is(
        "CREATE TABLE " + AGG_TABLE + " WITH (KAFKA_TOPIC='" + AGG_TABLE + "', PARTITIONS=1, REPLICAS=1) AS SELECT\n"
            + "  " + TEST_STREAM + ".STR STR,\n"
            + "  LATEST_BY_OFFSET(" + TEST_STREAM + ".LONG) LONG\n"
            + "FROM " + TEST_STREAM + " " + TEST_STREAM + "\n"
            + "GROUP BY " + TEST_STREAM + ".STR\n"
            + "EMIT CHANGES;"));
    assertThat(description.readQueries().get(0).getSink(), is(Optional.of(AGG_TABLE)));
    assertThat(description.readQueries().get(0).getSinkTopic(), is(Optional.of(AGG_TABLE)));
    assertThat(description.writeQueries(), hasSize(0));
    assertThat(description.timestampColumn(), is(Optional.empty()));
    assertThat(description.windowType(), is(Optional.empty()));
    assertThat(description.sqlStatement(), is(
        "CREATE STREAM " + TEST_STREAM + " (`STR` STRING KEY, `LONG` BIGINT, "
            + "`DEC` DECIMAL(4, 2), `ARRAY` ARRAY<STRING>, `MAP` MAP<STRING, STRING>, "
            + "`STRUCT` STRUCT<`F1` INTEGER>, `COMPLEX` STRUCT<`DECIMAL` DECIMAL(2, 1), "
            + "`STRUCT` STRUCT<`F1` STRING, `F2` INTEGER>, `ARRAY_ARRAY` ARRAY<ARRAY<STRING>>, "
            + "`ARRAY_STRUCT` ARRAY<STRUCT<`F1` STRING>>, `ARRAY_MAP` ARRAY<MAP<STRING, INTEGER>>, "
            + "`MAP_ARRAY` MAP<STRING, ARRAY<STRING>>, `MAP_MAP` MAP<STRING, MAP<STRING, INTEGER>>, "
            + "`MAP_STRUCT` MAP<STRING, STRUCT<`F1` STRING>>>) WITH "
            + "(kafka_topic='" + TEST_TOPIC + "', value_format='json');"));
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

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
  }

  private void verifyNumStreams(final int numStreams) {
    assertThatEventually(() -> {
      try {
        return client.listStreams().get().size();
      } catch (Exception e) {
        return -1;
      }
    }, is(numStreams));
  }

  private void verifyNumQueries(final int numQueries) {
    assertThatEventually(() -> {
      try {
        return client.listQueries().get().size();
      } catch (Exception e) {
        return -1;
      }
    }, is(numQueries));
  }

  private String findQueryIdForSink(final String sinkName) throws Exception {
    final List<String> queryIds = client.listQueries().get().stream()
        .filter(q -> q.getSink().equals(Optional.of(sinkName)))
        .map(QueryInfo::getId)
        .collect(Collectors.toList());
    assertThat(queryIds, hasSize(1));
    return queryIds.get(0);
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static void verifyNumActiveQueries(final int numQueries) {
    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
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
    assertThat(rows, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      verifyStreamRowWithIndex(rows.get(i), i);
    }
  }

  private static void verifyStreamRowWithIndex(final Row row, final int index) {
    final KsqlArray expectedRow = TEST_EXPECTED_ROWS.get(index);

    // verify metadata
    assertThat(row.values(), equalTo(expectedRow));
    assertThat(row.columnNames(), equalTo(TEST_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(TEST_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getString("STR"), is(expectedRow.getString(0)));
    assertThat(row.getLong("LONG"), is(expectedRow.getLong(1)));
    assertThat(row.getDecimal("DEC"), is(expectedRow.getDecimal(2)));
    assertThat(row.getKsqlArray("ARRAY"), is(expectedRow.getKsqlArray(3)));
    assertThat(row.getKsqlObject("MAP"), is(expectedRow.getKsqlObject(4)));
    assertThat(row.getKsqlObject("STRUCT"), is(expectedRow.getKsqlObject(5)));
    assertThat(row.getKsqlObject("COMPLEX"), is(expectedRow.getKsqlObject(6)));

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("STR")));
    assertThat(row.getLong(2), is(row.getLong("LONG")));
    assertThat(row.getDecimal(3), is(row.getDecimal("DEC")));
    assertThat(row.getKsqlArray(4), is(row.getKsqlArray("ARRAY")));
    assertThat(row.getKsqlObject(5), is(row.getKsqlObject("MAP")));
    assertThat(row.getKsqlObject(6), is(row.getKsqlObject("STRUCT")));
    assertThat(row.getKsqlObject(7), is(row.getKsqlObject("COMPLEX")));

    // verify isNull() evaluation
    assertThat(row.isNull("STR"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("STR"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getString(0), is(row.getString("STR")));
    assertThat(values.getLong(1), is(row.getLong("LONG")));
    assertThat(values.getDecimal(2), is(row.getDecimal("DEC")));
    assertThat(values.getKsqlArray(3), is(row.getKsqlArray("ARRAY")));
    assertThat(values.getKsqlObject(4), is(row.getKsqlObject("MAP")));
    assertThat(values.getKsqlObject(5), is(row.getKsqlObject("STRUCT")));
    assertThat(values.getKsqlObject(6), is(row.getKsqlObject("COMPLEX")));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(TEST_COLUMN_NAMES.toArray()));
    assertThat(obj.getString("STR"), is(row.getString("STR")));
    assertThat(obj.getLong("LONG"), is(row.getLong("LONG")));
    assertThat(obj.getDecimal("DEC"), is(row.getDecimal("DEC")));
    assertThat(obj.getKsqlArray("ARRAY"), is(row.getKsqlArray("ARRAY")));
    assertThat(obj.getKsqlObject("MAP"), is(row.getKsqlObject("MAP")));
    assertThat(obj.getKsqlObject("STRUCT"), is(row.getKsqlObject("STRUCT")));
    assertThat(obj.getKsqlObject("COMPLEX"), is(row.getKsqlObject("COMPLEX")));
    assertThat(obj.containsKey("DEC"), is(true));
    assertThat(obj.containsKey("notafield"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static void shouldReceivePullQueryRow(final Publisher<Row> publisher) {
    shouldReceiveRows(
        publisher,
        1,
        ClientIntegrationTest::verifyPullQueryRows,
        true
    );
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
    assertThat(row.getString("STR"), is(PULL_QUERY_EXPECTED_ROW.getString(0)));
    assertThat(row.getLong("LONG"), is(PULL_QUERY_EXPECTED_ROW.getLong(1)));

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("STR")));
    assertThat(row.getLong(2), is(row.getLong("LONG")));

    // verify isNull() evaluation
    assertThat(row.isNull("STR"), is(false));
    assertThat(row.isNull("LONG"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("STR"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getString(0), is(row.getString("STR")));
    assertThat(values.getLong(1), is(row.getLong("LONG")));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(PULL_QUERY_COLUMN_NAMES.toArray()));
    assertThat(obj.getString("STR"), is(row.getString("STR")));
    assertThat(obj.getLong("LONG"), is(row.getLong("LONG")));
    assertThat(obj.containsKey("LONG"), is(true));
    assertThat(obj.containsKey("notafield"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static List<KsqlArray> convertToClientRows(final Multimap<String, GenericRow> data) {
    final List<KsqlArray> expectedRows = new ArrayList<>();
    for (final Map.Entry<String, GenericRow> entry : data.entries()) {
      final KsqlArray expectedRow = new KsqlArray()
          .add(entry.getKey());
      for (final Object value : entry.getValue().values()) {
        if (value instanceof Struct) {
          expectedRow.add(StructuredTypesDataProvider.structToMap((Struct) value));
        } else if (value instanceof BigDecimal) {
          // Can't use expectedRow.add((BigDecimal) value) directly since client serializes BigDecimal as string,
          // whereas this method builds up the expected result (unrelated to serialization)
          expectedRow.addAll(new KsqlArray(Collections.singletonList(value)));
        } else {
          expectedRow.add(value);
        }
      }
      expectedRows.add(expectedRow);
    }
    return expectedRows;
  }

  private static Matcher<? super StreamInfo> streamForProvider(
      final TestDataProvider<?> testDataProvider
  ) {
    return streamInfo(testDataProvider.kstreamName(), testDataProvider.topicName(), "JSON");
  }

  private static Matcher<? super StreamInfo> streamInfo(
      final String streamName, final String topicName, final String format
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
        if (!format.equals(actual.getFormat())) {
          return false;
        }
        return true;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(String.format(
            "streamName: %s. topicName: %s. format: %s", streamName, topicName, format));
      }
    };
  }

  private static Matcher<? super TableInfo> tableInfo(
      final String tableName, final String topicName, final String format, final boolean isWindowed
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
        if (!format.equals(actual.getFormat())) {
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
            "tableName: %s. topicName: %s. format: %s. isWindowed: %s",
            tableName, topicName, format, isWindowed));
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