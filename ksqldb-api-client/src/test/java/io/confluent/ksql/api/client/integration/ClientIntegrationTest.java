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
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlClientException;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
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
      ImmutableList.of("STR", "LONG", "DEC", "ARRAY", "MAP");
  private static final List<ColumnType> TEST_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "BIGINT", "DECIMAL", "ARRAY", "MAP"));
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

  private static final String PUSH_QUERY = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES;";
  private static final String PULL_QUERY = "SELECT * from " + AGG_TABLE + " WHERE STR='" + AN_AGG_KEY + "';";
  private static final int PUSH_QUERY_LIMIT_NUM_ROWS = 2;
  private static final String PUSH_QUERY_WITH_LIMIT =
      "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT " + PUSH_QUERY_LIMIT_NUM_ROWS + ";";

  private static final List<String> PULL_QUERY_COLUMN_NAMES = ImmutableList.of("STR", "LONG");
  private static final List<ColumnType> PULL_QUERY_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "BIGINT"));
  private static final KsqlArray PULL_QUERY_EXPECTED_ROW = new KsqlArray(ImmutableList.of("FOO", 1));

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
    TEST_HARNESS.ensureTopics(TEST_TOPIC);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT STR, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM + " GROUP BY STR;"
    );

    TEST_HARNESS.ensureTopics(EMPTY_TEST_TOPIC);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);

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
        .put("`LONG`", 100L) // Quotes may be used to preserve case-sensitivity
        .put("DEC", new BigDecimal("13.31"))
        .put("ARRAY", new KsqlArray().add("v1").add("v2"))
        .put("MAP", new KsqlObject().put("some_key", "a_value").put("another_key", ""));

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
        .put("MAP", new KsqlObject().put("test_name", "shouldStreamQueryWithProperties"));

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
        .put("MAP", new KsqlObject().put("test_name", "shouldExecuteQueryWithProperties"));

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
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
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

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("STR")));
    assertThat(row.getLong(2), is(row.getLong("LONG")));
    assertThat(row.getDecimal(3), is(row.getDecimal("DEC")));
    assertThat(row.getKsqlArray(4), is(row.getKsqlArray("ARRAY")));
    assertThat(row.getKsqlObject(5), is(row.getKsqlObject("MAP")));

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
          .add(entry.getKey())
          .addAll(new KsqlArray(entry.getValue().values()));
      expectedRows.add(expectedRow);
    }
    return expectedRows;
  }
}