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
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.util.ClientTestUtil.TestSubscriber;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.PageViewDataProvider;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();
  private static final int PAGE_VIEW_NUM_ROWS = PAGE_VIEWS_PROVIDER.data().size();
  private static final List<String> PAGE_VIEW_COLUMN_NAMES =
      ImmutableList.of("PAGEID", "USERID", "VIEWTIME");
  private static final List<ColumnType> PAGE_VIEW_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "STRING", "BIGINT"));
  private static final List<KsqlArray> PAGE_VIEW_EXPECTED_ROWS = convertToClientRows(PAGE_VIEWS_PROVIDER.data());

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String AN_AGG_KEY = "USER_1";
  private static final PhysicalSchema AGG_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("COUNT"), SqlTypes.BIGINT)
          .build(),
      SerdeOption.none()
  );

  private static final String PUSH_QUERY = "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES;";
  private static final String PULL_QUERY = "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';";
  private static final String PUSH_QUERY_WITH_LIMIT =
      "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + PAGE_VIEW_NUM_ROWS + ";";

  private static final List<String> PULL_QUERY_COLUMN_NAMES = ImmutableList.of("USERID", "COUNT");
  private static final List<ColumnType> PULL_QUERY_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "BIGINT"));
  private static final KsqlArray PULL_QUERY_EXPECTED_ROW = new KsqlArray(ImmutableList.of("USER_1", 1));

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
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT USERID, COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        5, // Only unique keys are counted
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
    assertThat(streamedQueryResult.columnNames(), is(PAGE_VIEW_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PAGE_VIEW_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceivePageViewRows(streamedQueryResult, false);

    assertThat(streamedQueryResult.isComplete(), is(false));
  }

  @Test
  public void shouldStreamPushQuerySync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PAGE_VIEW_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PAGE_VIEW_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    for (int i = 0; i < PAGE_VIEW_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyPageViewRowWithIndex(row, i);
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
    assertThat(streamedQueryResult.columnNames(), is(PAGE_VIEW_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PAGE_VIEW_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceivePageViewRows(streamedQueryResult, true);

    assertThat(streamedQueryResult.isComplete(), is(true));
  }

  @Test
  public void shouldStreamPushQueryWithLimitSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(PAGE_VIEW_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(PAGE_VIEW_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    for (int i = 0; i < PAGE_VIEW_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyPageViewRowWithIndex(row, i);
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
    assertThat(e.getCause(), instanceOf(KsqlRestClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("NONEXISTENT does not exist"));
  }

  @Test
  public void shouldDeliverBufferedRowsViaPollIfComplete() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult = client.streamQuery(PUSH_QUERY_WITH_LIMIT).get();
    assertThatEventually(streamedQueryResult::isComplete, is(true));

    // When / Then
    for (int i = 0; i < PAGE_VIEW_NUM_ROWS; i++) {
      final Row row = streamedQueryResult.poll();
      verifyPageViewRowWithIndex(row, i);
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
    subscriber.getSub().request(PAGE_VIEW_NUM_ROWS);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(PAGE_VIEW_NUM_ROWS));
    verifyPageViewRows(subscriber.getValues());
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldExecutePullQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PULL_QUERY).get();

    // Then
    assertThat(batchedQueryResult.columnNames(), is(PULL_QUERY_COLUMN_NAMES));
    assertThat(batchedQueryResult.columnTypes(), is(PULL_QUERY_COLUMN_TYPES));
    assertThat(batchedQueryResult.queryID(), is(nullValue()));

    verifyPullQueryRows(batchedQueryResult.rows());
  }

  @Test
  public void shouldExecutePushWithLimitQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = client.executeQuery(PUSH_QUERY_WITH_LIMIT).get();

    // Then
    assertThat(batchedQueryResult.columnNames(), is(PAGE_VIEW_COLUMN_NAMES));
    assertThat(batchedQueryResult.columnTypes(), is(PAGE_VIEW_COLUMN_TYPES));
    assertThat(batchedQueryResult.queryID(), is(notNullValue()));

    verifyPageViewRows(batchedQueryResult.rows());
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteQuery() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.executeQuery("SELECT * from " + AGG_TABLE + ";").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlRestClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Missing WHERE clause"));
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
  public void shouldTerminatePushQueryIssuedViaExecuteQuery() {
    // Will implement once https://github.com/confluentinc/ksql/pull/5236#issuecomment-628997138
    // is resolved
  }

  @Test
  public void shouldHandleErrorResponseFromTerminatePushQuery() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.terminatePushQuery("NONEXISTENT").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlRestClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("No query with id NONEXISTENT"));
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort())
        .setUseTls(false);
    return Client.create(clientOptions, vertx);
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static void verifyNumActiveQueries(final int numQueries) {
    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    assertThatEventually(engine::numberOfLiveQueries, is(numQueries));
  }

  private static void shouldReceivePageViewRows(
      final Publisher<Row> publisher,
      final boolean subscriberCompleted
  ) {
    shouldReceiveRows(
        publisher,
        PAGE_VIEW_NUM_ROWS,
        ClientIntegrationTest::verifyPageViewRows,
        subscriberCompleted
    );
  }

  private static void verifyPageViewRows(final List<Row> rows) {
    assertThat(rows, hasSize(PAGE_VIEW_NUM_ROWS));
    for (int i = 0; i < PAGE_VIEW_NUM_ROWS; i++) {
      verifyPageViewRowWithIndex(rows.get(i), i);
    }
  }

  private static void verifyPageViewRowWithIndex(final Row row, final int index) {
    final KsqlArray expectedRow = PAGE_VIEW_EXPECTED_ROWS.get(index);

    // verify metadata
    assertThat(row.values(), equalTo(expectedRow));
    assertThat(row.columnNames(), equalTo(PAGE_VIEW_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(PAGE_VIEW_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getString("PAGEID"), is(expectedRow.getString(0)));
    assertThat(row.getString("USERID"), is(expectedRow.getString(1)));
    assertThat(row.getLong("VIEWTIME"), is(expectedRow.getLong(2)));

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("PAGEID")));
    assertThat(row.getString(2), is(row.getString("USERID")));
    assertThat(row.getLong(3), is(row.getLong("VIEWTIME")));

    // verify isNull() evaluation
    assertThat(row.isNull("PAGEID"), is(false));
    assertThat(row.isNull("VIEWTIME"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInt("PAGEID"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(PAGE_VIEW_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getString(0), is(row.getString("PAGEID")));
    assertThat(values.getString(1), is(row.getString("USERID")));
    assertThat(values.getLong(2), is(row.getLong("VIEWTIME")));
    assertThat(values.contains(row.getString("USERID")), is(true));
    assertThat(values.contains("bad"), is(false));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(PAGE_VIEW_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(PAGE_VIEW_COLUMN_NAMES.toArray()));
    assertThat(obj.getString("PAGEID"), is(row.getString("PAGEID")));
    assertThat(obj.getString("USERID"), is(row.getString("USERID")));
    assertThat(obj.getLong("VIEWTIME"), is(row.getLong("VIEWTIME")));
    assertThat(obj.containsKey("VIEWTIME"), is(true));
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
    assertThat(row.getString("USERID"), is(PULL_QUERY_EXPECTED_ROW.getString(0)));
    assertThat(row.getLong("COUNT"), is(PULL_QUERY_EXPECTED_ROW.getLong(1)));

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("USERID")));
    assertThat(row.getLong(2), is(row.getLong("COUNT")));

    // verify isNull() evaluation
    assertThat(row.isNull("USERID"), is(false));
    assertThat(row.isNull("COUNT"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInt("USERID"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getString(0), is(row.getString("USERID")));
    assertThat(values.getLong(1), is(row.getLong("COUNT")));
    assertThat(values.contains(row.getString("USERID")), is(true));
    assertThat(values.contains("bad"), is(false));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(PULL_QUERY_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(PULL_QUERY_COLUMN_NAMES.toArray()));
    assertThat(obj.getString("USERID"), is(row.getString("USERID")));
    assertThat(obj.getLong("COUNT"), is(row.getLong("COUNT")));
    assertThat(obj.containsKey("COUNT"), is(true));
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