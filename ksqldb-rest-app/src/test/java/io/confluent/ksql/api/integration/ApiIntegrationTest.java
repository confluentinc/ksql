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

package io.confluent.ksql.api.integration;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.utils.InsertsResponse;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.HashSet;
import java.util.Set;
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

@Category({IntegrationTest.class})
public class ApiIntegrationTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.kstreamName();

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final String AN_AGG_KEY = "USER_1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
      ).build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
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
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  private Vertx vertx;
  private WebClient client;

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
  public void shouldExecutePushQueryWithLimit() {

    // Given:
    String sql = "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + 2 + ";";

    // When:
    QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(2));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(
        new JsonArray().add("PAGEID").add("USERID").add("VIEWTIME")));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(
        new JsonArray().add("STRING").add("STRING").add("BIGINT")));
    assertThat(response.responseObject.getString("queryId"), is(notNullValue()));
  }

  @Test
  public void shouldFailPushQueryWithInvalidSql() {

    // Given:
    String sql = "SLECTT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "line 1:1: mismatched input 'SLECTT' expecting");
  }

  @Test
  public void shouldFailPushQueryWithMoreThanOneStatement() {

    // Given:
    String sql = "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES;" +
        "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "Expected exactly one KSQL statement; found 2 instead");
  }

  @Test
  public void shouldFailPushWithNonQuery() {

    // Given:
    String sql =
        "CREATE STREAM SOME_STREAM AS SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "Not a query");
  }

  @Test
  public void shouldExecutePushQueryNoLimit() throws Exception {

    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    // One persistent query for the agg table
    assertThatEventually(engine::numberOfLiveQueries, is(1));

    // Given:
    String sql = "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a query
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("sql", sql).put("properties", properties);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    client.post("/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(requestBody, responseFuture);

    assertThatEventually(engine::numberOfLiveQueries, is(2));

    // Wait for all rows in the response to arrive
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(7));

    // The response shouldn't have ended yet
    assertThat(writeStream.isEnded(), is(false));

    QueryResponse queryResponse = new QueryResponse(writeStream.getBody().toString());
    String queryId = queryResponse.responseObject.getString("queryId");

    // Now send another request to close the query
    JsonObject closeQueryRequestBody = new JsonObject().put("queryId", queryId);
    HttpResponse<Buffer> closeQueryResponse = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());
    assertThat(closeQueryResponse.statusCode(), is(200));

    // The response should now be ended
    assertThatEventually(writeStream::isEnded, is(true));
    HttpResponse<Void> response = responseFuture.get();
    assertThat(response.statusCode(), is(200));

    // Make sure it's cleaned up on the server
    assertThatEventually(engine::numberOfLiveQueries, is(1));
  }

  @Test
  public void shouldExecutePullQuery() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';";

    // When:
    // Maybe need to retry as populating agg table is async
    AtomicReference<QueryResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      QueryResponse queryResponse = executeQuery(sql);
      atomicReference.set(queryResponse);
      return queryResponse.rows;
    }, hasSize(1));

    QueryResponse response = atomicReference.get();

    // Then:
    JsonArray expectedColumnNames = new JsonArray().add("USERID").add("COUNT");
    JsonArray expectedColumnTypes = new JsonArray().add("STRING").add("BIGINT");
    assertThat(response.rows, hasSize(1));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(expectedColumnNames));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(expectedColumnTypes));
    assertThat(response.responseObject.getString("queryId"), is(nullValue()));
    assertThat(response.rows.get(0).getString(0), is("USER_1"));  // rowkey
    assertThat(response.rows.get(0).getLong(1), is(1L)); // count
  }

  @Test
  public void shouldFailPullQueryWithInvalidSql() {

    // Given:
    String sql = "SLLLECET * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';";

    // Then:
    shouldFailToExecuteQuery(sql, "line 1:1: mismatched input 'SLLLECET' expecting");
  }

  @Test
  public void shouldFailPullQueryWithMoreThanOneStatement() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';" +
        "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';";

    // Then:
    shouldFailToExecuteQuery(sql, "Expected exactly one KSQL statement; found 2 instead");
  }

  @Test
  public void shouldFailPullQueryWithNoWhereClause() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + ";";

    // Then:
    shouldFailToExecuteQuery(sql, "Missing WHERE clause.");
  }

  @Test
  public void shouldFailPullQueryWithNonKeyLookup() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE ROWTIME=12345;";

    // Then:
    shouldFailToExecuteQuery(sql, "WHERE clause on unsupported column: ROWTIME.");
  }

  @Test
  public void shouldExecuteInserts() {

    // Given:
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    int numRows = 10;

    for (int i = 0; i < numRows; i++) {
      JsonObject row = new JsonObject()
          .put("VIEWTIME", 1000 + i)
          .put("USERID", "User" + i % 3)
          .put("PAGEID", "PAGE" + (numRows - i));
      bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // When:
    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    // Then:
    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(numRows));
    assertThat(insertsResponse.error, is(nullValue()));

    Set<Long> sequences = new HashSet<>();
    for (JsonObject ack : insertsResponse.acks) {
      sequences.add(ack.getLong("seq"));
    }
    assertThat(sequences, hasSize(numRows));
    for (long l = 0; l < numRows; l++) {
      assertThat(sequences.contains(l), is(true));
    }
  }

  @Test
  public void shouldFailToInsertWithMissingKey() {

    // Given:
    JsonObject row = new JsonObject()
        .put("VIEWTIME", 1000)
        .put("USERID", "User123");

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Key field must be specified: PAGEID");
  }

  @Test
  public void shouldFailToInsertWithNonMatchingKeyType() {

    // Given:
    JsonObject row = new JsonObject()
        .put("PAGEID", true)
        .put("VIEWTIME", 1000)
        .put("USERID", "User123");

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Can't coerce a field of type class java.lang.Boolean (true) into type STRING");
  }

  @Test
  public void shouldFailToInsertWithNonMatchingValueType() {

    // Given:
    JsonObject row = new JsonObject()
        .put("VIEWTIME", 1000)
        .put("USERID", 123)
        .put("PAGEID", "PAGE23");

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Can't coerce a field of type class java.lang.Integer (123) into type STRING");
  }

  @Test
  public void shouldInsertWithMissingValueField() {

    // Given:
    JsonObject row = new JsonObject();
    row.put("PAGEID", "10");
    row.put("VIEWTIME", 1000);
    row.put("USERID", "User123");

    // Then:
    shouldInsert(row);
  }

  @Test
  public void shouldExecutePushQueryFromLatestOffset() {

    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    // One persistent query for the agg table
    assertThatEventually(engine::numberOfLiveQueries, is(1));

    // Given:
    String sql = "SELECT VIEWTIME, USERID, PAGEID from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT 1;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a query
    JsonObject queryProperties = new JsonObject().put("auto.offset.reset", "latest");
    JsonObject queryRequestBody = new JsonObject()
        .put("sql", sql).put("properties", queryProperties);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    client.post("/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(queryRequestBody, responseFuture);

    assertThatEventually(engine::numberOfLiveQueries, is(2));

    // New row to insert
    JsonObject row = new JsonObject()
        .put("VIEWTIME", 2000L)
        .put("USERID", "User_shouldExecutePushQueryFromLatestOffset")
        .put("PAGEID", "PAGE_shouldExecutePushQueryFromLatestOffset");

    // Insert a new row and wait for it to arrive
    assertThatEventually(() -> {
      try {
        shouldInsert(row); // Attempt the insert multiple times, in case the query hasn't started yet
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(1));

    // Verify that the received row is the expected one
    Buffer buff = writeStream.getBody();
    QueryResponse queryResponse = new QueryResponse(buff.toString());
    assertThat(queryResponse.rows.get(0).getLong(0), is(2000L));
    assertThat(queryResponse.rows.get(0).getString(1), is("User_shouldExecutePushQueryFromLatestOffset"));
    assertThat(queryResponse.rows.get(0).getString(2), is("PAGE_shouldExecutePushQueryFromLatestOffset"));

    // Check that query is cleaned up on the server
    assertThatEventually(engine::numberOfLiveQueries, is(1));
  }

  private void shouldFailToExecuteQuery(final String sql, final String message) {
    // When:
    QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(0));
    assertThat(response.responseObject.getInteger("error_code"),
        is(ERROR_CODE_BAD_STATEMENT));
    assertThat(response.responseObject.getString("message"),
        startsWith(message));
  }

  private QueryResponse executeQuery(final String sql) {
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("sql", sql).put("properties", properties);
    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());
    return new QueryResponse(response.bodyAsString());
  }

  private void shouldFailToInsert(final JsonObject row, final int errorCode, final String message) {
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(0));
    assertThat(insertsResponse.error, is(notNullValue()));
    assertThat(insertsResponse.error.getInteger("error_code"), is(errorCode));
    assertThat(insertsResponse.error.getString("message"),
        startsWith(message));
  }

  private void shouldInsert(final JsonObject row) {
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(1));
    assertThat(insertsResponse.error, is(nullValue()));
  }

  private WebClient createClient() {
    WebClientOptions options = new WebClientOptions().
        setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
        .setDefaultHost("localhost").setDefaultPort(REST_APP.getListeners().get(0).getPort());
    return WebClient.create(vertx, options);
  }

  private HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody) {
    return sendRequest(client, uri, requestBody);
  }

  private HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody) {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(uri)
        .sendBuffer(requestBody, requestFuture);
    try {
      return requestFuture.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }


}
