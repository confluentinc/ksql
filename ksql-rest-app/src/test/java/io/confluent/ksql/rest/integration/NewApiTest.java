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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.api.utils.TestUtils.findFilePath;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.impl.VertxCompletableFuture;
import io.confluent.ksql.api.server.ErrorCodes;
import io.confluent.ksql.api.utils.InsertsResponse;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.PageViewDataProvider;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class NewApiTest {

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
      .withProperty("ksql.new.api.enabled", true)
      .withProperty("ksql.apiserver.listen.host", "localhost")
      .withProperty("ksql.apiserver.listen.port", 8089)
      .withProperty("ksql.apiserver.key.path", findFilePath("test-server-key.pem"))
      .withProperty("ksql.apiserver.cert.path", findFilePath("test-server-cert.pem"))
      .withProperty("ksql.apiserver.verticle.instances", 4)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;"
    );
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

  private JsonArray expectedColumnNames = new JsonArray().add("ROWTIME").add("ROWKEY")
      .add("VIEWTIME").add("USERID").add("PAGEID");
  private JsonArray expectedColumnTypes = new JsonArray().add("BIGINT").add("BIGINT")
      .add("BIGINT").add("STRING").add("STRING");

  @Test
  public void shouldExecutePushQueryWithLimit() {

    // Given:
    String sql = "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + 2 + ";";

    // When:
    QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(2));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(expectedColumnNames));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(expectedColumnTypes));
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
    client.post(8089, "localhost", "/query-stream")
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
    String sql = "SELECT * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';";

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
    JsonArray expectedColumnNames = new JsonArray().add("ROWKEY").add("ROWTIME").add("COUNT");
    JsonArray expectedColumnTypes = new JsonArray().add("STRING").add("BIGINT").add("BIGINT");
    assertThat(response.rows, hasSize(1));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(expectedColumnNames));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(expectedColumnTypes));
    assertThat(response.responseObject.getString("queryId"), is(notNullValue()));
    assertThat(response.rows.get(0).getString(0), is("USER_1"));  // rowkey
    assertThat(response.rows.get(0).getLong(1), is(notNullValue()));  // rowtime - non deterministic
    assertThat(response.rows.get(0).getLong(2), is(1L)); // count
  }

  @Test
  public void shouldFailPullQueryWithInvalidSql() {

    // Given:
    String sql = "SLLLECET * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';";

    // Then:
    shouldFailToExecuteQuery(sql, "line 1:1: mismatched input 'SLLLECET' expecting");
  }

  @Test
  public void shouldFailPullQueryWithMoreThanOneStatement() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';" +
        "SELECT * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';";

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
    shouldFailToExecuteQuery(sql, "WHERE clause on unsupported field: ROWTIME.");
  }

  @Test
  public void shouldExecuteInserts() {

    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    int numRows = 10;

    for (int i = 0; i < numRows; i++) {
      JsonObject row = new JsonObject();
      row.put("ROWKEY", 10 + i);
      row.put("VIEWTIME", 1000 + i);
      row.put("USERID", "User" + i % 3);
      row.put("PAGEID", "PAGE" + (numRows - i));
      bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");
    }

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

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
  public void shouldFailToInsertWithNullKey() {

    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    JsonObject row = new JsonObject();
    row.put("VIEWTIME", 1000);
    row.put("USERID", "User123");
    row.put("PAGEID", "PAGE23");
    bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(0));
    assertThat(insertsResponse.error, is(notNullValue()));
    assertThat(insertsResponse.error.getString("status"), is("error"));
    assertThat(insertsResponse.error.getInteger("errorCode"), is(
        ErrorCodes.ERROR_CODE_MISSING_KEY_FIELD));
    assertThat(insertsResponse.error.getString("message"),
        startsWith("Key field must be specified: ROWKEY"));
  }

  @Test
  public void shouldInsertWithMissingValueField() {

    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", PAGE_VIEW_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    JsonObject row = new JsonObject();
    row.put("ROWKEY", 10);
    row.put("VIEWTIME", 1000);
    row.put("USERID", "User123");
    bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(1));
    assertThat(insertsResponse.error, is(nullValue()));
  }

  private void shouldFailToExecuteQuery(final String sql, final String message) {
    // When:
    QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(0));
    assertThat(response.responseObject.getString("status"), is("error"));
    assertThat(response.responseObject.getInteger("errorCode"),
        is(ErrorCodes.ERROR_CODE_INVALID_QUERY));
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

  private WebClient createClient() {
    WebClientOptions options = new WebClientOptions().setSsl(true).
        setUseAlpn(true).
        setProtocolVersion(HttpVersion.HTTP_2).
        setTrustAll(true);

    return WebClient.create(vertx, options);
  }

  private HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody) {
    return sendRequest(client, uri, requestBody);
  }

  private HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody) {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", uri)
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
