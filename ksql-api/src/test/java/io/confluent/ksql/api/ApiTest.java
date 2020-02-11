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

package io.confluent.ksql.api;

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_INTERNAL_ERROR;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_INVALID_QUERY;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MALFORMED_REQUEST;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MISSING_PARAM;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_UNKNOWN_PARAM;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_UNKNOWN_QUERY_ID;
import static io.confluent.ksql.api.utils.TestUtils.findFilePath;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.impl.VertxCompletableFuture;
import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.PushQueryId;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.utils.InsertsResponse;
import io.confluent.ksql.api.utils.ListRowGenerator;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.api.utils.SendStream;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiTest {

  private static final JsonArray DEFAULT_COLUMN_NAMES = new JsonArray().add("name").add("age")
      .add("male");
  private static final JsonArray DEFAULT_COLUMN_TYPES = new JsonArray().add("STRING").add("INT")
      .add("BOOLEAN");
  private static final List<JsonArray> DEFAULT_ROWS = generateRows();
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES = new JsonObject()
      .put("prop1", "val1").put("prop2", 23);
  private static final String DEFAULT_PULL_QUERY = "select * from foo where rowkey='1234';";
  private static final String DEFAULT_PUSH_QUERY = "select * from foo emit changes;";
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", DEFAULT_PUSH_QUERY)
      .put("properties", DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES);

  private Vertx vertx;
  private Server server;
  private TestEndpoints testEndpoints;
  private WebClient client;

  @Before
  public void setUp() {

    vertx = Vertx.vertx();

    JsonObject config = new JsonObject()
        .put("ksql.apiserver.listen.host", "localhost")
        .put("ksql.apiserver.listen.port", 8089)
        .put("ksql.apiserver.key.path", findFilePath("test-server-key.pem"))
        .put("ksql.apiserver.cert.path", findFilePath("test-server-cert.pem"))
        .put("ksql.apiserver.verticle.instances", 4);

    testEndpoints = new TestEndpoints(vertx);
    server = new Server(vertx, new ApiServerConfig(config), testEndpoints);
    server.start();
    client = createClient();
    setDefaultRowGenerator();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void shouldExecutePullQuery() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY);
    JsonObject properties = new JsonObject().put("prop1", "val1").put("prop2", 23);
    requestBody.put("properties", properties);

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));
    assertThat(testEndpoints.getLastSql(), is(DEFAULT_PULL_QUERY));
    assertThat(testEndpoints.getLastProperties(), is(properties));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertThat(queryResponse.responseObject.getJsonArray("columnNames"), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResponse.responseObject.getJsonArray("columnTypes"), is(DEFAULT_COLUMN_TYPES));
    assertThat(queryResponse.rows, is(DEFAULT_ROWS));
    assertThat(server.getQueryIDs(), hasSize(0));
    String queryId = queryResponse.responseObject.getString("queryId");
    assertThat(queryId, is(notNullValue()));
    assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(false));
  }

  @Test
  public void shouldExecutePushQuery() throws Exception {

    // When
    QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);

    // Then
    assertThat(testEndpoints.getLastSql(), is(DEFAULT_PUSH_QUERY));
    assertThat(testEndpoints.getLastProperties(), is(DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES));
    assertThat(queryResponse.responseObject.getJsonArray("columnNames"), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResponse.responseObject.getJsonArray("columnTypes"), is(DEFAULT_COLUMN_TYPES));
    assertThat(queryResponse.rows, is(DEFAULT_ROWS));
    assertThat(server.getQueryIDs(), hasSize(1));
    String queryId = queryResponse.responseObject.getString("queryId");
    assertThat(queryId, is(notNullValue()));
    assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
  }

  @Test
  public void shouldExecuteMultiplePushQueries() throws Exception {

    int numQueries = 10;
    for (int i = 0; i < numQueries; i++) {
      // When
      QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);

      // Then
      assertThat(server.getQueryIDs(), hasSize(i + 1));
      String queryId = queryResponse.responseObject.getString("queryId");
      assertThat(queryId, is(notNullValue()));
      assertThat(server.getQueryIDs(), hasItem(new PushQueryId(queryId)));
    }
  }

  @Test
  public void shouldCloseQueriesOnDifferentConnectionsWhenConnectionsAreClosed() throws Exception {

    int numQueries = 10;
    List<WebClient> clients = new ArrayList<>();
    for (int i = 0; i < numQueries; i++) {
      // We use different clients to ensure requests are sent on different connections
      WebClient client = createClient();
      clients.add(client);

      // When
      QueryResponse queryResponse = executePushQueryAndWaitForRows(client,
          DEFAULT_PUSH_QUERY_REQUEST_BODY);

      // Then
      String queryId = queryResponse.responseObject.getString("queryId");
      assertThat(queryId, is(notNullValue()));
      assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
      assertThat(server.getQueryIDs(), hasSize(i + 1));
      assertThat(server.queryConnectionCount(), is(i + 1));
    }

    // Now close them one by one and make sure queries are cleaned up
    int count = 0;
    for (WebClient client : clients) {
      // Given
      client.close();

      // Then
      int num = numQueries - count - 1;
      assertThatEventually(server::queryConnectionCount, is(num));
      assertThat(server.getQueryIDs(), hasSize(num));
      count++;
    }
  }

  @Test
  public void shouldCloseQueriesOnSameConnectionsWhenConnectionsAreClosed() throws Exception {

    int numQueries = 10;
    for (int i = 0; i < numQueries; i++) {
      // When
      QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);

      // Then
      String queryId = queryResponse.responseObject.getString("queryId");
      assertThat(queryId, is(notNullValue()));
      assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
      assertThat(server.getQueryIDs(), hasSize(i + 1));
    }
    assertThatEventually(server::queryConnectionCount, is(1));

    // When
    client.close();

    // Then
    assertThatEventually(server::queryConnectionCount, is(0));
    assertThat(server.getQueryIDs().isEmpty(), is(true));
    client = null;
  }

  @Test
  public void shouldCloseMultipleQueriesOnDifferentConnectionsWhenConnectionsAreClosed()
      throws Exception {

    int numConnections = 5;
    int numQueries = 5;
    List<WebClient> clients = new ArrayList<>();
    for (int i = 0; i < numConnections; i++) {
      WebClient client = createClient();
      clients.add(client);
      for (int j = 0; j < numQueries; j++) {
        // When
        QueryResponse queryResponse = executePushQueryAndWaitForRows(client,
            DEFAULT_PUSH_QUERY_REQUEST_BODY);

        // Then
        String queryId = queryResponse.responseObject.getString("queryId");
        assertThat(queryId, is(notNullValue()));
        assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
        int queries = i * numQueries + j + 1;
        assertThat(server.getQueryIDs(), hasSize(i * numQueries + j + 1));
        assertThat(server.queryConnectionCount(), is(i + 1));
      }
    }

    int count = 0;
    for (WebClient client : clients) {
      // When
      client.close();

      // Then
      int connections = numConnections - count - 1;
      assertThatEventually(server::queryConnectionCount, is(connections));
      assertThat(server.getQueryIDs(), hasSize(numQueries * connections));
      count++;
    }
  }

  @Test
  public void shouldHandleQueryWithMissingSql() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("foo", "bar");

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No sql in arguments", queryResponse.responseObject);
  }

  @Test
  public void shouldHandleExtraArgInQuery() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY)
        .put("badarg", 213);

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream",
        requestBody.toBuffer().appendString("\n"));

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_UNKNOWN_PARAM, "Unknown arg badarg",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleErrorInProcessingQuery() throws Exception {

    // Given
    testEndpoints.setRowsBeforePublisherError(DEFAULT_ROWS.size() - 1);

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream",
        DEFAULT_PUSH_QUERY_REQUEST_BODY.toBuffer());

    // Then
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertThat(queryResponse.rows, hasSize(DEFAULT_ROWS.size() - 1));
    validateError(ERROR_CODE_INTERNAL_ERROR, "Error in processing query", queryResponse.error);
    assertThat(testEndpoints.getQueryPublishers(), hasSize(1));
    assertThat(server.getQueryIDs().isEmpty(), is(true));
  }

  @Test
  public void shouldRejectMalformedJsonInQueryArgs() throws Exception {
    shouldRejectMalformedJsonInArgs("/query-stream");
  }

  @Test
  public void shouldRejectInvalidPushQuery() throws Exception {
    shouldRejectInvalidQuery("slllecct * from foo emit changes;");
  }

  @Test
  public void shouldRejectInvalidPullQuery() throws Exception {
    shouldRejectInvalidQuery("selllect * from foo where rowkey='123';");
  }

  @Test
  public void shouldRejectWhenInternalErrorInProcessingPushQuery() throws Exception {
    shouldRejectWhenInternalErrorInProcessingQuery("slllecct * from foo emit changes;");
  }

  @Test
  public void shouldRejectWhenInternalErrorInProcessingPullQuery() throws Exception {
    shouldRejectWhenInternalErrorInProcessingQuery("selllect * from foo where rowkey='123';");
  }

  @Test
  public void shouldCloseQuery() throws Exception {

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    // Make the request to stream a query
    client.post(8089, "localhost", "/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(DEFAULT_PUSH_QUERY_REQUEST_BODY, responseFuture);

    // Wait for all rows in the response to arrive
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(DEFAULT_ROWS.size()));

    // The response shouldn't have ended yet
    assertThat(writeStream.isEnded(), is(false));

    // Assert the query is still live on the server
    QueryResponse queryResponse = new QueryResponse(writeStream.getBody().toString());
    String queryId = queryResponse.responseObject.getString("queryId");
    assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
    assertThat(server.getQueryIDs(), hasSize(1));
    assertThat(testEndpoints.getQueryPublishers(), hasSize(1));

    // Now send another request to close the query
    JsonObject closeQueryRequestBody = new JsonObject().put("queryId", queryId);
    HttpResponse<Buffer> closeQueryResponse = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());
    assertThat(closeQueryResponse.statusCode(), is(200));

    // Assert the query no longer exists on the server
    assertThat(server.getQueryIDs(), not(hasItem(new PushQueryId(queryId))));
    assertThat(server.getQueryIDs(), hasSize(0));

    // The response should now be ended
    assertThatEventually(writeStream::isEnded, is(true));
    HttpResponse<Void> response = responseFuture.get();
    assertThat(response.statusCode(), is(200));
  }

  @Test
  public void shouldHandleMissingQueryIDInCloseQuery() throws Exception {

    // Given
    JsonObject closeQueryRequestBody = new JsonObject().put("foo", "bar");

    // When
    HttpResponse<Buffer> response = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No queryId in arguments",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleExtraArgInCloseQuery() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("queryId", "qwydguygwd")
        .put("badarg", 213);

    // When
    HttpResponse<Buffer> response = sendRequest("/close-query",
        requestBody.toBuffer().appendString("\n"));

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_UNKNOWN_PARAM, "Unknown arg badarg",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleUnknownQueryIDInCloseQuery() throws Exception {

    // Given
    JsonObject closeQueryRequestBody = new JsonObject().put("queryId", "xyzfasgf");

    // When
    HttpResponse<Buffer> response = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_UNKNOWN_QUERY_ID, "No query with id xyzfasgf",
        queryResponse.responseObject);
  }

  @Test
  public void shouldInsertWithNoAcksStream() throws Exception {

    // Given
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", false);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    //When
    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);

    // Then
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().getRowsInserted(), is(rows));
    assertThat(testEndpoints.getInsertsSubscriber().isCompleted(), is(true));
    assertThat(testEndpoints.getLastTarget(), is("test-stream"));
  }

  @Test
  public void shouldInsertWithAcksStream() throws Exception {

    // Given
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // When
    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);

    // Then
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));
    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertThat(insertsResponse.acks, hasSize(rows.size()));
    assertThat(testEndpoints.getInsertsSubscriber().getRowsInserted(), is(rows));
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().isCompleted(), is(true));
    assertThat(testEndpoints.getLastTarget(), is("test-stream"));
  }

  @Test
  public void shouldStreamInserts() throws Exception {

    // Given
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);

    // Stream for piping the HTTP request body
    SendStream readStream = new SendStream(vertx);
    // Stream for receiving the HTTP response body
    ReceiveStream writeStream = new ReceiveStream(vertx);
    VertxCompletableFuture<HttpResponse<Void>> fut = new VertxCompletableFuture<>();
    List<JsonObject> rows = generateInsertRows();

    // When

    // Make an HTTP request but keep the request body and response streams open
    client.post(8089, "localhost", "/inserts-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendStream(readStream, fut);

    // Write the initial params Json object to the request body
    readStream.acceptBuffer(params.toBuffer().appendString("\n"));

    // Asynchronously on a timer write inserts to the request body
    AtomicInteger rowIndex = new AtomicInteger();
    vertx.setPeriodic(100, tid -> {
      readStream.acceptBuffer(rows.get(rowIndex.getAndIncrement()).toBuffer().appendString("\n"));
      if (rowIndex.get() == rows.size()) {
        vertx.cancelTimer(tid);
        // End the inserts stream and request when we've written all the rows to the stream
        readStream.end();
      }
    });

    // Wait for the response to complete
    HttpResponse<Void> response = fut.get();

    // Then

    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));

    // Verify we got acks for all our inserts
    InsertsResponse insertsResponse = new InsertsResponse(writeStream.getBody().toString());
    assertThat(insertsResponse.acks, hasSize(rows.size()));

    // Make sure all inserts made it to the server
    assertThat(testEndpoints.getInsertsSubscriber().getRowsInserted(), is(rows));
    assertThat(testEndpoints.getInsertsSubscriber().isCompleted(), is(true));

    // Ensure we received at least some of the response before all the request body was written
    // Yay HTTP2!
    assertThat(readStream.getLastSentTime() > writeStream.getFirstReceivedTime(), is(true));
  }

  @Test
  public void shouldHandleMissingTargetInInserts() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("requiresAcks", true);

    // When
    HttpResponse<Buffer> response = sendRequest("/inserts-stream",
        requestBody.toBuffer().appendString("\n"));

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No target in arguments", queryResponse.responseObject);
  }

  @Test
  public void shouldHandleMissingAcksInInserts() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("target", "some-stream");

    // When
    HttpResponse<Buffer> response = sendRequest("/inserts-stream",
        requestBody.toBuffer().appendString("\n"));

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No requiresAcks in arguments",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleExtraArgInInserts() throws Exception {

    // Given
    JsonObject requestBody = new JsonObject().put("target", "some-stream")
        .put("requiresAcks", false)
        .put("badarg", 213);

    // When
    HttpResponse<Buffer> response = sendRequest("/inserts-stream",
        requestBody.toBuffer().appendString("\n"));

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_UNKNOWN_PARAM, "Unknown arg badarg",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleErrorInProcessingInserts() throws Exception {

    // Given
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // Inject an error on last row inserted
    testEndpoints.setAcksBeforePublisherError(rows.size() - 1);

    // When

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);

    // Then

    // The HTTP response will be OK as the error is later in the stream after response
    // headers have been written
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));
    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertThat(insertsResponse.acks, hasSize(rows.size() - 1));
    validateError(ERROR_CODE_INTERNAL_ERROR, "Error in processing inserts", insertsResponse.error);
    assertThat(testEndpoints.getInsertsSubscriber().isCompleted(), is(true));
  }

  @Test
  public void shouldRejectMalformedJsonInInsertsStreamArgs() throws Exception {
    shouldRejectMalformedJsonInArgs("/inserts-stream");
  }

  @Test
  public void shouldHandleMalformedJsonInInsertsStream() throws Exception {

    // Given
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (int i = 0; i < rows.size() - 1; i++) {
      JsonObject row = rows.get(i);
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }
    // Malformed row for the last one
    requestBody.appendString("{ijqwdijqw");

    // When

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);

    // Then

    // The HTTP response will be OK as the error is later in the stream after response
    // headers have been written
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));

    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    validateError(ERROR_CODE_MALFORMED_REQUEST, "Invalid JSON in inserts stream",
        insertsResponse.error);

    assertThat(testEndpoints.getInsertsSubscriber().isCompleted(), is(true));
  }

  @Test
  public void shouldReturn404ForInvalidUri() throws Exception {

    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();

    // When
    client
        .post(8089, "localhost", "/no-such-endpoint")
        .sendBuffer(Buffer.buffer(), requestFuture);
    HttpResponse<Buffer> response = requestFuture.get();

    // Then
    assertThat(response.statusCode(), is(404));
  }

  @Test
  public void shouldReturn406WithNoMatchingAcceptHeader() throws Exception {

    // When
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/query-stream")
        .putHeader("accept", "blahblah")
        .sendBuffer(Buffer.buffer(), requestFuture);
    HttpResponse<Buffer> response = requestFuture.get();

    // Then
    assertThat(response.statusCode(), is(406));
  }

  @Test
  public void shouldUseDelimitedFormatWhenNoAcceptHeaderQuery() throws Exception {
    // When
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY);
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/query-stream")
        .sendBuffer(requestBody.toBuffer(), requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertThat(queryResponse.rows, hasSize(DEFAULT_ROWS.size()));
    assertThat(response.bodyAsString().contains("\n"), is(true));
    assertThat(response.statusCode(), is(200));
  }

  @Test
  public void shouldUseDelimitedFormatWhenDelimitedAcceptHeaderQuery() throws Exception {
    // When
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY);
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/query-stream")
        .putHeader("accept", "application/vnd.ksqlapi.delimited.v1")
        .sendBuffer(requestBody.toBuffer(), requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertThat(queryResponse.rows, hasSize(DEFAULT_ROWS.size()));
    assertThat(response.bodyAsString().contains("\n"), is(true));
    assertThat(response.statusCode(), is(200));
  }

  @Test
  public void shouldUseJsonFormatWhenJsonAcceptHeaderQuery() throws Exception {
    // When
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY);
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/query-stream")
        .putHeader("accept", "application/json")
        .sendBuffer(requestBody.toBuffer(), requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    JsonArray jsonArray = new JsonArray(response.body());
    assertThat(jsonArray.size(), is(DEFAULT_ROWS.size() + 1));
    JsonObject metaData = jsonArray.getJsonObject(0);
    assertThat(metaData.getJsonArray("columnNames"), is(DEFAULT_COLUMN_NAMES));
    assertThat(metaData.getJsonArray("columnTypes"), is(DEFAULT_COLUMN_TYPES));
    for (int i = 0; i < DEFAULT_ROWS.size(); i++) {
      assertThat(jsonArray.getJsonArray(i + 1), is(DEFAULT_ROWS.get(i)));
    }
  }

  @Test
  public void shouldUseDelimitedFormatWhenNoAcceptHeaderInserts() throws Exception {
    // When
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/inserts-stream")
        .sendBuffer(requestBody, requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertThat(insertsResponse.acks, hasSize(rows.size()));
  }

  @Test
  public void shouldUseDelimitedFormatWhenDelimitedHeaderInserts() throws Exception {
    // When
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/inserts-stream")
        .putHeader("accept", "application/vnd.ksqlapi.delimited.v1")
        .sendBuffer(requestBody, requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertThat(insertsResponse.acks, hasSize(rows.size()));
  }

  @Test
  public void shouldUseJsonFormatWhenJsonHeaderInserts() throws Exception {
    // When
    JsonObject params = new JsonObject().put("target", "test-stream").put("requiresAcks", true);
    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", "/inserts-stream")
        .putHeader("accept", "application/json")
        .sendBuffer(requestBody, requestFuture);

    // Then
    HttpResponse<Buffer> response = requestFuture.get();
    JsonArray jsonArray = new JsonArray(response.body());
    assertThat(jsonArray.size(), is(DEFAULT_ROWS.size()));
    final JsonObject ackLine = new JsonObject().put("status", "ok");
    for (int i = 0; i < jsonArray.size(); i++) {
      assertThat(jsonArray.getJsonObject(i), is(ackLine));
    }
  }

  private void shouldRejectMalformedJsonInArgs(String uri) throws Exception {

    // Given
    Buffer requestBody = Buffer.buffer().appendString("{\"foo\":1");

    // When
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", uri)
        .sendBuffer(requestBody, requestFuture);
    HttpResponse<Buffer> response = requestFuture.get();

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));
    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MALFORMED_REQUEST, "Malformed JSON in request",
        queryResponse.responseObject);
  }

  private void shouldRejectInvalidQuery(final String query) throws Exception {

    // Given
    ParseFailedException pfe = new ParseFailedException("invalid query blah");
    testEndpoints.setCreateQueryPublisherException(pfe);
    JsonObject requestBody = new JsonObject().put("sql", query);

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream",
        requestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_INVALID_QUERY, pfe.getMessage(),
        queryResponse.responseObject);
  }

  private void shouldRejectWhenInternalErrorInProcessingQuery(final String query) throws Exception {

    // Given
    NullPointerException npe = new NullPointerException("oops");
    testEndpoints.setCreateQueryPublisherException(npe);
    JsonObject requestBody = new JsonObject().put("sql", query);

    // When
    HttpResponse<Buffer> response = sendRequest("/query-stream",
        requestBody.toBuffer());

    // Then
    assertThat(response.statusCode(), is(500));
    assertThat(response.statusMessage(), is("Internal Server Error"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_INTERNAL_ERROR,
        "The server encountered an internal error when processing the query." +
            " Please consult the server logs for more information.",
        queryResponse.responseObject);
  }

  private QueryResponse executePushQueryAndWaitForRows(final JsonObject requestBody)
      throws Exception {
    return executePushQueryAndWaitForRows(client, requestBody);
  }

  private QueryResponse executePushQueryAndWaitForRows(final WebClient client,
      final JsonObject requestBody)
      throws Exception {

    ReceiveStream writeStream = new ReceiveStream(vertx);

    client.post(8089, "localhost", "/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(requestBody, ar -> {
        });

    // Wait for all rows to arrive
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(DEFAULT_ROWS.size()));

    // Note, the response hasn't ended at this point
    assertThat(writeStream.isEnded(), is(false));

    return new QueryResponse(writeStream.getBody().toString());
  }

  private WebClient createClient() {
    WebClientOptions options = new WebClientOptions().setSsl(true).
        setUseAlpn(true).
        setProtocolVersion(HttpVersion.HTTP_2).
        setTrustAll(true);

    return WebClient.create(vertx, options);
  }

  private static void validateError(final int errorCode, final String message,
      final JsonObject error) {
    assertThat(error.getString("status"), is("error"));
    assertThat(error.getInteger("errorCode"), is(errorCode));
    assertThat(error.getString("message"), is(message));
    assertThat(error.size(), is(3));
  }

  private HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody)
      throws Exception {
    return sendRequest(client, uri, requestBody);
  }

  private HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(8089, "localhost", uri)
        .sendBuffer(requestBody, requestFuture);
    return requestFuture.get();
  }

  @SuppressWarnings("unchecked")
  private void setDefaultRowGenerator() {
    List<GenericRow> rows = new ArrayList<>();
    for (JsonArray ja : DEFAULT_ROWS) {
      rows.add(GenericRow.fromList(ja.getList()));
    }
    testEndpoints.setRowGeneratorFactory(
        () -> new ListRowGenerator(
            DEFAULT_COLUMN_NAMES.getList(),
            DEFAULT_COLUMN_TYPES.getList(),
            rows));
  }

  private static List<JsonArray> generateRows() {
    List<JsonArray> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      JsonArray row = new JsonArray().add("foo" + i).add(i).add(i % 2 == 0);
      rows.add(row);
    }
    return rows;
  }

  private static List<JsonObject> generateInsertRows() {
    List<JsonObject> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      JsonObject row = new JsonObject()
          .put("name", "foo" + i)
          .put("age", i)
          .put("male", i % 2 == 0);
      rows.add(row);
    }
    return rows;
  }

}
