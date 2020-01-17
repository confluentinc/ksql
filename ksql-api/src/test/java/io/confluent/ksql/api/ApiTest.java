/*
 * Copyright 2019 Confluent Inc.
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
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MISSING_PARAM;
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_UNKNOWN_QUERY_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.api.TestQueryPublisher.ListRowGenerator;
import io.confluent.ksql.api.impl.VertxCompletableFuture;
import io.confluent.ksql.api.server.QueryID;
import io.confluent.ksql.api.server.Server;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiTest {

  private static final long WAIT_TIMEOUT = 10000;
  private static final JsonArray DEFAULT_COLUMN_NAMES = new JsonArray().add("name").add("age")
      .add("male");
  private static final JsonArray DEFAULT_COLUMN_TYPES = new JsonArray().add("STRING").add("INT")
      .add("BOOLEAN");
  private static final List<JsonArray> DEFAULT_ROWS = generateRows();
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES = new JsonObject()
      .put("prop1", "val1").put("prop2", 23);
  private static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", "select * from foo")
      .put("push", true).put("properties", DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES);

  private Vertx vertx;
  private Server server;
  private TestEndpoints testEndpoints;
  private WebClient client;

  @Before
  public void setUp() throws Throwable {

    vertx = Vertx.vertx();

    HttpServerOptions httpServerOptions =
        new HttpServerOptions()
            .setHost("localhost")
            .setPort(8089)
            .setUseAlpn(true)
            .setSsl(true)
            .setPemKeyCertOptions(
                new PemKeyCertOptions().setKeyPath(findFilePath("test-server-key.pem"))
                    .setCertPath(findFilePath("test-server-cert.pem"))
            );

    testEndpoints = new TestEndpoints(vertx);
    JsonObject config = new JsonObject().put("verticle-instances", 4);
    server = new Server(vertx, config, testEndpoints, httpServerOptions);
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

    JsonObject requestBody = new JsonObject().put("sql", "select * from foo").put("push", false);
    JsonObject properties = new JsonObject().put("prop1", "val1").put("prop2", 23);
    requestBody.put("properties", properties);

    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());

    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());
    assertEquals("select * from foo", testEndpoints.getLastSql());
    assertFalse(testEndpoints.getLastPush());
    assertEquals(properties, testEndpoints.getLastProperties());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertEquals(DEFAULT_COLUMN_NAMES, queryResponse.responseObject.getJsonArray("columnNames"));
    assertEquals(DEFAULT_COLUMN_TYPES, queryResponse.responseObject.getJsonArray("columnTypes"));
    assertEquals(DEFAULT_ROWS, queryResponse.rows);
    assertEquals(0, server.getQueryIDs().size());
    String queryID = queryResponse.responseObject.getString("queryID");
    assertNotNull(queryID);
    assertFalse(server.getQueryIDs().contains(new QueryID(queryID)));
    Integer rowCount = queryResponse.responseObject.getInteger("rowCount");
    assertNotNull(rowCount);
    assertEquals(DEFAULT_ROWS.size(), rowCount.intValue());
  }

  @Test
  public void shouldExecutePushQuery() throws Exception {

    QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);

    assertEquals("select * from foo", testEndpoints.getLastSql());
    assertTrue(testEndpoints.getLastPush());
    assertEquals(DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES, testEndpoints.getLastProperties());

    assertEquals(DEFAULT_COLUMN_NAMES, queryResponse.responseObject.getJsonArray("columnNames"));
    assertEquals(DEFAULT_COLUMN_TYPES, queryResponse.responseObject.getJsonArray("columnTypes"));
    assertEquals(DEFAULT_ROWS, queryResponse.rows);
    assertEquals(1, server.getQueryIDs().size());
    String queryID = queryResponse.responseObject.getString("queryID");
    assertNotNull(queryID);
    assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
    assertFalse(queryResponse.responseObject.containsKey("rowCount"));
  }

  @Test
  public void shouldExecuteMultiplePushQueries() throws Exception {

    int numQueries = 10;
    for (int i = 0; i < numQueries; i++) {
      QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);
      assertEquals(i + 1, server.getQueryIDs().size());
      String queryID = queryResponse.responseObject.getString("queryID");
      assertNotNull(queryID);
      assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
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
      QueryResponse queryResponse = executePushQueryAndWaitForRows(client,
          DEFAULT_PUSH_QUERY_REQUEST_BODY);
      String queryID = queryResponse.responseObject.getString("queryID");
      assertNotNull(queryID);
      assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
      assertEquals(i + 1, server.getQueryIDs().size());
      assertEquals(i + 1, server.queryConnectionCount());
    }
    assertAllQueries(numQueries, true);

    int count = 0;
    for (WebClient client : clients) {
      client.close();
      int num = numQueries - count - 1;
      assertTrue(waitUntil(() -> server.queryConnectionCount() == num));
      assertEquals(num, server.getQueryIDs().size());
      count++;
    }
    assertAllQueries(numQueries, false);
  }

  @Test
  public void shouldCloseQueriesOnSameConnectionsWhenConnectionsAreClosed() throws Exception {

    int numQueries = 10;
    for (int i = 0; i < numQueries; i++) {
      QueryResponse queryResponse = executePushQueryAndWaitForRows(DEFAULT_PUSH_QUERY_REQUEST_BODY);
      String queryID = queryResponse.responseObject.getString("queryID");
      assertNotNull(queryID);
      assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
      assertEquals(i + 1, server.getQueryIDs().size());
    }
    assertEquals(1, server.queryConnectionCount());
    assertAllQueries(numQueries, true);

    client.close();
    assertTrue(waitUntil(() -> server.queryConnectionCount() == 0));
    assertTrue(server.getQueryIDs().isEmpty());
    client = null;

    assertAllQueries(numQueries, false);
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
        QueryResponse queryResponse = executePushQueryAndWaitForRows(client,
            DEFAULT_PUSH_QUERY_REQUEST_BODY);
        String queryID = queryResponse.responseObject.getString("queryID");
        assertNotNull(queryID);
        assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
        int queries = i * numQueries + j + 1;
        assertEquals(i * numQueries + j + 1, server.getQueryIDs().size());
        assertEquals(i + 1, server.queryConnectionCount());
        assertAllQueries(queries, true);
      }
    }

    int count = 0;
    for (WebClient client : clients) {
      client.close();
      int connections = numConnections - count - 1;
      assertTrue(waitUntil(() -> server.queryConnectionCount() == connections));
      assertEquals(numQueries * connections, server.getQueryIDs().size());
      count++;
    }

    assertAllQueries(numConnections * numQueries, false);
  }

  @Test
  public void shouldHandleQueryWithMissingSql() throws Exception {

    JsonObject requestBody = new JsonObject().put("foo", "bar");

    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No sql in arguments", queryResponse.responseObject);
  }

  @Test
  public void shouldHandleQueryWithMissingPush() throws Exception {

    JsonObject requestBody = new JsonObject().put("sql", "select * from foo");

    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No push in arguments", queryResponse.responseObject);
  }

  @Test
  public void shouldHandleErrorInProcessingQuery() throws Exception {

    testEndpoints.setRowsBeforePublisherError(DEFAULT_ROWS.size() - 1);

    HttpResponse<Buffer> response = sendRequest("/query-stream",
        DEFAULT_PUSH_QUERY_REQUEST_BODY.toBuffer());

    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertEquals(DEFAULT_ROWS.size() - 1, queryResponse.rows.size());
    validateError(ERROR_CODE_INTERNAL_ERROR, "Error in processing query", queryResponse.error);

    assertEquals(1, testEndpoints.getQueryPublishers().size());
    assertFalse(testEndpoints.getQueryPublishers().iterator().next().hasSubscriber());
    assertTrue(server.getQueryIDs().isEmpty());
  }

  @Test
  public void shouldCloseQuery() throws Exception {

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a query
    client.post(8089, "localhost", "/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(DEFAULT_PUSH_QUERY_REQUEST_BODY, ar -> {
        });

    // Wait for all rows in the response to arrive
    assertTrue(waitUntil(() -> {
      Buffer buff = writeStream.getBody();
      try {
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size() == DEFAULT_ROWS.size();
      } catch (Throwable t) {
        return false;
      }
    }));

    // The response shouldn't have ended yet
    assertFalse(writeStream.isEnded());

    // Assert the query is still live on the server
    QueryResponse queryResponse = new QueryResponse(writeStream.getBody().toString());
    String queryID = queryResponse.responseObject.getString("queryID");
    assertTrue(server.getQueryIDs().contains(new QueryID(queryID)));
    assertEquals(1, server.getQueryIDs().size());
    assertEquals(1, testEndpoints.getQueryPublishers().size());

    // Now send another request to close the query
    JsonObject closeQueryRequestBody = new JsonObject().put("queryID", queryID);
    HttpResponse<Buffer> closeQueryResponse = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());
    assertEquals(200, closeQueryResponse.statusCode());

    // Assert the query no longer exists on the server
    assertFalse(server.getQueryIDs().contains(new QueryID(queryID)));
    assertEquals(0, server.getQueryIDs().size());
    assertEquals(1, testEndpoints.getQueryPublishers().size());
    assertFalse(testEndpoints.getQueryPublishers().iterator().next().hasSubscriber());

    // The response should now be ended
    assertTrue(waitUntil(writeStream::isEnded));
  }

  @Test
  public void shouldHandleMissingQueryIDInCloseQuery() throws Exception {

    JsonObject closeQueryRequestBody = new JsonObject().put("foo", "bar");
    HttpResponse<Buffer> response = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No queryID in arguments",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleUnknownQueryIDInCloseQuery() throws Exception {

    JsonObject closeQueryRequestBody = new JsonObject().put("queryID", "xyzfasgf");
    HttpResponse<Buffer> response = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_UNKNOWN_QUERY_ID, "No query with id xyzfasgf",
        queryResponse.responseObject);
  }

  @Test
  public void shouldInsertWithNoAcksStream() throws Exception {

    JsonObject params = new JsonObject().put("target", "test-stream").put("acks", false);

    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);
    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());

    assertEquals(rows, testEndpoints.getInsertsSubscriber().getRowsInserted());
    assertTrue(testEndpoints.getInsertsSubscriber().isCompleted());
    assertEquals("test-stream", testEndpoints.getLastTarget());
  }

  @Test
  public void shouldInsertWithAcksStream() throws Exception {

    JsonObject params = new JsonObject().put("target", "test-stream").put("acks", true);

    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);
    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());

    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertEquals(rows.size(), insertsResponse.acks.size());

    assertEquals(rows, testEndpoints.getInsertsSubscriber().getRowsInserted());
    assertTrue(testEndpoints.getInsertsSubscriber().isCompleted());
    assertEquals("test-stream", testEndpoints.getLastTarget());
  }

  @Test
  public void shouldStreamInserts() throws Exception {

    JsonObject params = new JsonObject().put("target", "test-stream").put("acks", true);

    SendStream readStream = new SendStream(vertx);
    ReceiveStream writeStream = new ReceiveStream(vertx);
    VertxCompletableFuture<HttpResponse<Void>> fut = new VertxCompletableFuture<>();
    List<JsonObject> rows = generateInsertRows();

    client.post(8089, "localhost", "/inserts-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendStream(readStream, fut);

    readStream.acceptBuffer(params.toBuffer().appendString("\n"));

    AtomicInteger rowIndex = new AtomicInteger();
    vertx.setPeriodic(100, tid -> {
      readStream.acceptBuffer(rows.get(rowIndex.getAndIncrement()).toBuffer().appendString("\n"));
      if (rowIndex.get() == rows.size()) {
        vertx.cancelTimer(tid);
        readStream.end();
      }
    });

    HttpResponse<Void> response = fut.get();

    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());

    InsertsResponse insertsResponse = new InsertsResponse(writeStream.getBody().toString());
    assertEquals(rows.size(), insertsResponse.acks.size());
    assertEquals(rows, testEndpoints.getInsertsSubscriber().getRowsInserted());
    assertTrue(testEndpoints.getInsertsSubscriber().isCompleted());
    // When response is complete the acks subscriber should be unsubscribed
    assertFalse(testEndpoints.getAcksPublisher().hasSubscriber());

    // Ensure we received at last some of the response before all the request body was written
    // Yay HTTP2!
    assertTrue(readStream.getLastSentTime() > writeStream.getFirstReceivedTime());
  }

  @Test
  public void shouldHandleMissingTargetInInserts() throws Exception {

    JsonObject requestBody = new JsonObject().put("acks", true);

    HttpResponse<Buffer> response = sendRequest("/inserts-stream",
        requestBody.toBuffer().appendString("\n"));

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No target in arguments", queryResponse.responseObject);
  }

  @Test
  public void shouldHandleMissingAcksInInserts() throws Exception {

    JsonObject requestBody = new JsonObject().put("target", "some-stream");

    HttpResponse<Buffer> response = sendRequest("/inserts-stream",
        requestBody.toBuffer().appendString("\n"));

    assertEquals(400, response.statusCode());
    assertEquals("Bad Request", response.statusMessage());

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    validateError(ERROR_CODE_MISSING_PARAM, "No acks in arguments",
        queryResponse.responseObject);
  }

  @Test
  public void shouldHandleErrorInProcessingInserts() throws Exception {

    JsonObject params = new JsonObject().put("target", "test-stream").put("acks", true);

    List<JsonObject> rows = generateInsertRows();
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : rows) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // Inject an error on last row inserted
    testEndpoints.setAcksBeforePublisherError(rows.size() - 1);

    // The HTTP response will be OK as the error is later in the stream after response
    // headers have been written
    HttpResponse<Buffer> response = sendRequest("/inserts-stream", requestBody);
    assertEquals(200, response.statusCode());
    assertEquals("OK", response.statusMessage());

    String responseBody = response.bodyAsString();
    InsertsResponse insertsResponse = new InsertsResponse(responseBody);
    assertEquals(rows.size() - 1, insertsResponse.acks.size());
    validateError(ERROR_CODE_INTERNAL_ERROR, "Error in processing inserts", insertsResponse.error);

    assertTrue(testEndpoints.getInsertsSubscriber().isCompleted());
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
    assertTrue(waitUntil(() -> {
      Buffer buff = writeStream.getBody();
      try {
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size() == DEFAULT_ROWS.size();
      } catch (Throwable t) {
        return false;
      }
    }));

    // Note, the response hasn't ended at this point
    assertFalse(writeStream.isEnded());

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
    assertEquals("error", error.getString("status"));
    assertEquals(errorCode, error.getInteger("errorCode").intValue());
    assertEquals(message, error.getString("message"));
    assertEquals(3, error.size());
  }

  private void assertAllQueries(final int num, final boolean open) {
    assertEquals(num, testEndpoints.getQueryPublishers().size());
    for (TestQueryPublisher queryPublisher : testEndpoints.getQueryPublishers()) {
      if (open) {
        assertTrue(queryPublisher.hasSubscriber());
      } else {
        assertFalse(queryPublisher.hasSubscriber());
      }
    }
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

  private void setDefaultRowGenerator() {
    testEndpoints.setRowGeneratorFactory(
        () -> new ListRowGenerator(DEFAULT_COLUMN_NAMES, DEFAULT_COLUMN_TYPES,
            DEFAULT_ROWS));
  }

  private static boolean waitUntil(final Supplier<Boolean> test) throws Exception {
    long start = System.currentTimeMillis();
    do {
      if (test.get()) {
        return true;
      }
      Thread.sleep(1);
    } while (System.currentTimeMillis() - start < WAIT_TIMEOUT);
    return false;
  }

  private static List<JsonArray> generateRows() {
    List<JsonArray> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new JsonArray().add("foo" + i).add(i).add(i % 2 == 0));
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

  private static String findFilePath(String fileName) throws Exception {
    URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);
    if (url == null) {
      throw new FileNotFoundException(fileName);
    }
    return url.toURI().getPath();
  }

  private static class QueryResponse {

    public final JsonObject responseObject;
    public final List<JsonArray> rows;
    public final JsonObject error;

    public QueryResponse(String responseBody) {
      JsonObject error = null;
      String[] parts = responseBody.split("\n");
      responseObject = new JsonObject(parts[0]);
      rows = new ArrayList<>();
      for (int i = 1; i < parts.length; i++) {
        if (parts[i].startsWith("[")) {
          JsonArray row = new JsonArray(parts[i]);
          rows.add(row);
        } else {
          assertNull(error);
          error = new JsonObject(parts[i]);
        }
      }
      this.error = error;
    }

    @Override
    public String toString() {
      return "QueryResponse{" +
          "metadata=" + responseObject +
          ", rows=" + rows +
          '}';
    }
  }

  private static class InsertsResponse {

    public final List<JsonObject> acks;
    public final JsonObject error;

    public InsertsResponse(String responseBody) {
      String[] parts = responseBody.split("\n");
      acks = new ArrayList<>();
      JsonObject error = null;
      for (int i = 0; i < parts.length; i++) {
        JsonObject jsonObject = new JsonObject(parts[i]);
        String status = jsonObject.getString("status");
        assertNotNull(status);
        if (status.equals("ok")) {
          acks.add(jsonObject);
        } else {
          assertNull(error);
          error = jsonObject;
        }
      }
      this.error = error;
    }

    @Override
    public String toString() {
      return "QueryResponse{" +
          "acks=" + acks +
          '}';
    }
  }

  private static class SendStream implements ReadStream<Buffer> {

    private final Vertx vertx;
    private final Queue<Buffer> pending = new LinkedList<>();
    private Handler<Buffer> handler;
    private Handler<Void> endHandler;
    private boolean ended;
    private long lastSentTime;

    public SendStream(final Vertx vertx) {
      this.vertx = vertx;
    }

    synchronized void acceptBuffer(final Buffer buffer) {
      if (handler == null) {
        pending.add(buffer);
      } else {
        sendBuffer(buffer);
      }
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
      return this;
    }

    @Override
    public synchronized ReadStream<Buffer> handler(@Nullable final Handler<Buffer> handler) {
      this.handler = handler;
      if (handler != null) {
        Buffer buff;
        while ((buff = pending.poll()) != null) {
          sendBuffer(buff);
        }
      }
      return this;
    }

    private void sendBuffer(final Buffer buff) {
      lastSentTime = System.currentTimeMillis();
      handler.handle(buff);
    }

    @Override
    public ReadStream<Buffer> pause() {
      return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
      return this;
    }

    @Override
    public ReadStream<Buffer> fetch(final long amount) {
      return this;
    }

    @Override
    public synchronized ReadStream<Buffer> endHandler(@Nullable final Handler<Void> endHandler) {
      this.endHandler = endHandler;
      if (ended && endHandler != null) {
        vertx.runOnContext(v -> endHandler.handle(null));
      }
      return this;
    }

    synchronized void end() {
      this.ended = true;
      if (endHandler != null) {
        vertx.runOnContext(v -> endHandler.handle(null));
      }
    }

    synchronized long getLastSentTime() {
      return lastSentTime;
    }
  }

  private static class ReceiveStream implements WriteStream<Buffer> {

    private final Vertx vertx;
    private final Buffer body = Buffer.buffer();
    private long firstReceivedTime;
    private boolean ended;

    public ReceiveStream(final Vertx vertx) {
      this.vertx = vertx;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
      return this;
    }

    @Override
    public synchronized WriteStream<Buffer> write(final Buffer data) {
      firstReceivedTime = System.currentTimeMillis();
      body.appendBuffer(data);
      return this;
    }

    @Override
    public WriteStream<Buffer> write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
      body.appendBuffer(data);
      if (handler != null) {
        vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
      }
      return this;
    }

    @Override
    public synchronized void end() {
      ended = true;
    }

    @Override
    public void end(final Handler<AsyncResult<Void>> handler) {
      end();
      if (handler != null) {
        vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
      }
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(final int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@Nullable final Handler<Void> handler) {
      return this;
    }

    synchronized Buffer getBody() {
      return body;
    }

    synchronized long getFirstReceivedTime() {
      return firstReceivedTime;
    }

    synchronized boolean isEnded() {
      return ended;
    }
  }

}
