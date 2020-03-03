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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.ApiServerConfig;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.utils.ListRowGenerator;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseApiTest {

  private static final Logger log = LoggerFactory.getLogger(BaseApiTest.class);

  protected static final JsonArray DEFAULT_COLUMN_NAMES = new JsonArray().add("name").add("age")
      .add("male");
  protected static final JsonArray DEFAULT_COLUMN_TYPES = new JsonArray().add("STRING").add("INT")
      .add("BOOLEAN");
  protected static final List<JsonArray> DEFAULT_ROWS = generateRows();
  protected static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES = new JsonObject()
      .put("prop1", "val1").put("prop2", 23);
  protected static final String DEFAULT_PULL_QUERY = "select * from foo where rowkey='1234';";
  protected static final String DEFAULT_PUSH_QUERY = "select * from foo emit changes;";
  protected static final JsonObject DEFAULT_PUSH_QUERY_REQUEST_BODY = new JsonObject()
      .put("sql", DEFAULT_PUSH_QUERY)
      .put("properties", DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES);

  protected Vertx vertx;
  protected WebClient client;
  protected Server server;
  protected TestEndpoints testEndpoints;

  @Before
  public void setUp() throws Exception {

    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    ApiServerConfig serverConfig = createServerConfig();
    server = new Server(vertx, serverConfig, testEndpoints, false);
    server.start();
    this.client = createClient();
    setDefaultRowGenerator();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        log.error("Failed to shutdown server", e);
      }
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  protected ApiServerConfig createServerConfig() {
    final Map<String, Object> config = new HashMap<>();
    config.put(ApiServerConfig.LISTENERS, "http://localhost:0");
    config.put(ApiServerConfig.VERTICLE_INSTANCES, 4);
    return new ApiServerConfig(config);
  }

  protected WebClientOptions createClientOptions() {
    return new WebClientOptions()
        .setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
        .setDefaultHost("localhost")
        .setDefaultPort(server.getListeners().get(0).getPort())
        .setReusePort(true);
  }

  protected WebClient createClient() {
    return WebClient.create(vertx, createClientOptions());
  }

  protected QueryResponse executePushQueryAndWaitForRows(final JsonObject requestBody)
      throws Exception {
    return executePushQueryAndWaitForRows(client, requestBody);
  }

  protected QueryResponse executePushQueryAndWaitForRows(final WebClient client,
      final JsonObject requestBody)
      throws Exception {

    ReceiveStream writeStream = new ReceiveStream(vertx);

    client.post("/query-stream")
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


  protected HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody)
      throws Exception {
    return sendRequest(client, uri, requestBody);
  }

  protected HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(uri)
        .sendBuffer(requestBody, requestFuture);
    return requestFuture.get();
  }

  protected static void validateError(final int errorCode, final String message,
      final JsonObject error) {
    assertThat(error.size(), is(3));
    validateErrorCommon(errorCode, message, error);
  }

  protected static void validateErrorCommon(final int errorCode, final String message,
      final JsonObject error) {
    assertThat(error.getString("status"), is("error"));
    assertThat(error.getInteger("errorCode"), is(errorCode));
    assertThat(error.getString("message"), is(message));
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

}
