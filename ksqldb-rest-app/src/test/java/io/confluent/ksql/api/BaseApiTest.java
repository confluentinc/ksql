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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.api.server.ServerUtils;
import io.confluent.ksql.api.utils.ListRowGenerator;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlDefaultSecurityExtension;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseApiTest {

  protected static final Logger log = LoggerFactory.getLogger(BaseApiTest.class);

  protected static final JsonArray DEFAULT_COLUMN_NAMES = new JsonArray().add("f_str").add("f_int")
      .add("f_bool").add("f_long").add("f_double").add("f_decimal").add("f_bytes")
      .add("f_array").add("f_map").add("f_struct").add("f_null").add("f_timestamp")
      .add("f_date").add("f_time");
  protected static final JsonArray DEFAULT_COLUMN_TYPES = new JsonArray().add("STRING").add("INTEGER")
      .add("BOOLEAN").add("BIGINT").add("DOUBLE").add("DECIMAL(4, 2)").add("BYTES")
      .add("ARRAY<STRING>").add("MAP<STRING, STRING>").add("STRUCT<`F1` STRING, `F2` INTEGER>").add("INTEGER")
      .add("TIMESTAMP").add("DATE").add("TIME");
  protected static final LogicalSchema DEFAULT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f_str"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("f_int"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("f_bool"), SqlTypes.BOOLEAN)
      .valueColumn(ColumnName.of("f_long"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("f_double"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("f_decimal"), SqlTypes.decimal(4, 2))
      .valueColumn(ColumnName.of("f_bytes"), SqlTypes.BYTES)
      .valueColumn(ColumnName.of("f_array"), SqlTypes.array(SqlTypes.STRING))
      .valueColumn(ColumnName.of("f_map"), SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING))
      .valueColumn(ColumnName.of("f_struct"),
          SqlTypes.struct()
              .field("F1", SqlTypes.STRING)
              .field("F2", SqlTypes.INTEGER)
              .build())
      .valueColumn(ColumnName.of("f_null"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("f_timestamp"), SqlTypes.TIMESTAMP)
      .valueColumn(ColumnName.of("f_date"), SqlTypes.DATE)
      .valueColumn(ColumnName.of("f_time"), SqlTypes.TIME)
      .build();
  protected static final Schema F_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("F1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("F2", Schema.OPTIONAL_INT32_SCHEMA)
        .optional().build();
  protected static final List<GenericRow> DEFAULT_GENERIC_ROWS = generateGenericRows();
  protected static final List<JsonArray> DEFAULT_JSON_ROWS = convertToJsonRows(DEFAULT_GENERIC_ROWS);
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
  protected ServerState serverState;

  @Rule
  public final Timeout timeout = Timeout.seconds(90);

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    vertx.exceptionHandler(t -> log.error("Unhandled exception in Vert.x", t));

    testEndpoints = new TestEndpoints();
    KsqlRestConfig serverConfig = createServerConfig();
    serverState = new ServerState();
    serverState.setReady();
    createServer(serverConfig);
    this.client = createClient();
    setDefaultRowGenerator();
  }

  @After
  public void tearDown() {
    stopClient();
    stopServer();
    if (vertx != null) {
      vertx.close();
    }
  }

  protected void stopServer() {
    if (server != null) {
      try {
        server.stop();
        server = null;
      } catch (Exception e) {
        log.error("Failed to shutdown server", e);
      }
    }
  }

  protected void stopClient() {
    if (client != null) {
      try {
        client.close();
        client = null;
      } catch (Exception e) {
        log.error("Failed to close client", e);
      }
    }
  }

  protected void createServer(KsqlRestConfig serverConfig) {
    server = new Server(vertx, serverConfig, testEndpoints,
    new KsqlDefaultSecurityExtension(), Optional.empty(), serverState, Optional.empty());

    try {
      server.start();
    } catch (final Exception e) {
      server = null;
      throw e;
    }
  }

  protected KsqlRestConfig createServerConfig() {
    final Map<String, Object> config = new HashMap<>();
    config.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0");
    config.put(KsqlRestConfig.VERTICLE_INSTANCES, 4);
    return new KsqlRestConfig(config);
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

    sendPostRequest(client, "/query-stream",
        (request) -> request
            .as(BodyCodec.pipe(writeStream))
            .sendJsonObject(requestBody, ar -> {
    }));

    // Wait for all rows to arrive
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(DEFAULT_JSON_ROWS.size()));

    // Note, the response hasn't ended at this point
    assertThat(writeStream.isEnded(), is(false));

    return new QueryResponse(writeStream.getBody().toString());
  }

  protected HttpResponse<Buffer> sendGetRequest(final String uri) throws Exception {
    return sendGetRequest(client, uri);
  }

  protected HttpResponse<Buffer> sendGetRequest(final WebClient client, final String uri)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client.get(uri).send(requestFuture);
    return requestFuture.get();
  }

  protected HttpResponse<Buffer> sendPostRequest(final String uri, final Buffer requestBody)
      throws Exception {
    return sendPostRequest(client, uri, requestBody);
  }

  protected HttpResponse<Buffer> sendPostRequest(final WebClient client, final String uri,
                                                 final Buffer requestBody)
      throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(uri)
        .sendBuffer(requestBody, requestFuture);
    return requestFuture.get(10_000L, TimeUnit.MILLISECONDS);
  }

  protected void sendPostRequest(final String uri, final Consumer<HttpRequest<Buffer>> requestSender) {
    sendPostRequest(client, uri, requestSender);
  }

  protected void sendPostRequest(
      final WebClient client,
      final String uri,
      final Consumer<HttpRequest<Buffer>> requestSender) {
    requestSender.accept(client.post(uri));
  }

  protected void waitForQueryPublisherSubscribed() {
      final Set<Publisher<?>> queryPublishers = testEndpoints.getPublishers();
      assertThat(queryPublishers, hasSize(1));
      queryPublishers.forEach(
          publisher -> {
            TestQueryPublisher queryPublisher = (TestQueryPublisher) publisher;
              final long timeout = System.currentTimeMillis() + 30_000L;
              while ( queryPublisher.getSubscriber() == null) {
                  try {
                      Thread.sleep(100L);
                  } catch (InterruptedException swallow) {
                  }

                  if (System.currentTimeMillis() > timeout) {
                      throw new RuntimeException("Subscribers not registered.");
                  }
              }
          }
      );
  }

  protected static void validateError(final int errorCode, final String message,
      final JsonObject error) {
    validateErrorCommon(errorCode, message, error);
  }

  protected static void validateErrorCommon(final int errorCode, final String message,
      final JsonObject error) {
    assertThat(error.getInteger("error_code"), is(errorCode));
    assertThat(error.getString("message"), startsWith(message));
  }

  private static GenericRow rowWithIndex(final int index) {
    final Struct structField = new Struct(F_STRUCT_SCHEMA);
    structField.put("F1", "v" + index);
    structField.put("F2", index);
    return GenericRow.genericRow(
        "foo" + index,
        index,
        index % 2 == 0,
        index * index,
        index + 0.1111,
        BigDecimal.valueOf(index + 0.1),
        new byte[]{0, 1, 2, 3, 4, 5},
        ImmutableList.of("s" + index, "t" + index),
        ImmutableMap.of("k" + index, "v" + index),
        structField,
        null,
        "2020-01-01T04:40:34.789",
        "2020-01-01",
        "04:40:34.789"
    );
  }

  private static List<GenericRow> generateGenericRows() {
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(rowWithIndex(i));
    }
    return rows;
  }

  private static List<JsonArray> convertToJsonRows(final List<GenericRow> rows) {
    return rows.stream()
        .map(row -> ServerUtils.serializeObject(row).toJsonObject().getJsonArray("columns"))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  protected void setDefaultRowGenerator() {
    testEndpoints.setRowGeneratorFactory(
        () -> new ListRowGenerator(
            DEFAULT_COLUMN_NAMES.getList(),
            DEFAULT_COLUMN_TYPES.getList(),
            DEFAULT_SCHEMA,
            DEFAULT_GENERIC_ROWS));
  }
}
