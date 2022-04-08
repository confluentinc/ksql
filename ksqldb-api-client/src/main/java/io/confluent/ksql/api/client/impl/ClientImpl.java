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

package io.confluent.ksql.api.client.impl;

import static io.confluent.ksql.api.client.impl.DdlDmlRequestValidators.validateExecuteStatementRequest;
import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.USER_AGENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ConnectorDescription;
import io.confluent.ksql.api.client.ConnectorInfo;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.PushOffsetVector;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.parsetools.RecordParser;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class ClientImpl implements Client {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final String QUERY_STREAM_ENDPOINT = "/query-stream";
  private static final String INSERTS_ENDPOINT = "/inserts-stream";
  private static final String CLOSE_QUERY_ENDPOINT = "/close-query";
  private static final String KSQL_ENDPOINT = "/ksql";
  private static final String INFO_ENDPOINT = "/info";

  private final ClientOptions clientOptions;
  private final Vertx vertx;
  private final HttpClient httpClient;
  private final SocketAddress serverSocketAddress;
  private final String basicAuthHeader;
  private final boolean ownedVertx;
  private final Map<String, Object> sessionVariables;
  private final Map<String, Object> requestProperties;
  private final AtomicReference<String> serializedConsistencyVector;
  private final AtomicReference<String> continuationToken;
  private final ClientImpl client;
  /**
   * {@code Client} instances should be created via {@link Client#create(ClientOptions)}, NOT via
   * this constructor.
   */

  public ClientImpl(final ClientOptions clientOptions) {
    this(clientOptions, Vertx.vertx(), true);
  }

  /**
   * {@code Client} instances should be created via {@link Client#create(ClientOptions, Vertx)},
   * NOT via this constructor.
   */
  public ClientImpl(final ClientOptions clientOptions, final Vertx vertx) {
    this(clientOptions, vertx, false);
  }

  private ClientImpl(final ClientOptions clientOptions, final Vertx vertx,
      final boolean ownedVertx) {
    this.clientOptions = clientOptions.copy();
    this.vertx = vertx;
    this.ownedVertx = ownedVertx;
    this.httpClient = createHttpClient(vertx, clientOptions);
    this.basicAuthHeader = createBasicAuthHeader(clientOptions);
    this.serverSocketAddress =
        SocketAddress.inetSocketAddress(clientOptions.getPort(), clientOptions.getHost());
    this.sessionVariables = new HashMap<>();
    this.serializedConsistencyVector = new AtomicReference<>("");
    this.continuationToken = new AtomicReference<>("");
    this.requestProperties = new HashMap<>();
    this.client = this;
  }

  @Override
  public CompletableFuture<StreamedQueryResult> streamQuery(final String sql) {
    return streamQuery(sql, new HashMap<>());
  }

  @Override
  public CompletableFuture<StreamedQueryResult> streamQuery(
      final String sql,
      final Map<String, Object> properties
  ) {
    if (PushOffsetVector.isContinuationTokenEnabled(properties)) {
      properties.put(
          KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED,
          true);
      if (!continuationToken.get().equalsIgnoreCase("")) {
        requestProperties.put(
            KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN,
            continuationToken.get());
      }
    }
    final CompletableFuture<StreamedQueryResult> cf = new CompletableFuture<>();
    makeQueryRequest(sql, properties, cf,
        (ctx, rp, fut, req) -> new StreamQueryResponseHandler(
            ctx, rp, fut, serializedConsistencyVector, continuationToken, sql, properties, client));
    return cf;
  }

  @Override
  public BatchedQueryResult executeQuery(final String sql) {
    return executeQuery(sql, Collections.emptyMap());
  }

  @Override
  public BatchedQueryResult executeQuery(
      final String sql,
      final Map<String, Object> properties
  ) {
    final BatchedQueryResult result = new BatchedQueryResultImpl();
    makeQueryRequest(
        sql,
        properties,
        result,
        (context, recordParser, cf, request) -> new ExecuteQueryResponseHandler(
            context, recordParser, cf, clientOptions.getExecuteQueryMaxResultRows(),
            serializedConsistencyVector)
    );
    return result;
  }

  @Override
  public CompletableFuture<Void> insertInto(final String streamName, final KsqlObject row) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();

    final Buffer requestBody = Buffer.buffer();
    final JsonObject params = new JsonObject().put("target", streamName);
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    requestBody.appendString(row.toJsonString()).appendString("\n");

    makePostRequest(
        INSERTS_ENDPOINT,
        requestBody,
        cf,
        response -> handleStreamedResponse(response, cf,
            (ctx, rp, fut, req) -> new InsertIntoResponseHandler(ctx, rp, fut))
    );

    return cf;
  }

  @Override
  public CompletableFuture<AcksPublisher> streamInserts(
      final String streamName,
      final Publisher<KsqlObject> insertsPublisher) {
    final CompletableFuture<AcksPublisher> cf = new CompletableFuture<>();

    final Buffer requestBody = Buffer.buffer();
    final JsonObject params = new JsonObject().put("target", streamName);
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");

    makePostRequest(
        "/inserts-stream",
        requestBody,
        cf,
        response -> handleStreamedResponse(response, cf,
            (ctx, rp, fut, req) ->
                new StreamInsertsResponseHandler(ctx, rp, fut, req, insertsPublisher)),
        false
    );

    return cf;
  }

  @Override
  public CompletableFuture<Void> terminatePushQuery(final String queryId) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();

    makePostRequest(
        CLOSE_QUERY_ENDPOINT,
        new JsonObject().put("queryId", queryId),
        cf,
        response -> handleCloseQueryResponse(response, cf)
    );

    return cf;
  }

  @Override
  public CompletableFuture<ExecuteStatementResult> executeStatement(final String sql) {
    return executeStatement(sql, Collections.emptyMap());
  }

  @Override
  public CompletableFuture<ExecuteStatementResult> executeStatement(
      final String sql, final Map<String, Object> properties) {
    final CompletableFuture<ExecuteStatementResult> cf = new CompletableFuture<>();

    if (!validateExecuteStatementRequest(sql, cf)) {
      return cf;
    }

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql", sql)
            .put("streamsProperties", properties)
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response,
            cf,
            DdlDmlResponseHandlers::handleExecuteStatementResponse,
            DdlDmlResponseHandlers::handleUnexpectedNumResponseEntities)
    );

    return cf;
  }

  @Override
  public CompletableFuture<List<StreamInfo>> listStreams() {
    final CompletableFuture<List<StreamInfo>> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject().put("ksql", "list streams;"),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, AdminResponseHandlers::handleListStreamsResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<List<TableInfo>> listTables() {
    final CompletableFuture<List<TableInfo>> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject().put("ksql", "list tables;"),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, AdminResponseHandlers::handleListTablesResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<List<TopicInfo>> listTopics() {
    final CompletableFuture<List<TopicInfo>> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject().put("ksql", "list topics;"),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, AdminResponseHandlers::handleListTopicsResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<List<QueryInfo>> listQueries() {
    final CompletableFuture<List<QueryInfo>> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject().put("ksql", "list queries;"),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, AdminResponseHandlers::handleListQueriesResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<SourceDescription> describeSource(final String sourceName) {
    final CompletableFuture<SourceDescription> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql", "describe " + sourceName + ";")
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, AdminResponseHandlers::handleDescribeSourceResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<ServerInfo> serverInfo() {
    final CompletableFuture<ServerInfo> cf = new CompletableFuture<>();

    makeGetRequest(
        INFO_ENDPOINT,
        new JsonObject(),
        cf,
        response -> handleObjectResponse(
            response, cf, AdminResponseHandlers::handleServerInfoResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<Void> createConnector(
      final String name,
      final boolean isSource,
      final Map<String, Object> properties
  ) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();
    final String connectorConfigs = properties.entrySet()
                .stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));
    final String type = isSource ? "SOURCE" : "SINK";

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql",
                String.format("CREATE %s CONNECTOR %s WITH (%s);", type, name, connectorConfigs))
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleCreateConnectorResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<Void> createConnector(
      final String name,
      final boolean isSource,
      final Map<String, Object> properties,
      final boolean ifNotExists
  ) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();
    final String connectorConfigs = properties.entrySet()
        .stream()
        .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
        .collect(Collectors.joining(","));
    final String type = isSource ? "SOURCE" : "SINK";
    final String ifNotExistsClause = ifNotExists ? "IF NOT EXISTS" : "";

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql",
                String.format("CREATE %s CONNECTOR %s %s WITH (%s);",
                    type, ifNotExistsClause, name, connectorConfigs))
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleCreateConnectorResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<Void> dropConnector(final String name) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql", "drop connector " + name + ";")
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleDropConnectorResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<Void> dropConnector(final String name, final boolean ifExists) {
    final CompletableFuture<Void> cf = new CompletableFuture<>();
    final String ifExistsClause = ifExists ? "if exists " : "";

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql", "drop connector " + ifExistsClause + name + ";")
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleDropConnectorResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<List<ConnectorInfo>> listConnectors() {
    final CompletableFuture<List<ConnectorInfo>> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject().put("ksql", "list connectors;"),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleListConnectorsResponse)
    );

    return cf;
  }

  @Override
  public CompletableFuture<ConnectorDescription> describeConnector(final String name) {
    final CompletableFuture<ConnectorDescription> cf = new CompletableFuture<>();

    makePostRequest(
        KSQL_ENDPOINT,
        new JsonObject()
            .put("ksql", "describe connector " + name + ";")
            .put("sessionVariables", sessionVariables),
        cf,
        response -> handleSingleEntityResponse(
            response, cf, ConnectorCommandResponseHandler::handleDescribeConnectorsResponse)
    );

    return cf;
  }

  @Override
  public void define(final String variable, final Object value) {
    sessionVariables.put(variable, value);
  }

  @Override
  public void undefine(final String variable) {
    sessionVariables.remove(variable);
  }

  @Override
  public Map<String, Object> getVariables() {
    return new HashMap<>(sessionVariables);
  }

  @VisibleForTesting
  public String getSerializedConsistencyVector() {
    return serializedConsistencyVector.get();
  }

  @Override
  public void close() {
    httpClient.close();
    if (ownedVertx) {
      vertx.close();
    }
  }

  @FunctionalInterface
  private interface StreamedResponseHandlerSupplier<T extends CompletableFuture<?>> {
    ResponseHandler<T> get(Context ctx, RecordParser recordParser, T cf, HttpClientRequest request);
  }

  @FunctionalInterface
  private interface SingleEntityResponseHandler<T> {
    void accept(JsonObject entity, CompletableFuture<T> cf);
  }

  private <T extends CompletableFuture<?>> void makeQueryRequest(
      final String sql,
      final Map<String, Object> properties,
      final T cf,
      final StreamedResponseHandlerSupplier<T> responseHandlerSupplier
  ) {
    final JsonObject requestBody = new JsonObject()
        .put("sql", sql)
        .put("properties", properties)
        .put("sessionVariables", sessionVariables)
        .put("requestProperties", requestProperties);

    makePostRequest(
        QUERY_STREAM_ENDPOINT,
        requestBody,
        cf,
        response -> handleStreamedResponse(response, cf, responseHandlerSupplier)
    );
  }

  @Override
  public HttpRequest buildRequest(final String method, final String path) {
    return new HttpRequestImpl(method, path, this);
  }

  CompletableFuture<HttpResponse> send(
      final HttpMethod method,
      final String path,
      final Map<String, Object> payload
  ) {
    final CompletableFuture<HttpResponse> cf = new CompletableFuture<>();

    final JsonObject jsonPayload = new JsonObject(payload)
        .put("sessionVariables", sessionVariables);

    makeRequest(
        path,
        jsonPayload.toBuffer(),
        cf,
        response -> handleResponse(response, cf),
        true,
        method
    );

    return cf;
  }

  private <T extends CompletableFuture<?>> void makeGetRequest(
      final String path,
      final JsonObject requestBody,
      final T cf,
      final Handler<HttpClientResponse> responseHandler) {
    makeRequest(path, requestBody.toBuffer(), cf, responseHandler, true, HttpMethod.GET);
  }

  private <T extends CompletableFuture<?>> void makePostRequest(
      final String path,
      final JsonObject requestBody,
      final T cf,
      final Handler<HttpClientResponse> responseHandler) {
    makePostRequest(path, requestBody.toBuffer(), cf, responseHandler);
  }

  private <T extends CompletableFuture<?>> void makePostRequest(
      final String path,
      final Buffer requestBody,
      final T cf,
      final Handler<HttpClientResponse> responseHandler) {
    makePostRequest(path, requestBody, cf, responseHandler, true);
  }

  private <T extends CompletableFuture<?>> void makePostRequest(
      final String path,
      final Buffer requestBody,
      final T cf,
      final Handler<HttpClientResponse> responseHandler,
      final boolean endRequest) {
    makeRequest(path, requestBody, cf, responseHandler, endRequest, HttpMethod.POST);
  }

  private <T extends CompletableFuture<?>> void makeRequest(
      final String path,
      final Buffer requestBody,
      final T cf,
      final Handler<HttpClientResponse> responseHandler,
      final boolean endRequest,
      final HttpMethod method) {
    HttpClientRequest request = httpClient.request(method,
        serverSocketAddress, clientOptions.getPort(), clientOptions.getHost(),
        path,
        responseHandler)
        .exceptionHandler(cf::completeExceptionally);
    request = configureUserAgent(request);
    if (clientOptions.isUseBasicAuth()) {
      request = configureBasicAuth(request);
    }
    if (clientOptions.getRequestHeaders() != null) {
      for (final Entry<String, String> entry : clientOptions.getRequestHeaders().entrySet()) {
        request.putHeader(entry.getKey(), entry.getValue());
      }
    }
    if (endRequest) {
      request.end(requestBody);
    } else {
      final HttpClientRequest finalRequest = request;
      finalRequest.sendHead(version -> finalRequest.writeCustomFrame(0, 0, requestBody));
    }
  }

  private HttpClientRequest configureBasicAuth(final HttpClientRequest request) {
    return request.putHeader(AUTHORIZATION.toString(), basicAuthHeader);
  }

  private HttpClientRequest configureUserAgent(final HttpClientRequest request) {
    final String clientVersion = AppInfo.getVersion();
    return request.putHeader(USER_AGENT.toString(), "ksqlDB Java Client v" + clientVersion);
  }

  private <T extends CompletableFuture<?>> void handleStreamedResponse(
      final HttpClientResponse response,
      final T cf,
      final StreamedResponseHandlerSupplier<T> responseHandlerSupplier) {
    if (response.statusCode() == OK.code()) {
      final RecordParser recordParser = RecordParser.newDelimited("\n", response);
      final ResponseHandler<T> responseHandler =
          responseHandlerSupplier.get(Vertx.currentContext(), recordParser, cf, response.request());
      recordParser.handler(responseHandler::handleBodyBuffer);
      recordParser.endHandler(responseHandler::handleBodyEnd);
      recordParser.exceptionHandler(responseHandler::handleException);
    } else {
      handleErrorResponse(response, cf);
    }
  }

  private static void handleCloseQueryResponse(
      final HttpClientResponse response,
      final CompletableFuture<Void> cf
  ) {
    if (response.statusCode() == OK.code()) {
      cf.complete(null);
    } else {
      handleErrorResponse(response, cf);
    }
  }

  private static <T> void handleSingleEntityResponse(
      final HttpClientResponse response,
      final CompletableFuture<T> cf,
      final SingleEntityResponseHandler<T> responseHandler
  ) {
    handleSingleEntityResponse(response, cf, responseHandler,
        numEntities -> new IllegalStateException(
            "Unexpected number of entities in server response: " + numEntities));
  }

  private static <T> void handleSingleEntityResponse(
      final HttpClientResponse response,
      final CompletableFuture<T> cf,
      final SingleEntityResponseHandler<T> responseHandler,
      final Function<Integer, RuntimeException> multipleEntityErrorSupplier
  ) {
    if (response.statusCode() == OK.code()) {
      response.bodyHandler(buffer -> {
        final JsonArray entities = buffer.toJsonArray();
        if (entities.size() != 1) {
          cf.completeExceptionally(multipleEntityErrorSupplier.apply(entities.size()));
          return;
        }

        final JsonObject entity;
        try {
          entity = entities.getJsonObject(0);
        } catch (Exception e) {
          cf.completeExceptionally(new IllegalStateException(
              "Unexpected server response format. Response: " + entities.getJsonObject(0)));
          return;
        }

        responseHandler.accept(entity, cf);
      });
    } else {
      handleErrorResponse(response, cf);
    }
  }

  private static <T> void handleObjectResponse(
      final HttpClientResponse response,
      final CompletableFuture<T> cf,
      final SingleEntityResponseHandler<T> responseHandler
  ) {
    if (response.statusCode() == OK.code()) {
      response.bodyHandler(buffer -> {
        final JsonObject entity = buffer.toJsonObject();
        responseHandler.accept(entity, cf);
      });
    } else {
      handleErrorResponse(response, cf);
    }
  }

  static void handleResponse(
      final HttpClientResponse httpResponse,
      final CompletableFuture<HttpResponse> cf
  ) {
    httpResponse.bodyHandler(
        buffer -> cf.complete(new HttpResponseImpl(httpResponse.statusCode(), buffer.getBytes()))
    );
    httpResponse.exceptionHandler(cf::completeExceptionally);
  }

  private static <T extends CompletableFuture<?>> void handleErrorResponse(
      final HttpClientResponse response,
      final T cf
  ) {
    response.bodyHandler(buffer -> {
      final JsonObject errorResponse = buffer.toJsonObject();
      cf.completeExceptionally(new KsqlClientException(String.format(
          "Received %d response from server: %s. Error code: %d",
          response.statusCode(),
          errorResponse.getString("message"),
          errorResponse.getInteger("error_code")
      )));
    });
  }

  private static HttpClient createHttpClient(final Vertx vertx, final ClientOptions clientOptions) {
    HttpClientOptions options = new HttpClientOptions()
        .setSsl(clientOptions.isUseTls())
        .setUseAlpn(clientOptions.isUseAlpn())
        .setProtocolVersion(HttpVersion.HTTP_2)
        .setHttp2ClearTextUpgrade(false)
        .setVerifyHost(clientOptions.isVerifyHost())
        .setDefaultHost(clientOptions.getHost())
        .setDefaultPort(clientOptions.getPort())
        .setHttp2MultiplexingLimit(clientOptions.getHttp2MultiplexingLimit());
    if (clientOptions.isUseTls() && !clientOptions.getTrustStore().isEmpty()) {
      final JksOptions jksOptions = VertxSslOptionsFactory.getJksTrustStoreOptions(
          clientOptions.getTrustStore(),
          clientOptions.getTrustStorePassword()
      );

      options = options.setTrustStoreOptions(jksOptions);
    }
    if (!clientOptions.getKeyStore().isEmpty()) {
      final JksOptions jksOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
          clientOptions.getKeyStore(),
          clientOptions.getKeyStorePassword(),
          Optional.of(clientOptions.getKeyPassword()),
          Optional.of(clientOptions.getKeyAlias())
      );

      options = options.setKeyStoreOptions(jksOptions);
    }
    return vertx.createHttpClient(options);
  }

  private static String createBasicAuthHeader(final ClientOptions clientOptions) {
    if (!clientOptions.isUseBasicAuth()) {
      return "";
    }

    final String creds = clientOptions.getBasicAuthUsername()
        + ":"
        + clientOptions.getBasicAuthPassword();
    final String base64creds =
        Base64.getEncoder().encodeToString(creds.getBytes(Charset.defaultCharset()));
    return "Basic " + base64creds;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClientImpl client = (ClientImpl) o;
    return clientOptions.equals(client.clientOptions)
        && vertx.equals(client.vertx);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientOptions, vertx);
  }

  @Override
  public String toString() {
    return "Client{"
        + "clientOptions=" + clientOptions
        + ", vertx=" + vertx
        + '}';
  }
}
