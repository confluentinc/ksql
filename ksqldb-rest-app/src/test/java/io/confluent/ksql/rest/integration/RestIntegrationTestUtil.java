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

package io.confluent.ksql.rest.integration;

import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpVersion.HTTP_1_1;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.WebsocketVersion;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class RestIntegrationTestUtil {

  private RestIntegrationTestUtil() {
  }

  public static HealthCheckResponse checkServerHealth(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient()) {

      final RestResponse<HealthCheckResponse> res = restClient.getServerHealth();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }

      return res.getResponse();
    }
  }

  public static List<KsqlEntity> makeKsqlRequest(final TestKsqlRestApp restApp, final String sql) {
    return makeKsqlRequest(restApp, sql, Optional.empty());
  }

  static List<KsqlEntity> makeKsqlRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);
      throwOnError(res);

      return awaitResults(restClient, res.getResponse());
    }
  }

  static KsqlErrorMessage makeKsqlRequestWithError(
      final TestKsqlRestApp restApp,
      final String sql
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

      throwOnNoError(res);

      return res.getErrorMessage();
    }
  }

  static ServerInfo makeInfoRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerInfo> res = restClient.getServerInfo();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static CommandStatus makeStatusRequest(final TestKsqlRestApp restApp, final String commandId) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<CommandStatus> res = restClient.getStatus(commandId);

      throwOnError(res);

      return res.getResponse();
    }
  }

  static CommandStatuses makeStatusesRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<CommandStatuses> res = restClient.getAllStatuses();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static ServerMetadata makeServerMetadataRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerMetadata> res = restClient.getServerMetadata();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static ServerClusterId makeServerMetadataIdRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerClusterId> res = restClient.getServerMetadataId();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static List<StreamedRow> makeQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds
  ) {
    return makeQueryRequest(restApp, sql, userCreds, null,
        ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true));
  }

  static List<StreamedRow> makeQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds,
      final Map<String, ?> properties,
      final Map<String, ?> requestProperties
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<List<StreamedRow>> res =
          restClient.makeQueryRequest(sql, null, properties, requestProperties);

      throwOnError(res);

      return res.getResponse();
    }
  }

  static KsqlErrorMessage makeQueryRequestWithError(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds,
      final Map<String, ?> properties
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<List<StreamedRow>> res =
          restClient.makeQueryRequest(sql, null, properties, Collections.emptyMap());

      throwOnNoError(res);

      return res.getErrorMessage();
    }
  }

  /**
   * Make a query request using a basic Http client.
   *
   * @param restApp the test app instance to issue the request against
   * @param sql the sql payload
   * @param mediaType the type of media the client can accept
   * @return the response payload
   */
  static HttpResponse<Buffer> rawRestQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final String mediaType
  ) {
    final KsqlRequest request =
        new KsqlRequest(sql, ImmutableMap.of(), ImmutableMap.of(), null);

      return rawRestRequest(restApp, HTTP_1_1, POST, "/query", request, mediaType,
          Optional.empty());
  }

  static HttpResponse<Buffer> rawRestRequest(
      final TestKsqlRestApp restApp,
      final HttpVersion httpVersion,
      final HttpMethod method,
      final String uri,
      final Object requestBody
  ) {
      return rawRestRequest(
          restApp,
          httpVersion,
          method,
          uri,
          requestBody,
          "application/json",
          Optional.empty()
      );
  }

  static HttpResponse<Buffer> rawRestRequest(
      final TestKsqlRestApp restApp,
      final HttpVersion httpVersion,
      final HttpMethod method,
      final String uri,
      final Object requestBody,
      final String mediaType,
      final Optional<WriteStream<Buffer>> writeStream
  ) {
    Vertx vertx = Vertx.vertx();
    WebClient webClient = null;
    try {
      WebClientOptions webClientOptions = new WebClientOptions()
          .setDefaultHost(restApp.getHttpListener().getHost())
          .setDefaultPort(restApp.getHttpListener().getPort())
          .setFollowRedirects(false);

      if (httpVersion == HttpVersion.HTTP_2) {
        webClientOptions.setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false);
      }

      webClient = WebClient.create(vertx, webClientOptions);
      return rawRestRequest(
          vertx,
          webClient,
          restApp,
          httpVersion,
          method,
          uri,
          requestBody,
          mediaType,
          writeStream
      );
    } finally {
      if (webClient != null) {
        webClient.close();
      }
      vertx.close();
    }
  }

  static HttpResponse<Buffer> rawRestRequest(
      final Vertx vertx,
      final WebClient webClient,
      final TestKsqlRestApp restApp,
      final HttpVersion httpVersion,
      final HttpMethod method,
      final String uri,
      final Object requestBody,
      final String mediaType,
      final Optional<WriteStream<Buffer>> writeStream
  ) {
    try {
      byte[] bytes = ApiJsonMapper.INSTANCE.get().writeValueAsBytes(requestBody);

      Buffer bodyBuffer = Buffer.buffer(bytes);

      // When:
      VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
      HttpRequest<Buffer> request = webClient
          .request(method, uri)
          .putHeader(CONTENT_TYPE.toString(), "application/json")
          .putHeader(ACCEPT.toString(), mediaType);
      if (bodyBuffer != null) {
        request.sendBuffer(bodyBuffer, requestFuture);
      } else {
        request.send(requestFuture);
      }
      writeStream.ifPresent(s -> request.as(BodyCodec.pipe(s)));
      return requestFuture.get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void createStream(
      final TestKsqlRestApp restApp,
      final TestDataProvider dataProvider
  ) {
    createStream(restApp, dataProvider, Optional.empty());
  }

  public static void createStream(
      final TestKsqlRestApp restApp,
      final TestDataProvider dataProvider,
      final Optional<BasicCredentials> userCreds
  ) {
    createSource(restApp, dataProvider, false, userCreds);
  }

  public static void createTable(
      final TestKsqlRestApp restApp,
      final TestDataProvider dataProvider
  ) {
    createSource(restApp, dataProvider, true, Optional.empty());
  }

  public static void createSource(
      final TestKsqlRestApp restApp,
      final TestDataProvider dataProvider,
      final boolean table,
      final Optional<BasicCredentials> userCreds
  ) {
    makeKsqlRequest(
        restApp,
        "CREATE " + (table ? "TABLE" : "STREAM") + " " + dataProvider.sourceName()
            + " (" + dataProvider.ksqlSchemaString(table) + ") "
            + "WITH (kafka_topic='" + dataProvider.topicName() + "', value_format='json');",
        userCreds
    );
  }

  private static List<KsqlEntity> awaitResults(
      final KsqlRestClient ksqlRestClient,
      final List<KsqlEntity> pending
  ) {
    return pending.stream()
        .map(e -> awaitResult(e, ksqlRestClient))
        .collect(Collectors.toList());
  }

  private static KsqlEntity awaitResult(
      final KsqlEntity e,
      final KsqlRestClient ksqlRestClient
  ) {
    if (!(e instanceof CommandStatusEntity)) {
      return e;
    }
    CommandStatusEntity cse = (CommandStatusEntity) e;
    final String commandId = cse.getCommandId().toString();

    while (cse.getCommandStatus().getStatus() != Status.ERROR
        && cse.getCommandStatus().getStatus() != Status.SUCCESS
        && cse.getCommandStatus().getStatus() != Status.QUEUED) {

      final RestResponse<CommandStatus> res = ksqlRestClient.makeStatusRequest(commandId);

      throwOnError(res);

      cse = new CommandStatusEntity(
          cse.getStatementText(),
          cse.getCommandId(),
          res.getResponse(),
          cse.getCommandSequenceNumber()
      );
    }

    return cse;
  }

  private static void throwOnError(final RestResponse<?> res) {
    if (res.isErroneous()) {
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }
  }

  private static void throwOnNoError(final RestResponse<?> res) {
    if (!res.isErroneous()) {
      throw new AssertionError("Failed to get erroneous result.");
    }
  }

  public static List<String> makeWsRequest(
      final URI baseUri,
      final String sql,
      final Optional<String> mediaType,
      final Optional<String> contentType,
      final Optional<Credentials> credentials
  ) {
    Vertx vertx = Vertx.vertx();
    HttpClient httpClient = null;
    try {
      httpClient = vertx.createHttpClient();

      final String uri = baseUri.toString() + "/ws/query?request=" + buildStreamingRequest(sql);

      final MultiMap headers = MultiMap.caseInsensitiveMultiMap();

      credentials.ifPresent(
          creds -> headers.add(AUTHORIZATION.toString(), "Basic " + buildBasicAuthHeader(creds)));

      mediaType.ifPresent(mt -> headers.add(ACCEPT.toString(), mt));
      contentType.ifPresent(ct -> headers.add(CONTENT_TYPE.toString(), ct));

      CompletableFuture<List<String>> completableFuture = new CompletableFuture<>();

      httpClient.webSocketAbs(uri, headers, WebsocketVersion.V07,
          Collections.emptyList(), ar -> {
            if (ar.succeeded()) {
              List<String> messages = new ArrayList<>();
              ar.result().frameHandler(frame -> {
                if (frame.isText()) {
                  messages.add(frame.textData());
                }
              });
              ar.result().endHandler(v -> {
                completableFuture.complete(messages);
              });
            } else {
              completableFuture.completeExceptionally(ar.cause());
            }
          });
      return completableFuture.get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
      vertx.close();
    }
  }

  private static String buildBasicAuthHeader(final Credentials credentials) {
    final String creds = credentials.username + ":" + credentials.password;
    return Base64.getEncoder().encodeToString(creds.getBytes(Charset.defaultCharset()));
  }

  private static String buildStreamingRequest(final String sql) {
    return UrlEscapers.urlFormParameterEscaper()
        .escape("{"
            + " \"ksql\": \"" + sql + "\""
            + "}");
  }
}
