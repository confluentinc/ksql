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

package io.confluent.ksql.rest.client;

import static io.confluent.ksql.rest.client.KsqlClientUtil.deserialize;
import static io.confluent.ksql.rest.client.KsqlClientUtil.serialize;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlTarget {

  private static final Logger log = LoggerFactory.getLogger(KsqlTarget.class);

  private static final String STATUS_PATH = "/status";
  private static final String KSQL_PATH = "/ksql";
  private static final String QUERY_PATH = "/query";
  private static final String HEARTBEAT_PATH = "/heartbeat";
  private static final String CLUSTERSTATUS_PATH = "/clusterStatus";
  private static final String LAG_REPORT_PATH = "/lag";

  private final HttpClient httpClient;
  private final SocketAddress socketAddress;
  private final LocalProperties localProperties;
  private final Optional<String> authHeader;

  KsqlTarget(
      final HttpClient httpClient,
      final SocketAddress socketAddress,
      final LocalProperties localProperties,
      final Optional<String> authHeader
  ) {
    this.httpClient = requireNonNull(httpClient, "httpClient");
    this.socketAddress = requireNonNull(socketAddress, "socketAddress");
    this.localProperties = requireNonNull(localProperties, "localProperties");
    this.authHeader = requireNonNull(authHeader, "authHeader");
  }

  public KsqlTarget authorizationHeader(final String authHeader) {
    return new KsqlTarget(httpClient, socketAddress, localProperties,
        Optional.of(authHeader));
  }

  public KsqlTarget properties(final Map<String, ?> properties) {
    return new KsqlTarget(httpClient, socketAddress,
        new LocalProperties(properties),
        authHeader);
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return get("/info", ServerInfo.class);
  }

  public RestResponse<HealthCheckResponse> getServerHealth() {
    return get("/healthcheck", HealthCheckResponse.class);
  }

  public void postAsyncHeartbeatRequest(
      final KsqlHostInfoEntity host,
      final long timestamp
  ) {
    executeRequestAsync(
        HEARTBEAT_PATH,
        new HeartbeatMessage(host, timestamp)
    );
  }

  public RestResponse<ClusterStatusResponse> getClusterStatus() {
    return get(CLUSTERSTATUS_PATH, ClusterStatusResponse.class);
  }

  public void postAsyncLagReportingRequest(
      final LagReportingMessage lagReportingMessage
  ) {
    executeRequestAsync(
        LAG_REPORT_PATH,
        lagReportingMessage
    );
  }

  public RestResponse<CommandStatuses> getStatuses() {
    return get(STATUS_PATH, CommandStatuses.class);
  }

  public RestResponse<CommandStatus> getStatus(final String commandId) {
    return get(STATUS_PATH + "/" + commandId, CommandStatus.class);
  }

  public RestResponse<KsqlEntityList> postKsqlRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        KSQL_PATH,
        createKsqlRequest(ksql, Collections.emptyMap(), previousCommandSeqNum),
        r -> deserialize(r.getBody(), KsqlEntityList.class)
    );
  }

  public RestResponse<List<StreamedRow>> postQueryRequest(
      final String ksql,
      final Map<String, ?> serverProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        createKsqlRequest(ksql, Collections.emptyMap(), previousCommandSeqNum),
        KsqlTarget::toRows
    );
  }

  public RestResponse<StreamPublisher<StreamedRow>> postQueryRequestStreamed(
      final String sql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return executeQueryRequestWithStreamResponse(sql, previousCommandSeqNum,
        buff -> deserialize(buff, StreamedRow.class));
  }

  public RestResponse<StreamPublisher<String>> postPrintTopicRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return executeQueryRequestWithStreamResponse(ksql, previousCommandSeqNum, Object::toString);
  }

  private KsqlRequest createKsqlRequest(
      final String ksql,
      final Map<String, ?> serverProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return new KsqlRequest(
        ksql,
        localProperties.toMap(),
        serverProperties,
        previousCommandSeqNum.orElse(null),
        false
    );
  }

  private <T> RestResponse<T> get(final String path, final Class<T> type) {
    return executeRequestSync(HttpMethod.GET, path, null, r -> deserialize(r.getBody(), type));
  }

  private <T> RestResponse<T> post(
      final String path,
      final Object jsonEntity,
      final Function<ResponseWithBody, T> mapper
  ) {
    return executeRequestSync(HttpMethod.POST, path, jsonEntity, mapper);
  }

  private void executeRequestAsync(
      final String path,
      final Object jsonEntity
  ) {
    execute(HttpMethod.POST, path, jsonEntity, (resp, vcf) -> {
    }).exceptionally(t -> {
      log.error("Unexpected exception in async request", t);
      return null;
    });
  }

  private <T> RestResponse<T> executeRequestSync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper
  ) {
    return executeSync(httpMethod, path, requestBody, mapper, (resp, vcf) -> {
      resp.bodyHandler(buff -> vcf.complete(new ResponseWithBody(resp, buff)));
    });
  }

  private <T> RestResponse<StreamPublisher<T>> executeQueryRequestWithStreamResponse(
      final String ksql,
      final Optional<Long> previousCommandSeqNum,
      final Function<Buffer, T> mapper
  ) {
    final KsqlRequest ksqlRequest = createKsqlRequest(
        ksql, Collections.emptyMap(), previousCommandSeqNum);
    final AtomicReference<StreamPublisher<T>> pubRef = new AtomicReference<>();
    return executeSync(HttpMethod.POST, QUERY_PATH, ksqlRequest, resp -> pubRef.get(),
        (resp, vcf) -> {
          if (resp.statusCode() == 200) {
            pubRef.set(new StreamPublisher<>(Vertx.currentContext(),
                resp, mapper, vcf));
            vcf.complete(new ResponseWithBody(resp));
          } else {
            resp.bodyHandler(body -> vcf.complete(new ResponseWithBody(resp, body)));
          }
        });
  }

  private <T> RestResponse<T> executeSync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final CompletableFuture<ResponseWithBody> vcf =
        execute(httpMethod, path, requestBody, responseHandler);

    final ResponseWithBody response;
    try {
      response = vcf.get();
    } catch (Exception e) {
      throw new KsqlRestClientException(
          "Error issuing " + httpMethod + " to KSQL server. path:" + path, e);
    }
    return KsqlClientUtil.toRestResponse(response, path, mapper);
  }

  private CompletableFuture<ResponseWithBody> execute(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final VertxCompletableFuture<ResponseWithBody> vcf = new VertxCompletableFuture<>();

    final HttpClientRequest httpClientRequest = httpClient.request(httpMethod,
        socketAddress, socketAddress.port(), socketAddress.host(),
        path,
        resp -> responseHandler.accept(resp, vcf))
        .exceptionHandler(vcf::completeExceptionally);

    httpClientRequest.putHeader("Accept", "application/json");
    authHeader.ifPresent(v -> httpClientRequest.putHeader("Authorization", v));

    if (requestBody != null) {
      httpClientRequest.end(serialize(requestBody));
    } else {
      httpClientRequest.end();
    }

    return vcf;
  }

  private static List<StreamedRow> toRows(final ResponseWithBody resp) {

    final List<StreamedRow> rows = new ArrayList<>();
    final Buffer buff = resp.getBody();
    int begin = 0;

    for (int i = 0; i <= buff.length(); i++) {
      if ((i == buff.length() && (i - begin > 1)) || buff.getByte(i) == (byte) '\n') {
        if (begin != i) { // Ignore random newlines - the server can send these
          final Buffer sliced = buff.slice(begin, i);
          final Buffer tidied = StreamPublisher.toJsonMsg(sliced);
          final StreamedRow row = deserialize(tidied, StreamedRow.class);
          rows.add(row);
        }

        begin = i + 1;
      }
    }

    return rows;
  }

}
