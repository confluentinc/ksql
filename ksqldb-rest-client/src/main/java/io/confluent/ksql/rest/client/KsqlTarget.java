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
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"WeakerAccess", "checkstyle:ClassDataAbstractionCoupling"}) // Public API
public final class KsqlTarget {

  private static final Logger log = LoggerFactory.getLogger(KsqlTarget.class);

  private static final String STATUS_PATH = "/status";
  private static final String KSQL_PATH = "/ksql";
  private static final String QUERY_PATH = "/query";
  private static final String HEARTBEAT_PATH = "/heartbeat";
  private static final String CLUSTERSTATUS_PATH = "/clusterStatus";
  private static final String LAG_REPORT_PATH = "/lag";
  private static final String SERVER_METADATA_PATH = "/v1/metadata";
  private static final String SERVER_METADATA_ID_PATH = "/v1/metadata/id";

  private final HttpClient httpClient;
  private final SocketAddress socketAddress;
  private final LocalProperties localProperties;
  private final Optional<String> authHeader;
  private final String host;

  /**
   * Create a KsqlTarget containing all of the connection information required to make a request
   * @param httpClient The HttpClient to use to make the request
   * @param socketAddress The SocketAddress to use for connecting to the remote host
   * @param localProperties Properties sent with ksql requests
   * @param authHeader Optional auth headers
   * @param host The hostname to use for the request, used to set the host header of the request
   */
  KsqlTarget(
      final HttpClient httpClient,
      final SocketAddress socketAddress,
      final LocalProperties localProperties,
      final Optional<String> authHeader,
      final String host
  ) {
    this.httpClient = requireNonNull(httpClient, "httpClient");
    this.socketAddress = requireNonNull(socketAddress, "socketAddress");
    this.localProperties = requireNonNull(localProperties, "localProperties");
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.host = host;
  }

  public KsqlTarget authorizationHeader(final String authHeader) {
    return new KsqlTarget(httpClient, socketAddress, localProperties,
        Optional.of(authHeader), host);
  }

  public KsqlTarget properties(final Map<String, ?> properties) {
    return new KsqlTarget(httpClient, socketAddress,
        new LocalProperties(properties),
        authHeader, host);
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return get("/info", ServerInfo.class);
  }

  public RestResponse<HealthCheckResponse> getServerHealth() {
    return get("/healthcheck", HealthCheckResponse.class);
  }

  public CompletableFuture<RestResponse<HeartbeatResponse>> postAsyncHeartbeatRequest(
      final KsqlHostInfoEntity host,
      final long timestamp
  ) {
    return executeRequestAsync(
        HttpMethod.POST,
        HEARTBEAT_PATH,
        new HeartbeatMessage(host, timestamp),
        r -> deserialize(r.getBody(), HeartbeatResponse.class)
    );
  }

  public RestResponse<ClusterStatusResponse> getClusterStatus() {
    return get(CLUSTERSTATUS_PATH, ClusterStatusResponse.class);
  }

  public CompletableFuture<RestResponse<LagReportingResponse>> postAsyncLagReportingRequest(
      final LagReportingMessage lagReportingMessage
  ) {
    return executeRequestAsync(
        HttpMethod.POST,
        LAG_REPORT_PATH,
        lagReportingMessage,
        r -> deserialize(r.getBody(), LagReportingResponse.class)
    );
  }

  public RestResponse<CommandStatuses> getStatuses() {
    return get(STATUS_PATH, CommandStatuses.class);
  }

  public RestResponse<CommandStatus> getStatus(final String commandId) {
    return get(STATUS_PATH + "/" + commandId, CommandStatus.class);
  }

  public RestResponse<ServerMetadata> getServerMetadata() {
    return get(SERVER_METADATA_PATH, ServerMetadata.class);
  }

  public RestResponse<ServerClusterId> getServerMetadataId() {
    return get(SERVER_METADATA_ID_PATH, ServerClusterId.class);
  }

  public RestResponse<KsqlEntityList> postKsqlRequest(
      final String ksql,
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        KSQL_PATH,
        createKsqlRequest(ksql, requestProperties, previousCommandSeqNum),
        r -> deserialize(r.getBody(), KsqlEntityList.class)
    );
  }

  public RestResponse<Integer> postQueryRequest(
      final String ksql,
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum,
      final Consumer<List<StreamedRow>> rowConsumer
  ) {
    final AtomicInteger rowCount = new AtomicInteger(0);
    return post(
        QUERY_PATH,
        createKsqlRequest(ksql, requestProperties, previousCommandSeqNum),
        rowCount::get,
        rows -> {
          final List<StreamedRow> streamedRows = toRows(rows);
          rowCount.addAndGet(streamedRows.size());
          return streamedRows;
        },
        rowConsumer);
  }

  public RestResponse<List<StreamedRow>> postQueryRequest(
      final String ksql,
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        createKsqlRequest(ksql, requestProperties, previousCommandSeqNum),
        KsqlTarget::toRows);
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
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return new KsqlRequest(
        ksql,
        localProperties.toMap(),
        requestProperties,
        previousCommandSeqNum.orElse(null)
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

  private <R, T> RestResponse<R> post(
      final String path,
      final Object jsonEntity,
      final Supplier<R> responseSupplier,
      final Function<Buffer, T> mapper,
      final Consumer<T> chunkHandler
  ) {
    return executeRequestSync(HttpMethod.POST, path, jsonEntity, responseSupplier, mapper,
        chunkHandler);
  }

  private <T> CompletableFuture<RestResponse<T>> executeRequestAsync(
      final HttpMethod httpMethod,
      final String path,
      final Object jsonEntity,
      final Function<ResponseWithBody, T> mapper
  ) {
    return executeAsync(httpMethod, path, jsonEntity, mapper, (resp, vcf) -> {
      resp.bodyHandler(buff -> vcf.complete(new ResponseWithBody(resp, buff)));
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

  private <R, T> RestResponse<R> executeRequestSync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Supplier<R> responseSupplier,
      final Function<Buffer, T> chunkMapper,
      final Consumer<T> chunkHandler
  ) {
    return executeSync(httpMethod, path, requestBody, resp -> responseSupplier.get(),
        (resp, vcf) -> {
        resp.handler(buff -> {
          try {
            chunkHandler.accept(chunkMapper.apply(buff));
          } catch (Throwable t) {
            log.error("Error while handling chunk", t);
            vcf.completeExceptionally(t);
          }
        });
        resp.endHandler(v -> {
          try {
            chunkHandler.accept(null);
            vcf.complete(new ResponseWithBody(resp, Buffer.buffer()));
          } catch (Throwable t) {
            log.error("Error while handling end", t);
            vcf.completeExceptionally(t);
          }
        });
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

  private <T> CompletableFuture<RestResponse<T>> executeAsync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final CompletableFuture<ResponseWithBody> vcf =
        execute(httpMethod, path, requestBody, responseHandler);
    return vcf.thenApply(response -> KsqlClientUtil.toRestResponse(response, path, mapper));
  }

  private CompletableFuture<ResponseWithBody> execute(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final VertxCompletableFuture<ResponseWithBody> vcf = new VertxCompletableFuture<>();

    final HttpClientRequest httpClientRequest = httpClient.request(httpMethod,
        socketAddress, socketAddress.port(), host,
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
    return toRows(resp.getBody());
  }

  // This is meant to parse partial chunk responses as well as full pull query responses.
  private static List<StreamedRow> toRows(final Buffer buff) {

    final List<StreamedRow> rows = new ArrayList<>();
    int begin = 0;

    for (int i = 0; i <= buff.length(); i++) {
      if ((i == buff.length() && (i - begin > 1))
          || (i < buff.length() && buff.getByte(i) == (byte) '\n')) {
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
