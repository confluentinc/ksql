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

import com.google.common.base.Functions;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.WriteStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"WeakerAccess", "checkstyle:ClassDataAbstractionCoupling"}) // Public API
public final class KsqlTarget {

  private static final Logger log = LoggerFactory.getLogger(KsqlTarget.class);

  private static final String STATUS_PATH = "/status";
  private static final String KSQL_PATH = "/ksql";
  private static final String QUERY_PATH = "/query";
  private static final String QUERY_STREAM_PATH = "/query-stream";
  private static final String HEARTBEAT_PATH = "/heartbeat";
  private static final String CLUSTERSTATUS_PATH = "/clusterStatus";
  private static final String LAG_REPORT_PATH = "/lag";
  private static final String SERVER_METADATA_PATH = "/v1/metadata";
  private static final String SERVER_METADATA_ID_PATH = "/v1/metadata/id";
  private static final String IS_VALID_PATH = "/is_valid_property/";

  private final HttpClient httpClient;
  private final SocketAddress socketAddress;
  private final LocalProperties localProperties;
  private final Optional<String> authHeader;
  private final String host;
  private final String subPath;
  private final Map<String, String> additionalHeaders;

  /**
   * Create a KsqlTarget containing all of the connection information required to make a request
   * @param httpClient The HttpClient to use to make the request
   * @param socketAddress The SocketAddress to use for connecting to the remote host
   * @param localProperties Properties sent with ksql requests
   * @param authHeader Optional auth headers
   * @param host The hostname to use for the request, used to set the host header of the request
   * @param subPath Optional path that can be provided with server name
   */
  KsqlTarget(
      final HttpClient httpClient,
      final SocketAddress socketAddress,
      final LocalProperties localProperties,
      final Optional<String> authHeader,
      final String host,
      final String subPath,
      final Map<String, String> additionalHeaders
  ) {
    this.httpClient = requireNonNull(httpClient, "httpClient");
    this.socketAddress = requireNonNull(socketAddress, "socketAddress");
    this.localProperties = requireNonNull(localProperties, "localProperties");
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.host = host;
    this.subPath = subPath.replaceAll("/\\z", "");
    this.additionalHeaders = requireNonNull(additionalHeaders, "additionalHeaders");
  }

  public KsqlTarget authorizationHeader(final String authHeader) {
    return new KsqlTarget(httpClient, socketAddress, localProperties,
        Optional.of(authHeader), host, subPath, additionalHeaders);
  }

  public KsqlTarget properties(final Map<String, ?> properties) {
    return new KsqlTarget(httpClient, socketAddress,
        new LocalProperties(properties),
        authHeader, host, subPath, additionalHeaders);
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

  public RestResponse<Boolean> getIsValidRequest(final String propertyName) {
    return get(IS_VALID_PATH + propertyName, Boolean.class);
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
      final WriteStream<List<StreamedRow>> rowConsumer,
      final CompletableFuture<Void> shouldCloseConnection,
      final Function<StreamedRow, StreamedRow> addHostInfo
  ) {
    final AtomicInteger rowCount = new AtomicInteger(0);
    return post(
        QUERY_PATH,
        createKsqlRequest(ksql, requestProperties, previousCommandSeqNum),
        rowCount::get,
        rows -> {
          final List<StreamedRow> streamedRows = KsqlTargetUtil.toRows(rows, addHostInfo);
          rowCount.addAndGet(streamedRows.size());
          return streamedRows;
        },
        "\n", // delimiter
        rowConsumer,
        shouldCloseConnection);
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

  public RestResponse<List<StreamedRow>> postQueryStreamRequestProto(
          final String ksql,
          final Map<String, Object> requestProperties
  ) {
    final QueryStreamArgs queryStreamArgs = new QueryStreamArgs(ksql, localProperties.toMap(),
            Collections.emptyMap(), requestProperties);
    return executeRequestSync(HttpMethod.POST,
            QUERY_STREAM_PATH,
            queryStreamArgs,
            KsqlTarget::toRowsFromProto,
            Optional.of(KsqlMediaType.KSQL_V1_PROTOBUF.mediaType()));
  }

  public RestResponse<StreamPublisher<StreamedRow>> postQueryRequestStreamed(
      final String sql,
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum
  ) {
    return executeQueryRequestWithStreamResponse(sql, requestProperties, previousCommandSeqNum,
        buff -> deserialize(buff, StreamedRow.class));
  }

  public CompletableFuture<RestResponse<StreamPublisher<StreamedRow>>>
      postQueryRequestStreamedAsync(
      final String sql,
      final Map<String, ?> requestProperties
  ) {
    return executeQueryStreamRequest(sql, requestProperties,
        KsqlTargetUtil::toRowFromDelimited);
  }

  public RestResponse<StreamPublisher<String>> postPrintTopicRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return executeQueryRequestWithStreamResponse(ksql, Collections.emptyMap(),
        previousCommandSeqNum, Object::toString);
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
    return executeRequestSync(HttpMethod.GET,
            path,
            null,
            r -> deserialize(r.getBody(), type),
            Optional.empty());
  }

  private <T> RestResponse<T> post(
      final String path,
      final Object jsonEntity,
      final Function<ResponseWithBody, T> mapper
  ) {
    return executeRequestSync(HttpMethod.POST, path, jsonEntity, mapper, Optional.empty());
  }

  private <R, T> RestResponse<R> post(
      final String path,
      final Object jsonEntity,
      final Supplier<R> responseSupplier,
      final Function<Buffer, T> mapper,
      final String delimiter,
      final WriteStream<T> chunkHandler,
      final CompletableFuture<Void> shouldCloseConnection
  ) {
    return executeRequestSync(HttpMethod.POST, path, jsonEntity, responseSupplier, mapper,
        delimiter, chunkHandler, shouldCloseConnection);
  }

  private <T> CompletableFuture<RestResponse<T>> executeRequestAsync(
      final HttpMethod httpMethod,
      final String path,
      final Object jsonEntity,
      final Function<ResponseWithBody, T> mapper
  ) {
    return executeAsync(httpMethod, path, Optional.empty(), jsonEntity, mapper, (resp, vcf) -> {
      resp.bodyHandler(buff -> vcf.complete(new ResponseWithBody(resp, buff)));
    });
  }

  private <T> RestResponse<T> executeRequestSync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper,
      final Optional<String> mediaType
  ) {
    return executeSync(httpMethod, path, mediaType, requestBody, mapper, (resp, vcf) -> {
      resp.bodyHandler(buff -> vcf.complete(new ResponseWithBody(resp, buff)));
    });
  }

  private <R, T> RestResponse<R> executeRequestSync(
      final HttpMethod httpMethod,
      final String path,
      final Object requestBody,
      final Supplier<R> responseSupplier,
      final Function<Buffer, T> chunkMapper,
      final String delimiter,
      final WriteStream<T> chunkHandler,
      final CompletableFuture<Void> shouldCloseConnection
  ) {
    return executeSync(httpMethod, path, Optional.empty(), requestBody,
        resp -> responseSupplier.get(),
        (resp, vcf) -> {
        final RecordParser recordParser = RecordParser.newDelimited(delimiter, resp);
        final AtomicBoolean end = new AtomicBoolean(false);

        final WriteStream<Buffer> ws = new BufferMapWriteStream<>(chunkMapper, chunkHandler);
        recordParser.exceptionHandler(vcf::completeExceptionally);
        // don't end the stream on successful queries as the write stream is potentially
        // reused by multiple read streams
        recordParser.pipe().endOnSuccess(false).to(ws, ar -> {
          end.set(true);
          if (ar.succeeded()) {
            vcf.complete(new ResponseWithBody(resp, Buffer.buffer()));
          }
          if (ar.failed()) {
            log.error("Error while handling response.", ar.cause());
            vcf.completeExceptionally(ar.cause());
          }
        });

        // Closing after the end handle was called resulted in errors about the connection being
        // closed, so we even turn this on the context so there's no race.
        final Context context = Vertx.currentContext();
        shouldCloseConnection.handle((v, t) -> {
          context.runOnContext(v2 -> {
            if (!end.get()) {
              try {
                resp.request().connection().close();
                vcf.completeExceptionally(new KsqlRestClientException("Closing connection"));
              } catch (Throwable closing) {
                log.error("Error while handling close", closing);
                vcf.completeExceptionally(closing);
              }
            }
          });
          return null;
        });
      });
  }

  private <T> RestResponse<StreamPublisher<T>> executeQueryRequestWithStreamResponse(
      final String ksql,
      final Map<String, ?> requestProperties,
      final Optional<Long> previousCommandSeqNum,
      final Function<Buffer, T> mapper
  ) {
    final KsqlRequest ksqlRequest = createKsqlRequest(
        ksql, requestProperties, previousCommandSeqNum);
    final AtomicReference<StreamPublisher<T>> pubRef = new AtomicReference<>();
    return executeSync(HttpMethod.POST, QUERY_PATH, Optional.empty(), ksqlRequest,
        resp -> pubRef.get(),
        (resp, vcf) -> {
          if (resp.statusCode() == 200) {
            pubRef.set(new StreamPublisher<>(Vertx.currentContext(),
                resp, mapper, vcf, true));
            vcf.complete(new ResponseWithBody(resp));
          } else {
            resp.bodyHandler(body -> vcf.complete(new ResponseWithBody(resp, body)));
          }
        });
  }

  private <T> CompletableFuture<RestResponse<StreamPublisher<T>>> executeQueryStreamRequest(
      final String ksql,
      final Map<String, ?> requestProperties,
      final Function<Buffer, T> mapper
  ) {
    final Map<String, Object> requestPropertiesObject = requestProperties.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    final QueryStreamArgs queryStreamArgs = new QueryStreamArgs(ksql, localProperties.toMap(),
        Collections.emptyMap(), requestPropertiesObject);
    final AtomicReference<StreamPublisher<T>> pubRef = new AtomicReference<>();
    return executeAsync(HttpMethod.POST, QUERY_STREAM_PATH,
        Optional.of("application/vnd.ksqlapi.delimited.v1"), queryStreamArgs,
        resp -> pubRef.get(),
        (resp, vcf) -> {
          if (resp.statusCode() == 200) {
            pubRef.set(new StreamPublisher<>(Vertx.currentContext(),
                resp, mapper, vcf, false));
            vcf.complete(new ResponseWithBody(resp));
          } else {
            resp.bodyHandler(body -> vcf.complete(new ResponseWithBody(resp, body)));
          }
        });
  }

  private <T> RestResponse<T> executeSync(
      final HttpMethod httpMethod,
      final String path,
      final Optional<String> mediaType,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final CompletableFuture<ResponseWithBody> vcf =
        execute(httpMethod, path, mediaType, requestBody, responseHandler);

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
      final Optional<String> mediaType,
      final Object requestBody,
      final Function<ResponseWithBody, T> mapper,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final CompletableFuture<ResponseWithBody> vcf =
        execute(httpMethod, path, mediaType, requestBody, responseHandler);
    return vcf.thenApply(response -> KsqlClientUtil.toRestResponse(response, path, mapper));
  }

  private CompletableFuture<ResponseWithBody> execute(
      final HttpMethod httpMethod,
      final String path,
      final Optional<String> mediaType,
      final Object requestBody,
      final BiConsumer<HttpClientResponse, CompletableFuture<ResponseWithBody>> responseHandler
  ) {
    final VertxCompletableFuture<ResponseWithBody> vcf = new VertxCompletableFuture<>();

    final RequestOptions options = new RequestOptions();
    options.setMethod(httpMethod);
    options.setServer(socketAddress);
    options.setPort(socketAddress.port());
    options.setHost(host);
    options.setURI(subPath + path);

    httpClient.request(options, ar -> {
      if (ar.failed()) {
        vcf.completeExceptionally(ar.cause());
        return;
      }

      final HttpClientRequest httpClientRequest = ar.result();
      httpClientRequest.response(response -> {
        if (response.failed()) {
          vcf.completeExceptionally(response.cause());
        }

        responseHandler.accept(response.result(), vcf);
      });
      httpClientRequest.exceptionHandler(vcf::completeExceptionally);

      if (mediaType.isPresent()) {
        httpClientRequest.putHeader("Accept", mediaType.get());
      } else {
        httpClientRequest.putHeader("Accept", "application/json");
      }
      authHeader.ifPresent(v -> httpClientRequest.putHeader("Authorization", v));
      additionalHeaders.forEach(httpClientRequest::putHeader);

      if (requestBody != null) {
        httpClientRequest.end(serialize(requestBody));
      } else {
        httpClientRequest.end();
      }
    });

    return vcf;
  }

  private static List<StreamedRow> toRows(final ResponseWithBody resp) {
    return KsqlTargetUtil.toRows(resp.getBody(), Functions.identity());
  }

  private static List<StreamedRow> toRowsFromProto(final ResponseWithBody resp) {
    return KsqlTargetUtil.toRows(resp.getBody(), Functions.identity());
  }
}
