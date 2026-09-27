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

package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.apache.hc.core5.http.HeaderElements.CHUNKED_ENCODING;
import static org.apache.hc.core5.http.HttpHeaders.TRANSFER_ENCODING;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.impl.BlockingPrintPublisher;
import io.confluent.ksql.api.server.JsonStreamedRowResponseWriter.RowFormat;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.properties.ConfigOverrideLogger;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to the query-stream endpoint
 */
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class QueryStreamHandler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamHandler.class);

  static final String DELIMITED_CONTENT_TYPE = "application/vnd.ksqlapi.delimited.v1";
  static final String JSON_CONTENT_TYPE = "application/json";
  static final String CONTENT_TYPE = "content-type";

  private final Endpoints endpoints;
  private final ConnectionQueryManager connectionQueryManager;
  private final Context context;
  private final Server server;
  private final boolean queryCompatibilityMode;
  private final boolean cancelOnDisconnect;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public QueryStreamHandler(final Endpoints endpoints,
      final ConnectionQueryManager connectionQueryManager,
      final Context context,
      final Server server,
      final boolean queryCompatibilityMode
  ) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.connectionQueryManager = Objects.requireNonNull(connectionQueryManager);
    this.context = Objects.requireNonNull(context);
    this.server = Objects.requireNonNull(server);
    this.queryCompatibilityMode = queryCompatibilityMode;
    // Read once: the value cannot change after startup, and this is on the per-query path.
    this.cancelOnDisconnect = server.getConfig()
        .getBoolean(KsqlRestConfig.KSQL_QUERY_PULL_CANCEL_ON_DISCONNECT_ENABLED_CONFIG);
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    // We must set it to allow chunked encoding if we're using http1.1
    if (routingContext.request().version() == HttpVersion.HTTP_1_1) {
      routingContext.response().putHeader(TRANSFER_ENCODING, CHUNKED_ENCODING);
    } else if (routingContext.request().version() == HttpVersion.HTTP_2) {
      // Nothing required
    } else {
      routingContext.fail(BAD_REQUEST.code(),
          new KsqlApiException("This endpoint is only available when using HTTP1.1 or HTTP2",
              ERROR_CODE_BAD_REQUEST));
    }

    final CommonRequest request = getRequest(routingContext);
    if (request == null) {
      return;
    }
    ConfigOverrideLogger.logOverrides(
        routingContext.request().path(), request.configOverrides);

    final Optional<Boolean> internalRequest = ServerVerticle.isInternalRequest(routingContext);
    final MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
    final long startTimeNanos = Time.SYSTEM.nanoseconds();

    endpoints.createQueryPublisher(
            request.sql, request.configOverrides, request.sessionProperties,
            request.requestProperties,
            context, server.getWorkerExecutor(),
            DefaultApiSecurityContext.create(routingContext, server), metricsCallbackHolder,
            internalRequest)
        .thenAccept(publisher -> {
          if (publisher instanceof BlockingPrintPublisher) {
            handlePrintPublisher(
                routingContext,
                (BlockingPrintPublisher) publisher);
          } else {
            handleQueryPublisher(
                routingContext,
                (QueryPublisher) publisher,
                metricsCallbackHolder,
                startTimeNanos);
          }
        })
        .exceptionally(t ->
            ServerUtils.handleEndpointException(t, routingContext, "Failed to execute query"));

  }

  private QueryStreamResponseWriter getQueryStreamResponseWriter(
      final RoutingContext routingContext,
      final QueryPublisher queryPublisher,
      final Optional<String> completionMessage,
      final Optional<String> limitMessage,
      final boolean bufferOutput
  ) {
    final String contentType = routingContext.getAcceptableContentType();
    if (DELIMITED_CONTENT_TYPE.equals(contentType)
        || (contentType == null && !queryCompatibilityMode)) {
      // Default
      routingContext.response().putHeader(CONTENT_TYPE, DELIMITED_CONTENT_TYPE);
      return new DelimitedQueryStreamResponseWriter(routingContext.response());
    } else if (KsqlMediaType.KSQL_V1_PROTOBUF.mediaType().equals(contentType)) {
      routingContext.response().putHeader(
          CONTENT_TYPE, KsqlMediaType.KSQL_V1_PROTOBUF.mediaType());
      return new JsonStreamedRowResponseWriter(
          routingContext.response(),
          queryPublisher,
          completionMessage,
          limitMessage,
          Clock.systemUTC(),
          bufferOutput,
          context,
          RowFormat.PROTOBUF
      );
    } else if (KsqlMediaType.KSQL_V1_JSON.mediaType().equals(contentType)
        || ((contentType == null || JSON_CONTENT_TYPE.equals(contentType)
        && queryCompatibilityMode))) {
      routingContext.response().putHeader(
          CONTENT_TYPE, KsqlMediaType.KSQL_V1_JSON.mediaType());
      return new JsonStreamedRowResponseWriter(
          routingContext.response(),
          queryPublisher,
          completionMessage,
          limitMessage,
          Clock.systemUTC(),
          bufferOutput,
          context,
          RowFormat.JSON);
    } else {
      routingContext.response().putHeader(CONTENT_TYPE, JSON_CONTENT_TYPE);
      return new JsonQueryStreamResponseWriter(routingContext.response());
    }
  }

  private CommonRequest getRequest(final RoutingContext routingContext) {
    final String sql;
    final Map<String, Object> configOverrides;
    final Map<String, Object> sessionProperties;
    final Map<String, Object> requestProperties;
    if (queryCompatibilityMode) {
      final Optional<KsqlRequest> ksqlRequest = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, KsqlRequest.class);
      if (!ksqlRequest.isPresent()) {
        return null;
      }
      // Set masked sql statement if request is not from OldApiUtils.handleOldApiRequest
      ApiServerUtils.setMaskedSqlIfNeeded(ksqlRequest.get());
      sql = ksqlRequest.get().getUnmaskedKsql();
      configOverrides = ksqlRequest.get().getConfigOverrides();
      sessionProperties = ksqlRequest.get().getSessionVariables();
      requestProperties = ksqlRequest.get().getRequestProperties();
    } else {
      final Optional<QueryStreamArgs> queryStreamArgs = ServerUtils
          .deserialiseObject(routingContext.getBody(), routingContext, QueryStreamArgs.class);
      if (!queryStreamArgs.isPresent()) {
        return null;
      }
      sql = queryStreamArgs.get().sql;
      configOverrides = queryStreamArgs.get().properties;
      sessionProperties = queryStreamArgs.get().sessionVariables;
      requestProperties = queryStreamArgs.get().requestProperties;
    }
    return new CommonRequest(sql, configOverrides, sessionProperties, requestProperties);
  }

  private void handleQueryPublisher(
      final RoutingContext routingContext,
      final QueryPublisher queryPublisher,
      final MetricsCallbackHolder metricsCallbackHolder,
      final long startTimeNanos
  ) {

    final QueryResponseMetadata metadata;
    Optional<String> completionMessage = Optional.empty();
    Optional<String> limitMessage = Optional.of("Limit Reached");
    boolean bufferOutput = false;
    // The end handler can be called twice if the connection is closed by the client.  The
    // call to response.end() resulting from queryPublisher.close() may result in a second
    // call to the end handler, which will mess up metrics, so we ensure that this called just
    // once by keeping track of the calls.
    final AtomicBoolean endedResponse = new AtomicBoolean(false);
    // Set by QuerySubscriber once the query has produced its last row. Neither endedResponse nor
    // response().ended() can stand in for this: both also become true when the client aborts, so
    // they cannot say whether the server got to finish.
    final AtomicBoolean serverCompleted = new AtomicBoolean(false);

    if (queryPublisher.isPullQuery()) {
      metadata = new QueryResponseMetadata(
          queryPublisher.queryId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          queryPublisher.geLogicalSchema());
      limitMessage = Optional.empty();
      bufferOutput = true;

      // When response is complete, publisher should be closed
      final Handler<Void> pullQueryCleanup = v -> {
        if (endedResponse.getAndSet(true)) {
          log.warn("Connection already closed so just returning");
          return;
        }
        queryPublisher.close();
        metricsCallbackHolder.reportMetrics(
            routingContext.response().getStatusCode(),
            routingContext.request().bytesRead(),
            routingContext.response().bytesWritten(),
            startTimeNanos);
      };
      routingContext.response().endHandler(pullQueryCleanup);

      // The end handler above only runs once the response has been ended by the server. If the
      // client goes away mid-response it never fires, so the publisher is never closed and the
      // query keeps executing with nothing waiting for its result. Closing the publisher
      // completes the query's cancellation future, which the scan operators poll between rows,
      // so the query stops promptly instead of running to completion.
      //
      // The /query endpoint has always done this (OldApiUtils installs a connection close
      // handler) and scalable push queries are covered by ConnectionQueryManager; pull queries on
      // this endpoint are the gap. Behind a flag because running to completion is the existing
      // behaviour and we want to be able to measure both.
      // response(), not request().connection(): the handler must be per-response, not
      // per-connection. HTTP/2 multiplexes many queries onto one connection, so abandoning a
      // single query resets its stream and leaves the connection open - a connection-level
      // handler would never fire. HttpConnection.closeHandler is also a setter, so one handler
      // per connection: registering per request would have each query silently overwrite the
      // previous one's. HttpServerResponse.closeHandler is documented as always called on
      // HTTP/2 stream close, and on HTTP/1.x when the connection closes before end().
      //
      // Registered unconditionally so disconnects are counted even when cancellation is off.
      routingContext.response().closeHandler(v -> {
        // On HTTP/2 this fires for every stream close, including normal ones, so it has to be
        // filtered. serverCompleted is the only signal that separates the two: the query
        // produced its last row before the stream closed.
        if (serverCompleted.get()) {
          return;
        }
        server.getPullQueryMetrics()
            .ifPresent(PullQueryExecutorMetrics::recordClientDisconnected);
        if (cancelOnDisconnect) {
          pullQueryCleanup.handle(null);
        }
      });
    } else if (queryPublisher.isScalablePushQuery()) {
      metadata = new QueryResponseMetadata(
          queryPublisher.queryId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          preparePushProjectionSchema(queryPublisher.geLogicalSchema()));

      routingContext.response().endHandler(v -> {
        if (endedResponse.getAndSet(true)) {
          log.warn("Connection already closed so just returning");
          return;
        }
        queryPublisher.close();
        metricsCallbackHolder.reportMetrics(
            routingContext.response().getStatusCode(),
            routingContext.request().bytesRead(),
            routingContext.response().bytesWritten(),
            startTimeNanos);
      });
    } else {
      final PushQueryHolder query = connectionQueryManager
          .createApiQuery(queryPublisher, routingContext.request());

      metadata = new QueryResponseMetadata(
          queryPublisher.queryId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          preparePushProjectionSchema(queryPublisher.geLogicalSchema()));
      completionMessage = Optional.of("Query Completed");

      // When response is complete, publisher should be closed and query unregistered
      routingContext.response().endHandler(v -> {
        if (endedResponse.getAndSet(true)) {
          log.warn("Connection already closed so just returning");
          return;
        }
        query.close();
        metricsCallbackHolder.reportMetrics(
            routingContext.response().getStatusCode(),
            routingContext.request().bytesRead(),
            routingContext.response().bytesWritten(),
            startTimeNanos);
      });
    }

    final QueryStreamResponseWriter queryStreamResponseWriter
        = getQueryStreamResponseWriter(routingContext, queryPublisher, completionMessage,
        limitMessage, bufferOutput);
    queryStreamResponseWriter.writeMetadata(metadata);

    final QuerySubscriber querySubscriber = new QuerySubscriber(context,
        routingContext.response(), queryStreamResponseWriter,
        queryPublisher::hitLimit, serverCompleted);

    queryPublisher.subscribe(querySubscriber);
  }

  private void handlePrintPublisher(
      final RoutingContext routingContext,
      final BlockingPrintPublisher printPublisher
  ) {
    final String contentType = routingContext.getAcceptableContentType();
    if (!(DELIMITED_CONTENT_TYPE.equals(contentType)
        || (contentType == null && !queryCompatibilityMode))) {
      // We currently only support delimited format for print topic
      // So we send 406 not acceptable back
      routingContext.response().setStatusCode(406).end();
      // Without this return, execution falls through to subscribe the
      // PrintSubscriber to the publisher — starting a KafkaConsumer poll loop
      // that writes into an already-ended response. Writes are silently
      // dropped but the consumer wastes broker connections until the end
      // handler chain eventually closes it.
      return;
    }

    // The end handler can be called twice if the connection is closed by the client.  The
    // call to response.end() resulting from queryPublisher.close() may result in a second
    // call to the end handler, which will mess up metrics, so we ensure that this called just
    // once by keeping track of the calls.
    final AtomicBoolean endedResponse = new AtomicBoolean(false);
    // When response is complete, publisher should be closed
    routingContext.response().endHandler(v ->
        endhandler(
            printPublisher,
            endedResponse
        ));

    final PrintSubscriber printSubscriber = new PrintSubscriber(
        context,
        routingContext.response()
    );

    printPublisher.subscribe(printSubscriber);
  }

  private void endhandler(
      final BlockingPrintPublisher printPublisher, final AtomicBoolean endedResponse) {
    if (endedResponse.getAndSet(true)) {
      log.warn("Connection already closed so just returning");
      return;
    }

    printPublisher.close();
  }

  private LogicalSchema preparePushProjectionSchema(final LogicalSchema schema) {
    final Builder projectionSchema = LogicalSchema.builder();
    schema.value().forEach(projectionSchema::valueColumn);
    return projectionSchema.build();
  }

  private static class CommonRequest {

    final String sql;
    final Map<String, Object> configOverrides;
    final Map<String, Object> sessionProperties;
    final Map<String, Object> requestProperties;

    CommonRequest(
        final String sql,
        final Map<String, Object> configOverrides,
        final Map<String, Object> sessionProperties,
        final Map<String, Object> requestProperties
    ) {
      this.sql = sql;
      this.configOverrides = configOverrides;
      this.sessionProperties = sessionProperties;
      this.requestProperties = requestProperties;
    }
  }
}
