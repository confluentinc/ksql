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
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.RoutingContext;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests to the query-stream endpoint
 */
public class QueryStreamHandler implements Handler<RoutingContext> {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamHandler.class);

  static final String DELIMITED_CONTENT_TYPE = "application/vnd.ksqlapi.delimited.v1";
  static final String JSON_CONTENT_TYPE = "application/json";

  private final Endpoints endpoints;
  private final ConnectionQueryManager connectionQueryManager;
  private final Context context;
  private final Server server;
  private final boolean queryCompatibilityMode;

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

    final QueryStreamResponseWriter queryStreamResponseWriter
        = getQueryStreamResponseWriter(routingContext);

    final CommonRequest request = getRequest(routingContext);
    if (request == null) {
      return;
    }

    final Optional<Boolean> internalRequest = ServerVerticle.isInternalRequest(routingContext);
    final MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
    final long startTimeNanos = Time.SYSTEM.nanoseconds();
    endpoints.createQueryPublisher(
        request.sql, request.configOverrides, request.sessionProperties,
        request.requestProperties,
        context, server.getWorkerExecutor(),
        DefaultApiSecurityContext.create(routingContext, server), metricsCallbackHolder,
        internalRequest)
        .thenAccept(queryPublisher -> {
          handleQueryPublisher(
              routingContext,
              queryPublisher,
              queryStreamResponseWriter,
              metricsCallbackHolder,
              startTimeNanos);
        })
        .exceptionally(t ->
            ServerUtils.handleEndpointException(t, routingContext, "Failed to execute query"));
  }

  private QueryStreamResponseWriter getQueryStreamResponseWriter(
      final RoutingContext routingContext
  ) {
    final String contentType = routingContext.getAcceptableContentType();
    if (DELIMITED_CONTENT_TYPE.equals(contentType)
        || (contentType == null && !queryCompatibilityMode)) {
      // Default
      return new DelimitedQueryStreamResponseWriter(routingContext.response());
    } else if (KsqlMediaType.KSQL_V1_JSON.mediaType().equals(contentType)
        || ((contentType == null || JSON_CONTENT_TYPE.equals(contentType)
        && queryCompatibilityMode))) {
      return new JsonStreamedRowResponseWriter(routingContext.response());
    } else {
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
      final QueryStreamResponseWriter queryStreamResponseWriter,
      final MetricsCallbackHolder metricsCallbackHolder,
      final long startTimeNanos
  ) {

    final QueryResponseMetadata metadata;
    final Optional<String> completionMessage;

    if (queryPublisher.isPullQuery()) {
      metadata = new QueryResponseMetadata(
          queryPublisher.queryId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          queryPublisher.geLogicalSchema());
      completionMessage = Optional.of("Pull query complete");

      // When response is complete, publisher should be closed
      routingContext.response().endHandler(v -> {
        queryPublisher.close();
        metricsCallbackHolder.reportMetrics(
            routingContext.response().getStatusCode(),
            routingContext.request().bytesRead(),
            routingContext.response().bytesWritten(),
            startTimeNanos);
      });
    }  else if (queryPublisher.isScalablePushQuery()) {
      metadata = new QueryResponseMetadata(
          queryPublisher.queryId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          preparePushProjectionSchema(queryPublisher.geLogicalSchema()));
      completionMessage = Optional.empty();
      routingContext.response().endHandler(v -> {
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
          query.getId().toString(),
          queryPublisher.getColumnNames(),
          queryPublisher.getColumnTypes(),
          preparePushProjectionSchema(queryPublisher.geLogicalSchema()));
      completionMessage = Optional.empty();

      // When response is complete, publisher should be closed and query unregistered
      routingContext.response().endHandler(v -> {
        query.close();
        metricsCallbackHolder.reportMetrics(
            routingContext.response().getStatusCode(),
            routingContext.request().bytesRead(),
            routingContext.response().bytesWritten(),
            startTimeNanos);
      });
    }

    queryStreamResponseWriter.writeMetadata(metadata);

    final QuerySubscriber querySubscriber = new QuerySubscriber(context,
        routingContext.response(), queryStreamResponseWriter, completionMessage,
        queryPublisher::hitLimit);

    queryPublisher.subscribe(querySubscriber);
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
