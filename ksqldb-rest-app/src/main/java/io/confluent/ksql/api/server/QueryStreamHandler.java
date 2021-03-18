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

import static io.confluent.ksql.api.server.ServerUtils.checkHttp2;

import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
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

  private final Endpoints endpoints;
  private final ConnectionQueryManager connectionQueryManager;
  private final Context context;
  private final Server server;

  public QueryStreamHandler(final Endpoints endpoints,
      final ConnectionQueryManager connectionQueryManager,
      final Context context,
      final Server server) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.connectionQueryManager = Objects.requireNonNull(connectionQueryManager);
    this.context = Objects.requireNonNull(context);
    this.server = Objects.requireNonNull(server);
  }

  @Override
  public void handle(final RoutingContext routingContext) {

    if (!checkHttp2(routingContext)) {
      return;
    }

    final String contentType = routingContext.getAcceptableContentType();
    final QueryStreamResponseWriter queryStreamResponseWriter;
    if (DELIMITED_CONTENT_TYPE.equals(contentType) || contentType == null) {
      // Default
      queryStreamResponseWriter =
          new DelimitedQueryStreamResponseWriter(routingContext.response());
    } else {
      queryStreamResponseWriter = new JsonQueryStreamResponseWriter(routingContext.response());
    }

    final Optional<QueryStreamArgs> queryStreamArgs = ServerUtils
        .deserialiseObject(routingContext.getBody(), routingContext, QueryStreamArgs.class);
    if (!queryStreamArgs.isPresent()) {
      return;
    }

    final MetricsCallbackHolder metricsCallbackHolder = new MetricsCallbackHolder();
    final long startTimeNanos = Time.SYSTEM.nanoseconds();
    endpoints.createQueryPublisher(queryStreamArgs.get().sql, queryStreamArgs.get().properties, queryStreamArgs.get().sessionVariables,
        context, server.getWorkerExecutor(), DefaultApiSecurityContext.create(routingContext),
        metricsCallbackHolder)
        .thenAccept(queryPublisher -> {

          final QueryResponseMetadata metadata;

          if (queryPublisher.isPullQuery()) {
            metadata = new QueryResponseMetadata(
                queryPublisher.getColumnNames(),
                queryPublisher.getColumnTypes());

            // When response is complete, publisher should be closed
            routingContext.response().endHandler(v -> {
              queryPublisher.close();
              metricsCallbackHolder.reportMetrics(
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
                queryPublisher.getColumnTypes());

            // When response is complete, publisher should be closed and query unregistered
            routingContext.response().endHandler(v -> query.close());
          }

          queryStreamResponseWriter.writeMetadata(metadata);

          final QuerySubscriber querySubscriber = new QuerySubscriber(context,
              routingContext.response(),
              queryStreamResponseWriter);

          queryPublisher.subscribe(querySubscriber);

        })
        .exceptionally(t ->
            ServerUtils.handleEndpointException(t, routingContext, "Failed to execute query"));
  }
}
