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
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import io.confluent.ksql.api.auth.DefaultApiSecurityContext;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.rest.entity.QueryResponseMetadata;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.util.KsqlStatementException;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
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

    endpoints.createQueryPublisher(queryStreamArgs.get().sql, queryStreamArgs.get().properties,
        context, server.getWorkerExecutor(), DefaultApiSecurityContext.create(routingContext))
        .thenAccept(queryPublisher -> {

          final QueryResponseMetadata metadata;

          if (queryPublisher.isPullQuery()) {
            metadata = new QueryResponseMetadata(
                queryPublisher.getColumnNames(),
                queryPublisher.getColumnTypes());
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
        .exceptionally(t -> handleQueryPublisherException(t, routingContext));
  }

  private Void handleQueryPublisherException(final Throwable t,
      final RoutingContext routingContext) {
    if (t instanceof CompletionException) {
      final Throwable actual = t.getCause();
      log.error("Failed to execute query", actual);
      if (actual instanceof KsqlStatementException) {
        routingContext.fail(BAD_REQUEST.code(),
            new KsqlApiException(actual.getMessage(), ERROR_CODE_BAD_STATEMENT));
        return null;
      } else if (actual instanceof KsqlApiException) {
        routingContext.fail(BAD_REQUEST.code(), actual);
        return null;
      }
    } else {
      log.error("Failed to execute query", t);
    }
    // We don't expose internal error message via public API
    routingContext.fail(INTERNAL_SERVER_ERROR.code(), new KsqlApiException(
        "The server encountered an internal error when processing the query."
            + " Please consult the server logs for more information.",
        ERROR_CODE_SERVER_ERROR));
    return null;
  }

}
