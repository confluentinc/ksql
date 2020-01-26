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

import io.confluent.ksql.api.server.protocol.QueryResponseMetadata;
import io.confluent.ksql.api.server.protocol.QueryStreamArgs;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;

/**
 * Handles requests to the query-stream endpoint
 */
public class QueryStreamHandler implements Handler<RoutingContext> {

  static final String DELIMITED_CONTENT_TYPE = "application/vnd.ksqlapi.delimited.v1";

  private final Endpoints endpoints;
  private final ConnectionQueryManager connectionQueryManager;

  public QueryStreamHandler(final Endpoints endpoints,
      final ConnectionQueryManager connectionQueryManager) {
    this.endpoints = Objects.requireNonNull(endpoints);
    this.connectionQueryManager = Objects.requireNonNull(connectionQueryManager);
  }

  @Override
  public void handle(final RoutingContext routingContext) {

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
        .deserialiseObject(routingContext.getBody(), routingContext.response(),
            QueryStreamArgs.class);
    if (!queryStreamArgs.isPresent()) {
      return;
    }

    final QueryPublisher queryPublisher = endpoints
        .createQueryPublisher(queryStreamArgs.get().sql, queryStreamArgs.get().push,
            queryStreamArgs.get().properties);

    final QuerySubscriber querySubscriber = new QuerySubscriber(routingContext.response(),
        queryStreamResponseWriter);

    final PushQueryHolder query = connectionQueryManager
        .createApiQuery(querySubscriber, routingContext.request());

    final QueryResponseMetadata metadata = new QueryResponseMetadata(query.getId().toString(),
        queryPublisher.getColumnNames(),
        queryPublisher.getColumnTypes(),
        queryStreamArgs.get().push ? null : queryPublisher.getRowCount());

    queryStreamResponseWriter.writeMetadata(metadata);
    queryPublisher.subscribe(querySubscriber);

    // When response is complete, publisher should be closed and query unregistered
    routingContext.response().endHandler(v -> query.close());
  }
}
