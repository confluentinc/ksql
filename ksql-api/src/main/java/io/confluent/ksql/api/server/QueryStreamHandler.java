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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_MISSING_PARAM;
import static io.confluent.ksql.api.server.ServerUtils.decodeJsonObject;
import static io.confluent.ksql.api.server.ServerUtils.handleError;

import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.Map;

/**
 * Handles requests to the query-stream endpoint
 */
public class QueryStreamHandler implements Handler<RoutingContext> {

  static final String JSON_CONTENT_TYPE = "application/json";
  static final String DELIMITED_CONTENT_TYPE = "application/vnd.ksqlapi.delimited.v1";

  private final Endpoints endpoints;
  private final Server server;
  private final Map<HttpConnection, ConnectionQueries> connectionsMap;

  public QueryStreamHandler(final Endpoints endpoints, final Server server,
      final Map<HttpConnection, ConnectionQueries> connectionsMap) {
    this.endpoints = endpoints;
    this.server = server;
    this.connectionsMap = connectionsMap;
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

    final JsonObject requestBody = decodeJsonObject(routingContext.getBody(), routingContext);
    if (requestBody == null) {
      return;
    }
    final String sql = requestBody.getString("sql");
    if (sql == null) {
      handleError(routingContext.response(), 400, ERROR_CODE_MISSING_PARAM, "No sql in arguments");
      return;
    }
    final Boolean push = requestBody.getBoolean("push");
    if (push == null) {
      handleError(routingContext.response(), 400, ERROR_CODE_MISSING_PARAM, "No push in arguments");
      return;
    }
    final JsonObject properties = requestBody.getJsonObject("properties");
    final QueryPublisher queryPublisher = endpoints.createQueryPublisher(sql, push, properties);

    final QuerySubscriber querySubscriber = new QuerySubscriber(routingContext.response(),
        queryStreamResponseWriter);

    final HttpConnection conn = routingContext.request().connection();
    ConnectionQueries connectionQueries = connectionsMap.get(conn);
    if (connectionQueries == null) {
      connectionQueries = new ConnectionQueries(conn, server, connectionsMap);
      connectionsMap.put(conn, connectionQueries);
    }
    final ApiQuery query = new ApiQuery(server, connectionQueries, querySubscriber);
    final JsonObject metadata = new JsonObject();
    metadata.put("columnNames", queryPublisher.getColumnNames());
    metadata.put("columnTypes", queryPublisher.getColumnTypes());
    metadata.put("queryID", query.getId().toString());
    if (!push) {
      metadata.put("rowCount", queryPublisher.getRowCount());
    }
    queryStreamResponseWriter.writeMetadata(metadata);
    queryPublisher.subscribe(querySubscriber);

    // When response is complete, publisher should be closed and query unregistered
    routingContext.response().endHandler(v -> query.close());
  }
}
