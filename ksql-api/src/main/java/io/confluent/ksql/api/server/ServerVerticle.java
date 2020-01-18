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
import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_UNKNOWN_QUERY_ID;
import static io.confluent.ksql.api.server.ServerUtils.handleError;

import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server deploys multiple server verticles. This is where the HTTP2 requests are handled. The
 * actual implementation of the endpoints is provided by an implementation of {@code Endpoints}.
 */
public class ServerVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(ServerVerticle.class);

  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Server server;
  private final Map<HttpConnection, ConnectionQueries> connectionsMap = new HashMap<>();
  private HttpServer httpServer;

  public ServerVerticle(final Endpoints endpoints, final HttpServerOptions httpServerOptions,
      final Server server) {
    this.endpoints = endpoints;
    this.httpServerOptions = httpServerOptions;
    this.server = server;
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(ServerUtils::unhandledExceptonHandler);
    final Future<HttpServer> listenFuture = Promise.<HttpServer>promise().future();
    Utils.connectPromise(listenFuture.map(s -> null), startPromise);
    httpServer.listen(listenFuture);
    vertx.getOrCreateContext().exceptionHandler(ServerUtils::unhandledExceptonHandler);
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    if (httpServer == null) {
      stopPromise.complete();
    } else {
      httpServer.close(stopPromise.future());
    }
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);
    router.route(HttpMethod.POST, "/query-stream").handler(BodyHandler.create())
        .handler(this::handleQueryStream);
    router.route(HttpMethod.POST, "/inserts-stream").handler(this::handleInsertsStream);
    router.route(HttpMethod.POST, "/close-query").handler(BodyHandler.create())
        .handler(this::handleCloseQuery);
    return router;
  }

  private void handleInsertsStream(final RoutingContext routingContext) {
    InsertsBodyHandler.connectBodyHandler(context, endpoints, routingContext);
  }

  private void handleQueryStream(final RoutingContext routingContext) {
    final HttpConnection conn = routingContext.request().connection();
    ConnectionQueries connectionQueries = connectionsMap.get(conn);
    if (connectionQueries == null) {
      connectionQueries = new ConnectionQueries(conn);
      connectionsMap.put(conn, connectionQueries);
      conn.closeHandler(connectionQueries);
      server.registerQueryConnection(conn);
    }
    final JsonObject requestBody = routingContext.getBodyAsJson();
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
    final QuerySubscriber querySubscriber = new QuerySubscriber(routingContext.response());

    final QueryID queryID = server.registerQuery(querySubscriber);
    connectionQueries.addQuery(queryID);
    final JsonObject metadata = new JsonObject();
    metadata.put("columnNames", queryPublisher.getColumnNames());
    metadata.put("columnTypes", queryPublisher.getColumnTypes());
    metadata.put("queryID", queryID.toString());
    if (!push) {
      metadata.put("rowCount", queryPublisher.getRowCount());
    }
    routingContext.response().write(metadata.toBuffer()).write("\n");
    queryPublisher.subscribe(querySubscriber);

    // When response is complete, publisher should be closed and query unregistered
    routingContext.response().endHandler(v -> closeQuery(queryID, routingContext));
  }

  private boolean closeQuery(final QueryID queryID, final RoutingContext routingContext) {
    final QuerySubscriber querySubscriber = server.removeQuery(queryID);
    if (querySubscriber == null) {
      return false;
    }
    final HttpConnection conn = routingContext.request().connection();
    final ConnectionQueries connectionQueries = connectionsMap.get(conn);
    connectionQueries.removeQuery(queryID);
    querySubscriber.close();
    return true;
  }

  private void handleCloseQuery(final RoutingContext routingContext) {
    final JsonObject requestBody = routingContext.getBodyAsJson();
    final String queryIDArg = requestBody.getString("queryID");
    if (queryIDArg == null) {
      handleError(routingContext.response(), 400, ERROR_CODE_MISSING_PARAM,
          "No queryID in arguments");
      return;
    }
    final QueryID queryID = new QueryID(queryIDArg);
    if (!closeQuery(queryID, routingContext)) {
      handleError(routingContext.response(), 400, ERROR_CODE_UNKNOWN_QUERY_ID,
          "No query with id " + queryID);
      return;
    }
    routingContext.response().end();
  }

  /*
  Keep track of which queries are owned by which connection so we can close them when the
  connection is closed
   */
  private class ConnectionQueries implements Handler<Void> {

    private final HttpConnection conn;
    private final Set<QueryID> queries = new HashSet<>();

    ConnectionQueries(final HttpConnection conn) {
      this.conn = conn;
    }

    public void addQuery(final QueryID queryID) {
      queries.add(queryID);
    }

    public void removeQuery(final QueryID queryID) {
      queries.remove(queryID);
    }

    @Override
    public void handle(final Void v) {
      for (QueryID queryID : queries) {
        final QuerySubscriber querySubscriber = server.removeQuery(queryID);
        querySubscriber.close();
      }
      connectionsMap.remove(conn);
      server.removeQueryConnection(conn);
    }
  }
}
