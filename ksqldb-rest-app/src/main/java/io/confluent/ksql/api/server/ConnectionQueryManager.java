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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ConnectionQueryManager {

  private final Context context;
  private final Server server;
  private final Map<HttpConnection, ConnectionQueries> connectionsMap = new HashMap<>();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ConnectionQueryManager(final Context context, final Server server) {
    this.context = Objects.requireNonNull(context);
    this.server = Objects.requireNonNull(server);
  }

  public PushQueryHolder createApiQuery(
      final QueryPublisher queryPublisher,
      final HttpServerRequest request) {
    checkContext();
    final ConnectionQueries connectionQueries = getConnectionQueries(request);
    final PushQueryHolder query = new PushQueryHolder(server,
        queryPublisher, connectionQueries::removeQuery);
    server.registerQuery(query);
    connectionQueries.addQuery(query);
    return query;
  }

  private ConnectionQueries getConnectionQueries(final HttpServerRequest request) {
    final HttpConnection conn = request.connection();
    return connectionsMap.computeIfAbsent(conn, ConnectionQueries::new);
  }

  private void checkContext() {
    VertxUtils.checkContext(context);
  }

  private class ConnectionQueries implements Handler<Void> {

    private final HttpConnection conn;
    private final Set<PushQueryHolder> queries = new HashSet<>();

    ConnectionQueries(final HttpConnection conn) {
      this.conn = Objects.requireNonNull(conn);
      conn.closeHandler(this);
      server.registerQueryConnection(conn);
    }

    void addQuery(final PushQueryHolder query) {
      checkContext();
      queries.add(Objects.requireNonNull(query));
    }

    void removeQuery(final PushQueryHolder query) {
      checkContext();
      queries.remove(Objects.requireNonNull(query));
    }

    @Override
    public void handle(final Void v) {
      checkContext();
      for (PushQueryHolder query : queries) {
        query.close();
      }
      connectionsMap.remove(conn);
      server.removeQueryConnection(conn);
    }
  }

}
