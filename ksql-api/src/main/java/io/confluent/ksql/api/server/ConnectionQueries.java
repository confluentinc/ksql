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

import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Keeps track of which queries are owned by which connection so we can close them when the
 * connection is closed
 */
public class ConnectionQueries implements Handler<Void> {

  private final HttpConnection conn;
  private final Server server;
  private final Map<HttpConnection, ConnectionQueries> connectionsMap;

  private final Set<ApiQuery> queries = new HashSet<>();

  public ConnectionQueries(final HttpConnection conn, final Server server,
      final Map<HttpConnection, ConnectionQueries> connectionsMap) {
    this.conn = conn;
    this.server = server;
    this.connectionsMap = connectionsMap;
    conn.closeHandler(this);
    server.registerQueryConnection(conn);
  }

  public void addQuery(final ApiQuery query) {
    queries.add(query);
  }

  public void removeQuery(final ApiQuery query) {
    queries.remove(query);
  }

  @Override
  public void handle(final Void v) {
    for (ApiQuery query : queries) {
      query.close();
    }
    connectionsMap.remove(conn);
    server.removeQueryConnection(conn);
  }
}