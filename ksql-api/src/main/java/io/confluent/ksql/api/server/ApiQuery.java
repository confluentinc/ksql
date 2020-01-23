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

/**
 * Holder for a push query running on the server
 */
public class ApiQuery {

  private final Server server;
  private final ApiQueryID id;
  private final ConnectionQueries connectionQueries;
  private final QuerySubscriber querySubscriber;

  public ApiQuery(final Server server, final ConnectionQueries connectionQueries,
      final QuerySubscriber querySubscriber) {
    this.server = server;
    this.id = new ApiQueryID();
    this.connectionQueries = connectionQueries;
    this.querySubscriber = querySubscriber;
    connectionQueries.addQuery(this);
    server.registerQuery(this);
  }

  public void close() {
    connectionQueries.removeQuery(this);
    server.removeQuery(id);
    querySubscriber.close();
  }

  public ApiQueryID getId() {
    return id;
  }
}
