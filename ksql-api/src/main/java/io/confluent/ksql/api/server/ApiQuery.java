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

import java.util.UUID;

/**
 * Holder for a push query running on the server.
 *
 * <p>Uses UUID.randomUUID() for the id which internally uses SecureRandom - this makes the id
 * cryptographically secure. This is important as we don't want random users guessing query IDs and
 * closing other peoples queries.
 */
public class ApiQuery {

  private final Server server;
  private final String id;
  private final ConnectionQueries connectionQueries;
  private final QuerySubscriber querySubscriber;

  public ApiQuery(final Server server, final ConnectionQueries connectionQueries,
      final QuerySubscriber querySubscriber) {
    this.server = server;
    this.id = UUID.randomUUID().toString();
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

  public String getId() {
    return id;
  }
}
