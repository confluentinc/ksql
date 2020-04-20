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

import static io.confluent.ksql.api.server.ErrorCodes.ERROR_CODE_UNKNOWN_QUERY_ID;

import io.confluent.ksql.api.server.protocol.CloseQueryArgs;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;
import org.apache.http.HttpStatus;

/**
 * Handles requests to the close-query endpoint
 */
public class CloseQueryHandler implements Handler<RoutingContext> {

  private final Server server;

  public CloseQueryHandler(final Server server) {
    this.server = Objects.requireNonNull(server);
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final Optional<CloseQueryArgs> closeQueryArgs = ServerUtils
        .deserialiseObject(routingContext.getBody(), routingContext, CloseQueryArgs.class);
    if (!closeQueryArgs.isPresent()) {
      return;
    }

    final Optional<PushQueryHolder> query = server
        .removeQuery(closeQueryArgs.get().queryId);
    if (!query.isPresent()) {
      routingContext
          .fail(HttpStatus.SC_BAD_REQUEST,
              new KsqlApiException("No query with id " + closeQueryArgs.get().queryId,
                  ERROR_CODE_UNKNOWN_QUERY_ID));
      return;
    }
    query.get().close();
    routingContext.response().end();
  }
}
