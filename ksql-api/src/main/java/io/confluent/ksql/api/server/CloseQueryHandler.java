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

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

/**
 * Handles requests to the close-query endpoint
 */
public class CloseQueryHandler implements Handler<RoutingContext> {

  private final Server server;

  public CloseQueryHandler(final Server server) {
    this.server = server;
  }

  @Override
  public void handle(final RoutingContext routingContext) {
    final JsonObject requestBody = routingContext.getBodyAsJson();
    final String queryID = requestBody.getString("queryID");
    if (queryID == null) {
      handleError(routingContext.response(), 400, ERROR_CODE_MISSING_PARAM,
          "No queryID in arguments");
      return;
    }
    final ApiQuery query = server.removeQuery(queryID);
    if (query == null) {
      handleError(routingContext.response(), 400, ERROR_CODE_UNKNOWN_QUERY_ID,
          "No query with id " + queryID);
      return;
    }
    query.close();
    routingContext.response().end();
  }
}
