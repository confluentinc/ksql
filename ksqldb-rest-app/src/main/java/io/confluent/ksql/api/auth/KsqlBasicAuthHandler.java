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

package io.confluent.ksql.api.auth;

import io.confluent.ksql.api.server.ErrorCodes;
import io.confluent.ksql.api.server.ServerUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.BasicAuthHandlerImpl;
import io.vertx.ext.web.impl.RoutingContextDecorator;

/**
 * Wraps the Vert.x BasicAuthHandlerImpl in order to more gracefully handle errors encoutnered when
 * parsing credentials.
 */
public class KsqlBasicAuthHandler extends BasicAuthHandlerImpl {

  public KsqlBasicAuthHandler(final AuthProvider authProvider, final String realm) {
    super(authProvider, realm);
  }

  @Override
  public void parseCredentials(
      final RoutingContext routingContext,
      final Handler<AsyncResult<JsonObject>> handler
  ) {
    // Intercept the context.fail() call of the BasicAuthHandlerImpl in order to populate
    // the response buffer with a meaningful message to be displayed to the user
    final RoutingContextDecorator routingContextDecorator =
        new RoutingContextDecorator(routingContext.currentRoute(), routingContext) {

      public void fail(final Throwable throwable) {
        ServerUtils.handleError(routingContext.response(), 400,
            ErrorCodes.ERROR_CODE_MALFORMED_REQUEST,
            "Malformed authorization header");
      }
    };
    super.parseCredentials(routingContextDecorator, ar -> {
      if (ar.succeeded()) {
        handler.handle(ar);
      }
    });
  }
}
