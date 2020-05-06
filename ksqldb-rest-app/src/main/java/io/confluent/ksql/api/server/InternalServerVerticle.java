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

import io.confluent.ksql.api.spi.InternalEndpoints;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import java.util.Objects;

public class InternalServerVerticle extends AbstractServerVerticle {
  private static final boolean INCLUDE_SHARED_ENDPOINTS = true;

  private final InternalEndpoints internalEndpoints;

  public InternalServerVerticle(
      final InternalEndpoints internalEndpoints,
      final HttpServerOptions httpServerOptions,
      final Server server) {
    super(httpServerOptions, server);
    this.internalEndpoints = Objects.requireNonNull(internalEndpoints);
  }

  protected Router setupRouter() {
    final Router router = Router.router(vertx);

    PortedEndpoints.setupFailureHandlerInternal(router, INCLUDE_SHARED_ENDPOINTS);

    router.route().failureHandler(RequestFailureHandler::handleFailure);

    RequestAuthenticationHandler.setupAuthHandlers(server, router);

    router.route().handler(new ServerStateHandler(server.getServerState()));

    PortedEndpoints.setupEndpointsInternal(internalEndpoints, server, router,
        INCLUDE_SHARED_ENDPOINTS);

    return router;
  }
}
