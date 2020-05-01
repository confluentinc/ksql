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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractServerVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(AbstractServerVerticle.class);

  protected final HttpServerOptions httpServerOptions;
  protected final Server server;
  protected ConnectionQueryManager connectionQueryManager;
  protected HttpServer httpServer;

  public AbstractServerVerticle(final HttpServerOptions httpServerOptions, final Server server) {
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
    this.server = Objects.requireNonNull(server);
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    this.connectionQueryManager = new ConnectionQueryManager(context, server);
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(AbstractServerVerticle::unhandledExceptionHandler);
    httpServer.listen(ar -> {
      if (ar.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(ar.cause());
      }
    });
  }

  @Override
  public void stop(final Promise<Void> stopPromise) {
    if (httpServer == null) {
      stopPromise.complete();
    } else {
      httpServer.close(stopPromise.future());
    }
  }

  int actualPort() {
    return httpServer.actualPort();
  }

  abstract Router setupRouter();

  private static void unhandledExceptionHandler(final Throwable t) {
    log.error("Unhandled exception", t);
  }
}
