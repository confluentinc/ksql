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

package io.confluent.ksql.rest.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FakeApiServer extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(FakeApiServer.class);

  private final HttpServerOptions httpServerOptions;

  private HttpServer httpServer;
  private volatile int port;

  private HttpMethod httpMethod;
  private String path;
  private Buffer body;
  private MultiMap headers;

  private Object responseObject;
  private Buffer responseBuffer;
  private volatile CompletableFuture<Buffer> bodyFuture;
  private boolean connectionClosed;
  private int errorCode = -1;

  public FakeApiServer(final HttpServerOptions httpServerOptions) {
    this.httpServerOptions = new HttpServerOptions(httpServerOptions);
  }

  @Override
  public void start(final Promise<Void> startPromise) {
    httpServer = vertx.createHttpServer(httpServerOptions).requestHandler(setupRouter())
        .exceptionHandler(FakeApiServer::unhandledExceptionHandler);
    httpServer.listen(ar -> {
      if (ar.succeeded()) {
        port = ar.result().actualPort();
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
      httpServer.close(x -> stopPromise.complete());
    }
  }

  private Router setupRouter() {
    final Router router = Router.router(vertx);
    router.route()
        .handler(BodyHandler.create())
        .handler(this::handleRequest);
    return router;
  }

  private static void unhandledExceptionHandler(Throwable t) {
    t.printStackTrace();
  }

  private synchronized void handleRequest(final RoutingContext routingContext) {
    HttpServerRequest request = routingContext.request();
    request.connection().closeHandler(v -> connectionClosed());

    httpMethod = request.method();
    path = request.path();
    headers = request.headers();
    body = routingContext.getBody();
    if (bodyFuture != null) {
      bodyFuture.complete(body);
    }
    if (errorCode != -1) {
      request.response().setStatusCode(errorCode);
    }
    if (responseBuffer != null) {
      request.response().end(responseBuffer);
    } else if (responseObject != null) {
      request.response().end(KsqlClientUtil.serialize(responseObject));
    } else {
      request.response().end();
    }
  }

  public synchronized HttpMethod getHttpMethod() {
    return httpMethod;
  }

  public synchronized String getPath() {
    return path;
  }

  public Buffer waitForRequestBody() throws Exception {
    this.bodyFuture = new CompletableFuture<>();
    return bodyFuture.get();
  }

  public synchronized Buffer getBody() {
    return body;
  }

  public synchronized MultiMap getHeaders() {
    return headers;
  }

  public synchronized void setResponseObject(final Object responseBody) {
    this.responseObject = responseBody;
  }

  public synchronized void setResponseBuffer(final Buffer responseBuffer) {
    this.responseBuffer = responseBuffer;
  }

  public int getPort() {
    return port;
  }

  private synchronized void connectionClosed() {
    connectionClosed = true;
  }

  public synchronized boolean isConnectionClosed() {
    return connectionClosed;
  }

  public synchronized void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  synchronized int getErrorCode() {
    return errorCode;
  }

}

