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

import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the API server. On start-up it deploys multiple server verticles to spread
 * the load across available cores.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class Server {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(Server.class);

  private final Vertx vertx;
  private final ApiServerConfig config;
  private final Endpoints endpoints;
  private final Map<PushQueryId, PushQueryHolder> queries = new ConcurrentHashMap<>();
  private final Set<HttpConnection> connections = new ConcurrentHashSet<>();
  private final int maxPushQueryCount;
  private final SocketAddress proxyTarget;
  private String deploymentID;
  private WorkerExecutor workerExecutor;
  private final AtomicInteger actualPort = new AtomicInteger(-1);

  public Server(final Vertx vertx, final ApiServerConfig config, final Endpoints endpoints) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.maxPushQueryCount = config.getInt(ApiServerConfig.MAX_PUSH_QUERIES);
    final int proxyPort = config.getInt(ApiServerConfig.PROXY_PORT);
    this.proxyTarget = SocketAddress.inetSocketAddress(proxyPort, "127.0.0.1");
  }

  public synchronized void start() {
    if (deploymentID != null) {
      throw new IllegalStateException("Already started");
    }
    final DeploymentOptions options = new DeploymentOptions()
        .setInstances(config.getInt(ApiServerConfig.VERTICLE_INSTANCES));
    this.workerExecutor = vertx.createSharedWorkerExecutor("ksql-workers",
        config.getInt(ApiServerConfig.WORKER_POOL_SIZE));
    log.debug("Deploying " + options.getInstances() + " instances of server verticle");
    final VertxCompletableFuture<String> future = new VertxCompletableFuture<>();
    vertx.deployVerticle(() ->
            new ServerVerticle(endpoints, createHttpServerOptions(config), this, proxyTarget),
        options,
        future);
    try {
      deploymentID = future.get();
    } catch (Exception e) {
      throw new KsqlException("Failed to start API server", e);
    }
    log.info("API server started");
  }

  public synchronized void stop() {
    if (deploymentID == null) {
      throw new IllegalStateException("Not started");
    }
    if (workerExecutor != null) {
      workerExecutor.close();
    }
    final VertxCompletableFuture<Void> future = new VertxCompletableFuture<>();
    vertx.undeploy(deploymentID, future);
    try {
      future.get();
    } catch (Exception e) {
      throw new KsqlException("Failure in stopping API server", e);
    }
    log.info("API server stopped");
  }

  public WorkerExecutor getWorkerExecutor() {
    return workerExecutor;
  }

  synchronized void registerQuery(final PushQueryHolder query) {
    Objects.requireNonNull(query);
    if (queries.size() == maxPushQueryCount) {
      throw new KsqlApiException("Maximum number of push queries exceeded",
          ErrorCodes.ERROR_MAX_PUSH_QUERIES_EXCEEDED);
    }
    if (queries.putIfAbsent(query.getId(), query) != null) {
      // It should never happen
      // https://stackoverflow.com/questions/2513573/how-good-is-javas-uuid-randomuuid
      throw new IllegalStateException("Glitch in the matrix");
    }
  }

  Optional<PushQueryHolder> removeQuery(final PushQueryId queryId) {
    return Optional.ofNullable(queries.remove(queryId));
  }

  public Set<PushQueryId> getQueryIDs() {
    return new HashSet<>(queries.keySet());
  }

  void registerQueryConnection(final HttpConnection connection) {
    this.connections.add(Objects.requireNonNull(connection));
  }

  void removeQueryConnection(final HttpConnection connection) {
    connections.remove(Objects.requireNonNull(connection));
  }

  public int queryConnectionCount() {
    return connections.size();
  }

  void setActualPort(final int port) {
    actualPort.set(port);
  }

  public int getActualPort() {
    return actualPort.get();
  }

  private static HttpServerOptions createHttpServerOptions(final ApiServerConfig apiServerConfig) {

    final HttpServerOptions options = new HttpServerOptions()
        .setHost(apiServerConfig.getString(ApiServerConfig.LISTEN_HOST))
        .setPort(apiServerConfig.getInt(ApiServerConfig.LISTEN_PORT))
        .setReuseAddress(true)
        .setReusePort(true);

    if (apiServerConfig.getBoolean(ApiServerConfig.TLS_ENABLED)) {
      options.setUseAlpn(true)
          .setSsl(true)
          .setKeyStoreOptions(
              new JksOptions()
                  .setPath(apiServerConfig.getString(ApiServerConfig.TLS_KEY_STORE_PATH))
                  .setPassword(apiServerConfig.getString(ApiServerConfig.TLS_KEY_STORE_PASSWORD)))
          .setTrustStoreOptions(
              new JksOptions()
                  .setPath(apiServerConfig.getString(ApiServerConfig.TLS_TRUST_STORE_PATH))
                  .setPassword(
                      apiServerConfig.getString(ApiServerConfig.TLS_TRUST_STORE_PASSWORD)))
          .setClientAuth(apiServerConfig.getClientAuth());
    }

    return options;
  }
}
