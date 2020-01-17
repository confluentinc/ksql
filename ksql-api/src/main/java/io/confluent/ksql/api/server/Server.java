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

import io.confluent.ksql.api.impl.VertxCompletableFuture;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.util.KsqlException;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the API server. On start-up it deploys multiple server verticles to spread
 * the load across available cores.
 */
public class Server {

  private static final Logger log = LoggerFactory.getLogger(Server.class);

  private final Vertx vertx;
  private final JsonObject config;
  private final Endpoints endpoints;
  private final HttpServerOptions httpServerOptions;
  private final Map<QueryID, QuerySubscriber> queries = new ConcurrentHashMap<>();
  private final Set<HttpConnection> connections = new ConcurrentHashSet<>();
  private String deploymentID;

  public Server(final Vertx vertx, final JsonObject config, final Endpoints endpoints,
      final HttpServerOptions httpServerOptions) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.httpServerOptions = Objects.requireNonNull(httpServerOptions);
  }

  public synchronized void start() {
    if (deploymentID != null) {
      throw new IllegalStateException("Already started");
    }
    final DeploymentOptions options = new DeploymentOptions();
    final Integer verticleInstances = config.getInteger("verticle-instances");
    if (verticleInstances == null) {
      options.setInstances(Runtime.getRuntime().availableProcessors() * 2);
    } else {
      options.setInstances(verticleInstances);
    }
    log.info("Deploying " + options.getInstances() + " instances of server verticle");
    options.setConfig(config);
    final VertxCompletableFuture<String> future = new VertxCompletableFuture<>();
    vertx.deployVerticle(
        () -> new ServerVerticle(endpoints, httpServerOptions, this), options, future);
    try {
      deploymentID = future.get();
    } catch (Exception e) {
      throw new KsqlException("Failed to start API server", e);
    }
    log.info("API server started: " + deploymentID);
  }

  public synchronized void stop() {
    if (deploymentID == null) {
      throw new IllegalStateException("Not started");
    }
    final VertxCompletableFuture<Void> future = new VertxCompletableFuture<>();
    vertx.undeploy(deploymentID, future);
    try {
      future.get();
    } catch (Exception e) {
      throw new KsqlException("Failure in stopping API server", e);
    }
  }

  QueryID registerQuery(final QuerySubscriber querySubscriber) {
    Objects.requireNonNull(querySubscriber);
    final QueryID queryID = new QueryID();
    queries.put(queryID, querySubscriber);
    return queryID;
  }

  QuerySubscriber removeQuery(final QueryID queryID) {
    return queries.remove(queryID);
  }

  public Set<QueryID> getQueryIDs() {
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

}
