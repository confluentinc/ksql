/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.server.PreconditionVerticle;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.util.FileWatcher;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServerOptions;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreconditionServer {
  private static final Logger log
      = LoggerFactory.getLogger(io.confluent.ksql.api.server.Server.class);

  private final Vertx vertx;
  private final KsqlRestConfig config;
  private final Set<String> deploymentIds = new HashSet<>();
  private final ServerState serverState;
  private final List<URI> listeners;
  private final Set<URI> proxyProtocolListeners;
  private FileWatcher fileWatcher;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PreconditionServer(
      final Vertx vertx,
      final KsqlRestConfig config,
      final ServerState serverState
  ) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
    this.serverState = Objects.requireNonNull(serverState);
    this.listeners = ImmutableList.copyOf(ApiServerUtils.parseListeners(config));
    this.proxyProtocolListeners = new HashSet<>(
        ApiServerUtils.parseProxyProtocolListeners(config));
  }

  public synchronized void start() {
    if (!deploymentIds.isEmpty()) {
      throw new IllegalStateException("Already started");
    }
    final int idleConnectionTimeoutSeconds =
        config.getInt(KsqlRestConfig.IDLE_CONNECTION_TIMEOUT_SECONDS);

    fileWatcher = ApiServerUtils.configureTlsCertReload(config, this::restart);


    final int instances = config.getInt(KsqlRestConfig.VERTICLE_INSTANCES);
    log.debug("Deploying " + instances + " instances of server verticle");

    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();
    for (URI listener : listeners) {
      final boolean useProxyProtocol = proxyProtocolListeners.contains(listener);

      for (int i = 0; i < instances; i++) {
        final VertxCompletableFuture<String> vcf = new VertxCompletableFuture<>();
        final Verticle serverVerticle = new PreconditionVerticle(
            createHttpServerOptions(config, listener.getHost(), listener.getPort(),
                listener.getScheme().equalsIgnoreCase("https"),
                idleConnectionTimeoutSeconds,
                useProxyProtocol
            ),
            serverState,
            config
        );
        vertx.deployVerticle(serverVerticle, vcf);
        deployFutures.add(vcf);
      }
    }

    final CompletableFuture<Void> allDeployFuture = CompletableFuture.allOf(deployFutures
        .toArray(new CompletableFuture<?>[0]));

    try {
      allDeployFuture.get();
      for (CompletableFuture<String> deployFuture : deployFutures) {
        deploymentIds.add(deployFuture.get());
      }
    } catch (Exception e) {
      throw new KsqlException("Failed to start API server", e);
    }
    log.info("API server started");
    listeners.forEach(
        l -> log.info("Listening on: " + l.toString())
    );
  }

  public synchronized boolean started() {
    return !deploymentIds.isEmpty();
  }

  public synchronized void stop() {
    if (deploymentIds.isEmpty()) {
      throw new IllegalStateException("Not started");
    }
    if (fileWatcher != null) {
      fileWatcher.shutdown();
    }
    final List<CompletableFuture<Void>> undeployFutures = new ArrayList<>();
    for (String deploymentID : deploymentIds) {
      final VertxCompletableFuture<Void> future = new VertxCompletableFuture<>();
      vertx.undeploy(deploymentID, future);
      undeployFutures.add(future);
    }
    try {
      CompletableFuture.allOf(undeployFutures.toArray(new CompletableFuture<?>[0])).get();
    } catch (Exception e) {
      throw new KsqlException("Failure in stopping API server", e);
    }
    deploymentIds.clear();
    log.info("API server stopped");
  }

  public List<URI> getListeners() {
    return ImmutableList.copyOf(listeners);
  }

  private void restart() {
    log.info("Restarting precondition server");
    stop();
    start();
  }

  private static HttpServerOptions createHttpServerOptions(
      final KsqlRestConfig ksqlRestConfig,
      final String host,
      final int port,
      final boolean tls,
      final int idleTimeoutSeconds,
      final boolean useProxyProtocol
  ) {

    final HttpServerOptions options = new HttpServerOptions()
        .setHost(host)
        .setPort(port)
        .setReuseAddress(true)
        .setReusePort(true)
        .setIdleTimeout(idleTimeoutSeconds).setIdleTimeoutUnit(TimeUnit.SECONDS)
        .setPerMessageWebSocketCompressionSupported(true)
        .setPerFrameWebSocketCompressionSupported(true)
        .setUseProxyProtocol(useProxyProtocol);

    if (tls) {
      final String ksConfigName = KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG;
      final ClientAuth clientAuth = ksqlRestConfig.getClientAuth();

      final String alias = ksqlRestConfig.getString(ksConfigName);
      ApiServerUtils.setTlsOptions(ksqlRestConfig, options, alias, clientAuth);
    }
    return options;
  }
}


