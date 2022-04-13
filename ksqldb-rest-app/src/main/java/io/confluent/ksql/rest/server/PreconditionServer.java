package io.confluent.ksql.rest.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.server.PreconditionVerticle;
import io.confluent.ksql.api.util.ApiServerUtils;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.util.FileWatcher;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.netty.handler.ssl.OpenSsl;
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
  private static final Logger log = LoggerFactory.getLogger(io.confluent.ksql.api.server.Server.class);

  private final Vertx vertx;
  private final KsqlRestConfig config;
  private final Set<String> deploymentIds = new HashSet<>();
  private final ServerState serverState;
  private final List<URI> listeners = new ArrayList<>();
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
    if (!OpenSsl.isAvailable()) {
      log.warn("OpenSSL does not appear to be installed. ksqlDB will fall back to using the JDK "
          + "TLS implementation. OpenSSL is recommended for better performance.");
    }
  }

  public synchronized void start() {
    if (!deploymentIds.isEmpty()) {
      throw new IllegalStateException("Already started");
    }
    final int idleConnectionTimeoutSeconds =
        config.getInt(KsqlRestConfig.IDLE_CONNECTION_TIMEOUT_SECONDS);

    fileWatcher = ApiServerUtils.configureTlsCertReload(config, this::restart);

    final List<URI> listenUris = ApiServerUtils.parseListeners(config);

    final int instances = config.getInt(KsqlRestConfig.VERTICLE_INSTANCES);
    log.debug("Deploying " + instances + " instances of server verticle");

    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();
    for (URI listener : listenUris) {

      for (int i = 0; i < instances; i++) {
        final VertxCompletableFuture<String> vcf = new VertxCompletableFuture<>();
        final Verticle serverVerticle = new PreconditionVerticle(
            createHttpServerOptions(config, listener.getHost(), listener.getPort(),
                listener.getScheme().equalsIgnoreCase("https"),
                idleConnectionTimeoutSeconds),
            serverState
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
    listeners.clear();
    log.info("API server stopped");
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
      final int idleTimeoutSeconds
  ) {

    final HttpServerOptions options = new HttpServerOptions()
        .setHost(host)
        .setPort(port)
        .setReuseAddress(true)
        .setReusePort(true)
        .setIdleTimeout(idleTimeoutSeconds).setIdleTimeoutUnit(TimeUnit.SECONDS)
        .setPerMessageWebSocketCompressionSupported(true)
        .setPerFrameWebSocketCompressionSupported(true);

    if (tls) {
      final String ksConfigName = KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG;
      final ClientAuth clientAuth = ksqlRestConfig.getClientAuth();

      final String alias = ksqlRestConfig.getString(ksConfigName);
      ApiServerUtils.setTlsOptions(ksqlRestConfig, options, alias, clientAuth);
    }
    return options;
  }
}


