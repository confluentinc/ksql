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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_MAX_PUSH_QUERIES_EXCEEDED;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.auth.AuthenticationPlugin;
import io.confluent.ksql.api.spi.Endpoints;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.entity.PushQueryId;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.state.ServerState;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.util.FileWatcher;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PfxOptions;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
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
  private final KsqlRestConfig config;
  private final Endpoints endpoints;
  private final Map<PushQueryId, PushQueryHolder> queries = new ConcurrentHashMap<>();
  private final Set<HttpConnection> connections = new ConcurrentHashSet<>();
  private final int maxPushQueryCount;
  private final Set<String> deploymentIds = new HashSet<>();
  private final KsqlSecurityExtension securityExtension;
  private final Optional<AuthenticationPlugin> authenticationPlugin;
  private final ServerState serverState;
  private final List<URI> listeners = new ArrayList<>();
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private URI internalListener;
  private WorkerExecutor workerExecutor;
  private FileWatcher fileWatcher;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public Server(
      final Vertx vertx, final KsqlRestConfig config, final Endpoints endpoints,
      final KsqlSecurityExtension securityExtension,
      final Optional<AuthenticationPlugin> authenticationPlugin,
      final ServerState serverState,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics) {
    this.vertx = Objects.requireNonNull(vertx);
    this.config = Objects.requireNonNull(config);
    this.endpoints = Objects.requireNonNull(endpoints);
    this.securityExtension = Objects.requireNonNull(securityExtension);
    this.authenticationPlugin = Objects.requireNonNull(authenticationPlugin);
    this.serverState = Objects.requireNonNull(serverState);
    this.maxPushQueryCount = config.getInt(KsqlRestConfig.MAX_PUSH_QUERIES);
    this.pullQueryMetrics = Objects.requireNonNull(pullQueryMetrics, "pullQueryMetrics");
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

    this.workerExecutor = vertx.createSharedWorkerExecutor("ksql-workers",
        config.getInt(KsqlRestConfig.WORKER_POOL_SIZE));
    final LoggingRateLimiter loggingRateLimiter = new LoggingRateLimiter(config);
    configureTlsCertReload(config);

    final List<URI> listenUris = parseListeners(config);
    final Optional<URI> internalListenUri = parseInternalListener(config, listenUris);
    final List<URI> allListenUris = new ArrayList<>(listenUris);
    internalListenUri.ifPresent(allListenUris::add);

    final int instances = config.getInt(KsqlRestConfig.VERTICLE_INSTANCES);
    log.debug("Deploying " + instances + " instances of server verticle");

    final List<CompletableFuture<String>> deployFutures = new ArrayList<>();
    final Map<URI, URI> uris = new ConcurrentHashMap<>();
    for (URI listener : allListenUris) {
      final Optional<Boolean> isInternalListener =
          internalListenUri.map(uri -> uri.equals(listener));

      for (int i = 0; i < instances; i++) {
        final VertxCompletableFuture<String> vcf = new VertxCompletableFuture<>();
        final ServerVerticle serverVerticle = new ServerVerticle(endpoints,
            createHttpServerOptions(config, listener.getHost(), listener.getPort(),
                listener.getScheme().equalsIgnoreCase("https"), isInternalListener.orElse(false),
                idleConnectionTimeoutSeconds),
            this, isInternalListener, loggingRateLimiter);
        vertx.deployVerticle(serverVerticle, vcf);
        final int index = i;
        final CompletableFuture<String> deployFuture = vcf.thenApply(s -> {
          if (index == 0) {
            try {
              final URI uriWithPort = new URI(listener.getScheme(), null, listener.getHost(),
                  serverVerticle.actualPort(), null, null, null);
              uris.put(listener, uriWithPort);
            } catch (URISyntaxException e) {
              throw new KsqlException(e);
            }
          }
          return s;
        });
        deployFutures.add(deployFuture);
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
    for (URI uri : listenUris) {
      listeners.add(uris.get(uri));
    }
    if (internalListenUri.isPresent()) {
      internalListener = uris.get(internalListenUri.get());
    }
    log.info("API server started");
  }

  public synchronized void stop() {
    if (deploymentIds.isEmpty()) {
      throw new IllegalStateException("Not started");
    }
    if (workerExecutor != null) {
      workerExecutor.close();
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

  public synchronized void restart() {
    log.info("Restarting server");
    stop();
    start();
  }

  public WorkerExecutor getWorkerExecutor() {
    return workerExecutor;
  }

  synchronized void registerQuery(final PushQueryHolder query) throws KsqlApiException {
    Objects.requireNonNull(query);
    if (queries.size() == maxPushQueryCount) {
      throw new KsqlApiException("Maximum number of push queries exceeded",
          ERROR_CODE_MAX_PUSH_QUERIES_EXCEEDED);
    }
    if (queries.putIfAbsent(query.getId(), query) != null) {
      // It should never happen.  QueryIds are designed to not collide.
      throw new IllegalStateException("Glitch in the matrix");
    }
  }

  Optional<PushQueryHolder> removeQuery(final PushQueryId queryId) {
    return Optional.ofNullable(queries.remove(queryId));
  }

  public Set<PushQueryId> getQueryIDs() {
    return new HashSet<>(queries.keySet());
  }

  KsqlSecurityExtension getSecurityExtension() {
    return securityExtension;
  }

  public Optional<AuthenticationPlugin> getAuthenticationPlugin() {
    return authenticationPlugin;
  }

  ServerState getServerState() {
    return serverState;
  }

  public KsqlRestConfig getConfig() {
    return config;
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

  public synchronized List<URI> getListeners() {
    return ImmutableList.copyOf(listeners);
  }

  public synchronized Optional<URI> getInternalListener() {
    return Optional.ofNullable(internalListener);
  }

  private void configureTlsCertReload(final KsqlRestConfig config) {
    if (config.getBoolean(KsqlRestConfig.SSL_KEYSTORE_RELOAD_CONFIG)) {
      final Path watchLocation;
      if (!config.getString(KsqlRestConfig.SSL_KEYSTORE_WATCH_LOCATION_CONFIG).isEmpty()) {
        watchLocation = Paths.get(
            config.getString(KsqlRestConfig.SSL_KEYSTORE_WATCH_LOCATION_CONFIG));
      } else {
        watchLocation = Paths.get(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
      }

      try {
        fileWatcher = new FileWatcher(watchLocation, this::restart);
        fileWatcher.start();
        log.info("Enabled SSL cert auto reload for: " + watchLocation);
      } catch (java.io.IOException e) {
        log.error("Failed to enable SSL cert auto reload", e);
      }
    }
  }

  private static HttpServerOptions createHttpServerOptions(final KsqlRestConfig ksqlRestConfig,
      final String host, final int port, final boolean tls,
      final boolean isInternalListener, final int idleTimeoutSeconds) {

    final HttpServerOptions options = new HttpServerOptions()
        .setHost(host)
        .setPort(port)
        .setReuseAddress(true)
        .setReusePort(true)
        .setIdleTimeout(idleTimeoutSeconds).setIdleTimeoutUnit(TimeUnit.SECONDS)
        .setPerMessageWebSocketCompressionSupported(true)
        .setPerFrameWebSocketCompressionSupported(true);

    if (tls) {
      final String ksConfigName = isInternalListener
           ? KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG
           : KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG;
      final ClientAuth clientAuth = isInternalListener
          ? ksqlRestConfig.getClientAuthInternal()
          : ksqlRestConfig.getClientAuth();

      final String alias = ksqlRestConfig.getString(ksConfigName);
      setTlsOptions(ksqlRestConfig, options, alias, clientAuth);
    }
    return options;
  }

  private static void setTlsOptions(
      final KsqlRestConfig ksqlRestConfig,
      final HttpServerOptions options,
      final String keyStoreAlias,
      final ClientAuth clientAuth
  ) {
    options.setUseAlpn(true).setSsl(true);
    if (ksqlRestConfig.getBoolean(KsqlRestConfig.KSQL_SERVER_SNI_CHECK_ENABLE)) {
      options.setSni(true);
    }

    configureTlsKeyStore(ksqlRestConfig, options, keyStoreAlias);
    configureTlsTrustStore(ksqlRestConfig, options);

    final List<String> enabledProtocols =
        ksqlRestConfig.getList(KsqlRestConfig.SSL_ENABLED_PROTOCOLS_CONFIG);
    if (!enabledProtocols.isEmpty()) {
      options.setEnabledSecureTransportProtocols(new HashSet<>(enabledProtocols));
    }

    final List<String> cipherSuites =
        ksqlRestConfig.getList(KsqlRestConfig.SSL_CIPHER_SUITES_CONFIG);
    if (!cipherSuites.isEmpty()) {
      // Vert.x does not yet support a method for setting cipher suites, so we use the following
      // workaround instead. See https://github.com/eclipse-vertx/vert.x/issues/1507.
      final Set<String> enabledCipherSuites = options.getEnabledCipherSuites();
      enabledCipherSuites.clear();
      enabledCipherSuites.addAll(cipherSuites);
    }

    options.setClientAuth(clientAuth);
  }

  private static void configureTlsKeyStore(
      final KsqlRestConfig ksqlRestConfig,
      final HttpServerOptions httpServerOptions,
      final String keyStoreAlias
  ) {
    final Map<String, String> props = PropertiesUtil.toMapStrings(ksqlRestConfig.originals());
    final String keyStoreType = ksqlRestConfig.getString(KsqlRestConfig.SSL_KEYSTORE_TYPE_CONFIG);

    if (keyStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_JKS)) {
      final Optional<JksOptions> keyStoreOptions =
          VertxSslOptionsFactory.buildJksKeyStoreOptions(props, Optional.ofNullable(keyStoreAlias));

      keyStoreOptions.ifPresent(options -> httpServerOptions.setKeyStoreOptions(options));
    } else if (keyStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_PKCS12)) {
      final Optional<PfxOptions> keyStoreOptions =
          VertxSslOptionsFactory.getPfxKeyStoreOptions(props);

      keyStoreOptions.ifPresent(options -> httpServerOptions.setPfxKeyCertOptions(options));
    }
  }

  private static void configureTlsTrustStore(
      final KsqlRestConfig ksqlRestConfig,
      final HttpServerOptions httpServerOptions
  ) {
    final Map<String, String> props = PropertiesUtil.toMapStrings(ksqlRestConfig.originals());
    final String trustStoreType =
        ksqlRestConfig.getString(KsqlRestConfig.SSL_TRUSTSTORE_TYPE_CONFIG);

    if (trustStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_JKS)) {
      final Optional<JksOptions> trustStoreOptions =
          VertxSslOptionsFactory.getJksTrustStoreOptions(props);

      trustStoreOptions.ifPresent(options -> httpServerOptions.setTrustOptions(options));
    } else if (trustStoreType.equals(KsqlRestConfig.SSL_STORE_TYPE_PKCS12)) {
      final Optional<PfxOptions> trustStoreOptions =
          VertxSslOptionsFactory.getPfxTrustStoreOptions(props);

      trustStoreOptions.ifPresent(options -> httpServerOptions.setTrustOptions(options));
    }
  }

  private static List<URI> parseListeners(final KsqlRestConfig config) {
    final List<String> sListeners = config.getList(KsqlRestConfig.LISTENERS_CONFIG);
    return parseListenerStrings(config, sListeners);
  }

  private static Optional<URI> parseInternalListener(
      final KsqlRestConfig config,
      final List<URI> listenUris
  ) {
    if (config.getString(KsqlRestConfig.INTERNAL_LISTENER_CONFIG) == null) {
      return Optional.empty();
    }
    final URI uri = parseListenerStrings(config,
        ImmutableList.of(config.getString(KsqlRestConfig.INTERNAL_LISTENER_CONFIG))).get(0);
    if (listenUris.contains(uri)) {
      return Optional.empty();
    } else {
      return Optional.of(uri);
    }
  }

  private static List<URI> parseListenerStrings(
      final KsqlRestConfig config,
      final List<String> stringListeners) {
    final List<URI> listeners = new ArrayList<>();
    for (String listenerName : stringListeners) {
      try {
        final URI uri = new URI(listenerName);
        final String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
          throw new ConfigException("Invalid URI scheme should be http or https: " + listenerName);
        }
        if ("https".equalsIgnoreCase(scheme)) {
          final String keyStoreLocation = config
              .getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
          if (keyStoreLocation == null || keyStoreLocation.isEmpty()) {
            throw new ConfigException("https listener specified but no keystore provided");
          }
        }
        listeners.add(uri);
      } catch (URISyntaxException e) {
        throw new ConfigException("Invalid listener URI: " + listenerName);
      }
    }
    return listeners;
  }
}
