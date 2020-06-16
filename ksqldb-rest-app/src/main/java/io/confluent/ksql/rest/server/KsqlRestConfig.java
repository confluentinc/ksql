/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.ksql.configdef.ConfigValidators.oneOrMore;
import static io.confluent.ksql.configdef.ConfigValidators.zeroOrPositive;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.ErrorMessages;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.vertx.core.http.ClientAuth;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlRestConfig extends AbstractConfig {

  private static final Logger log = LoggerFactory.getLogger(KsqlRestConfig.class);

  private static final Logger LOGGER = LoggerFactory.getLogger(KsqlRestConfig.class);

  public static final String LISTENERS_CONFIG = "listeners";
  protected static final String LISTENERS_DOC =
      "List of listeners. http and https are supported. Each listener must include the protocol, "
          + "hostname, and port. For example: http://myhost:8080, https://0.0.0.0:8081";
  protected static final String LISTENERS_DEFAULT = "http://0.0.0.0:8088";

  public static final String AUTHENTICATION_SKIP_PATHS_CONFIG = "authentication.skip.paths";
  public static final String AUTHENTICATION_SKIP_PATHS_DOC = "Comma separated list of paths that "
      + "can be "
      + "accessed without authentication";
  public static final String AUTHENTICATION_SKIP_PATHS_DEFAULT = "";

  public static final String ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG = "access.control.allow.origin";
  protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DOC = "Set value for "
      + "Access-Control-Allow-Origin header";
  protected static final String ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT = "";

  public static final String ACCESS_CONTROL_ALLOW_METHODS = "access.control.allow.methods";
  protected static final String ACCESS_CONTROL_ALLOW_METHODS_DOC = "Set value to "
      + "Access-Control-Allow-Origin header for specified methods";
  protected static final List<String> ACCESS_CONTROL_ALLOW_METHODS_DEFAULT = Collections
      .emptyList();

  public static final String ACCESS_CONTROL_ALLOW_HEADERS = "access.control.allow.headers";
  protected static final String ACCESS_CONTROL_ALLOW_HEADERS_DOC = "Set value to "
      + "Access-Control-Allow-Origin header for specified headers. Leave blank to use "
      + "default.";
  protected static final List<String> ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT = Collections
      .emptyList();

  public static final String AUTHENTICATION_METHOD_CONFIG = "authentication.method";
  public static final String AUTHENTICATION_METHOD_NONE = "NONE";
  public static final String AUTHENTICATION_METHOD_BASIC = "BASIC";
  public static final String AUTHENTICATION_METHOD_DOC = "Method of authentication. Must be BASIC "
      + "to enable authentication. For BASIC, you must supply a valid JAAS config file "
      + "for the 'java.security.auth.login.config' system property for the appropriate "
      + "authentication provider";
  public static final ValidString AUTHENTICATION_METHOD_VALIDATOR =
      ValidString.in(AUTHENTICATION_METHOD_NONE, AUTHENTICATION_METHOD_BASIC);

  public static final String AUTHENTICATION_REALM_CONFIG = "authentication.realm";
  public static final String AUTHENTICATION_REALM_DOC =
      "Security realm to be used in authentication.";

  public static final String AUTHENTICATION_ROLES_CONFIG = "authentication.roles";
  public static final String AUTHENTICATION_ROLES_DOC = "Valid roles to authenticate against.";
  public static final List<String> AUTHENTICATION_ROLES_DEFAULT = Collections.singletonList("*");

  protected static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";
  protected static final String SSL_KEYSTORE_PASSWORD_DEFAULT = "";

  public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
  protected static final String SSL_KEYSTORE_TYPE_DOC =
      "The type of keystore file. Must be either 'JKS' or 'PKCS12'.";

  protected static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";
  protected static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "";

  public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
  protected static final String SSL_TRUSTSTORE_TYPE_DOC =
      "The type of trust store file. Must be either 'JKS' or 'PKCS12'.";

  public static final String SSL_STORE_TYPE_JKS = "JKS";
  public static final String SSL_STORE_TYPE_PKCS12 = "PKCS12";
  public static final ConfigDef.ValidString SSL_STORE_TYPE_VALIDATOR =
      ConfigDef.ValidString.in(
          SSL_STORE_TYPE_JKS,
          SSL_STORE_TYPE_PKCS12
      );

  public static final String SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth";
  public static final String SSL_CLIENT_AUTHENTICATION_CONFIG = "ssl.client.authentication";
  public static final String SSL_CLIENT_AUTHENTICATION_NONE = "NONE";
  public static final String SSL_CLIENT_AUTHENTICATION_REQUESTED = "REQUESTED";
  public static final String SSL_CLIENT_AUTHENTICATION_REQUIRED = "REQUIRED";
  protected static final String SSL_CLIENT_AUTHENTICATION_DOC =
      "SSL mutual auth. Set to NONE to disable SSL client authentication, set to REQUESTED to "
          + "request but not require SSL client authentication, and set to REQUIRED to require SSL "
          + "client authentication.";
  public static final ConfigDef.ValidString SSL_CLIENT_AUTHENTICATION_VALIDATOR =
      ConfigDef.ValidString.in(
          SSL_CLIENT_AUTHENTICATION_NONE,
          SSL_CLIENT_AUTHENTICATION_REQUESTED,
          SSL_CLIENT_AUTHENTICATION_REQUIRED
      );

  public static final String SSL_KEYSTORE_RELOAD_CONFIG = "ssl.keystore.reload";
  protected static final String SSL_KEYSTORE_RELOAD_DOC =
      "Enable auto reload of ssl keystore.";
  protected static final boolean SSL_KEYSTORE_RELOAD_DEFAULT = false;

  public static final String SSL_KEYSTORE_WATCH_LOCATION_CONFIG = "ssl.keystore.watch.location";
  protected static final String SSL_KEYSTORE_WATCH_LOCATION_DOC =
      "Location to watch for keystore file changes, if different from keystore location.";
  protected static final String SSL_KEYSTORE_WATCH_LOCATION_DEFAULT = "";

  private static final String KSQL_CONFIG_PREFIX = "ksql.";

  private static final String COMMAND_CONSUMER_PREFIX =
      KSQL_CONFIG_PREFIX + "server.command.consumer.";
  private static final String COMMAND_PRODUCER_PREFIX =
      KSQL_CONFIG_PREFIX + "server.command.producer.";

  public static final String ADVERTISED_LISTENER_CONFIG =
      KSQL_CONFIG_PREFIX + "advertised.listener";
  public static final String INTERNAL_LISTENER_CONFIG =
      KSQL_CONFIG_PREFIX + "internal.listener";
  private static final String ADVERTISED_LISTENER_DOC =
      "The listener this node will share with other ksqlDB nodes in the cluster for internal "
          + "communication. In IaaS environments, this may need to be different from the interface "
          + "to which the server binds. "
          + "If this is not set, the advertised listener will either default to "
          +  INTERNAL_LISTENER_CONFIG + ", if set, or else the first value from "
          +  LISTENERS_CONFIG + " will be used. "
          + "It is not valid to use the 0.0.0.0 (IPv4) or [::] (IPv6) wildcard addresses.";

  private static final String INTERNAL_LISTENER_DOC =
      "The listener used for inter-node communication, if different to the '"
          + LISTENERS_CONFIG + "' config. "
          + "The " + ADVERTISED_LISTENER_CONFIG + " config can be set to provide an "
          + "externally routable name for this listener, if required. "
          + "This listener can be used to bind a separate port or network interface for the "
          + "internal endpoints, separate from the external client endpoints, and provide a "
          + "layer of security at the network level.";

  static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG =
      "query.stream.disconnect.check";

  private static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC =
      "How often to send an empty line as part of the response while streaming queries as "
          + "JSON; this helps proactively determine if the connection has been terminated in "
          + "order to avoid keeping the created streams job alive longer than necessary";

  static final String DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "server.command.response.timeout.ms";

  private static final String DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC =
            "How long to wait for a distributed command to be executed by the local node before "
              + "returning a response";

  public static final String INSTALL_DIR_CONFIG = KSQL_CONFIG_PREFIX + "server.install.dir";
  private static final String INSTALL_DIR_DOC
      = "The directory that ksql is installed in. This is set in the ksql-server-start script.";

  static final String KSQL_WEBSOCKETS_NUM_THREADS =
      KSQL_CONFIG_PREFIX + "server.websockets.num.threads";
  private static final String KSQL_WEBSOCKETS_NUM_THREADS_DOC =
      "The number of websocket threads to handle query results";

  static final String KSQL_SERVER_PRECONDITIONS =
      KSQL_CONFIG_PREFIX + "server.preconditions";
  private static final String KSQL_SERVER_PRECONDITIONS_DOC =
      "A comma separated list of classes implementing KsqlServerPrecondition. The KSQL server "
      + "will not start serving requests until all preconditions are satisfied. Until that time, "
      + "requests will return a 503 error";

  static final String KSQL_SERVER_ERROR_MESSAGES =
          KSQL_CONFIG_PREFIX + "server.error.messages";
  private static final String KSQL_SERVER_ERRORS_DOC =
      "A class the implementing " + ErrorMessages.class.getSimpleName() + " interface."
      + "This allows the KSQL server to return pluggable error messages.";

  static final String KSQL_SERVER_ENABLE_UNCAUGHT_EXCEPTION_HANDLER =
      KSQL_CONFIG_PREFIX + "server.exception.uncaught.handler.enable";

  private static final String KSQL_SERVER_UNCAUGHT_EXCEPTION_HANDLER_DOC =
      "Whether or not to set KsqlUncaughtExceptionHandler as the UncaughtExceptionHandler "
          + "for all threads in the application (this can be overridden). Default is false.";

  public static final String KSQL_HEALTHCHECK_INTERVAL_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "healthcheck.interval.ms";
  private static final String KSQL_HEALTHCHECK_INTERVAL_MS_DOC =
      "Minimum time between consecutive health check evaluations. Health check queries before "
          + "the interval has elapsed will receive cached responses.";

  static final String KSQL_COMMAND_RUNNER_BLOCKED_THRESHHOLD_ERROR_MS =
          KSQL_CONFIG_PREFIX + "server.command.blocked.threshold.error.ms";

  private static final String KSQL_COMMAND_RUNNER_BLOCKED_THRESHHOLD_ERROR_MS_DOC =
      "How long to wait for the command runner to process a command from the command topic "
          + "before reporting an error metric.";
  public static final String KSQL_HEARTBEAT_ENABLE_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.enable";
  private static final String KSQL_HEARTBEAT_ENABLE_DOC =
      "Whether the heartheat mechanism is enabled or not. It is disabled by default.";

  public static final String KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.send.interval.ms";
  private static final String KSQL_HEARTBEAT_SEND_INTERVAL_MS_DOC =
      "Interval at which heartbeats are broadcasted to servers.";

  public static final String KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.check.interval.ms";
  private static final String KSQL_HEARTBEAT_CHECK_INTERVAL_MS_DOC =
      "Interval at which server processes received heartbeats.";

  public static final String KSQL_HEARTBEAT_WINDOW_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.window.ms";
  private static final String KSQL_HEARTBEAT_WINDOW_MS_DOC =
      "Size of time window across which to count missed heartbeats.";

  public static final String KSQL_HEARTBEAT_MISSED_THRESHOLD_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.missed.threshold.ms";
  private static final String KSQL_HEARTBEAT_MISSED_THRESHOLD_DOC =
      "Minimum number of consecutive missed heartbeats that flag a server as down.";

  public static final String KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.discover.interval.ms";
  private static final String KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_DOC =
      "Interval at which server attempts to discover what other ksql servers exist in the cluster.";

  public static final String KSQL_HEARTBEAT_THREAD_POOL_SIZE_CONFIG =
      KSQL_CONFIG_PREFIX + "heartbeat.thread.pool.size";
  private static final String KSQL_HEARTBEAT_THREAD_POOL_SIZE_CONFIG_DOC =
      "Size of thread pool used for sending / processing heartbeats and cluster discovery.";

  public static final String KSQL_LAG_REPORTING_ENABLE_CONFIG =
      KSQL_CONFIG_PREFIX + "lag.reporting.enable";
  private static final String KSQL_LAG_REPORTING_ENABLE_DOC =
      "Whether lag reporting is enabled or not. It is disabled by default.";
  public static final String KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "lag.reporting.send.interval.ms";
  private static final String KSQL_LAG_REPORTING_SEND_INTERVAL_MS_DOC =
      "Interval at which lag reports are broadcasted to servers.";

  public static final String VERTICLE_INSTANCES = KSQL_CONFIG_PREFIX + "verticle.instances";
  public static final int DEFAULT_VERTICLE_INSTANCES =
      2 * Runtime.getRuntime().availableProcessors();
  public static final String VERTICLE_INSTANCES_DOC =
      "The number of server verticle instances to start per listener. Usually you want at least "
          + "many instances as there are cores you want to use, as each instance is single "
          + "threaded.";

  public static final String WORKER_POOL_SIZE = KSQL_CONFIG_PREFIX + "worker.pool.size";
  public static final String WORKER_POOL_DOC =
      "Max number of worker threads for executing blocking code";
  public static final int DEFAULT_WORKER_POOL_SIZE = 100;

  public static final String MAX_PUSH_QUERIES = KSQL_CONFIG_PREFIX + "max.push.queries";
  public static final int DEFAULT_MAX_PUSH_QUERIES = 100;
  public static final String MAX_PUSH_QUERIES_DOC =
      "The maximum number of push queries allowed on the server at any one time";

  public static final String KSQL_AUTHENTICATION_PLUGIN_CLASS =
      KSQL_CONFIG_PREFIX + "authentication.plugin.class";
  public static final String KSQL_AUTHENTICATION_PLUGIN_DEFAULT = null;
  public static final String KSQL_AUTHENTICATION_PLUGIN_DOC = "An extension class that allows "
      + " custom authentication to be plugged in.";

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = new ConfigDef()
        .define(
            AUTHENTICATION_SKIP_PATHS_CONFIG,
            Type.LIST,
            AUTHENTICATION_SKIP_PATHS_DEFAULT,
            Importance.LOW,
            AUTHENTICATION_SKIP_PATHS_DOC
        ).define(
            AUTHENTICATION_METHOD_CONFIG,
            Type.STRING,
            AUTHENTICATION_METHOD_NONE,
            AUTHENTICATION_METHOD_VALIDATOR,
            Importance.LOW,
            AUTHENTICATION_METHOD_DOC
        ).define(
            AUTHENTICATION_REALM_CONFIG,
            Type.STRING,
            "",
            Importance.LOW,
            AUTHENTICATION_REALM_DOC
        ).define(
            AUTHENTICATION_ROLES_CONFIG,
            Type.LIST,
            AUTHENTICATION_ROLES_DEFAULT,
            Importance.LOW,
            AUTHENTICATION_ROLES_DOC
        ).define(
            LISTENERS_CONFIG,
            Type.LIST,
            LISTENERS_DEFAULT,
            Importance.HIGH,
            LISTENERS_DOC
        ).define(
            ACCESS_CONTROL_ALLOW_ORIGIN_CONFIG,
            Type.STRING,
            ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT,
            Importance.LOW,
            ACCESS_CONTROL_ALLOW_ORIGIN_DOC
        ).define(
            ACCESS_CONTROL_ALLOW_METHODS,
            Type.LIST,
            ACCESS_CONTROL_ALLOW_METHODS_DEFAULT,
            Importance.LOW,
            ACCESS_CONTROL_ALLOW_METHODS_DOC
        ).define(
            ACCESS_CONTROL_ALLOW_HEADERS,
            Type.LIST,
            ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT,
            Importance.LOW,
            ACCESS_CONTROL_ALLOW_HEADERS_DOC
        ).define(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            Type.STRING,
            SSL_KEYSTORE_LOCATION_DEFAULT,
            Importance.HIGH,
            SslConfigs.SSL_KEYSTORE_LOCATION_DOC
        ).define(
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            Type.PASSWORD,
            SSL_KEYSTORE_PASSWORD_DEFAULT,
            Importance.HIGH,
            SslConfigs.SSL_KEYSTORE_PASSWORD_DOC
        ).define(
            SSL_KEYSTORE_TYPE_CONFIG,
            Type.STRING,
            SSL_STORE_TYPE_JKS,
            SSL_STORE_TYPE_VALIDATOR,
            Importance.MEDIUM,
            SSL_KEYSTORE_TYPE_DOC
        ).define(
            SSL_KEYSTORE_RELOAD_CONFIG,
            Type.BOOLEAN,
            SSL_KEYSTORE_RELOAD_DEFAULT,
            Importance.LOW,
            SSL_KEYSTORE_RELOAD_DOC
        ).define(
            SSL_KEYSTORE_WATCH_LOCATION_CONFIG,
            Type.STRING,
            SSL_KEYSTORE_WATCH_LOCATION_DEFAULT,
            Importance.LOW,
            SSL_KEYSTORE_WATCH_LOCATION_DOC
        ).define(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            Type.STRING,
            SSL_TRUSTSTORE_LOCATION_DEFAULT,
            Importance.HIGH,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC
        ).define(
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            Type.PASSWORD,
            SSL_TRUSTSTORE_PASSWORD_DEFAULT,
            Importance.HIGH,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC
        ).define(
            SSL_TRUSTSTORE_TYPE_CONFIG,
            Type.STRING,
            SSL_STORE_TYPE_JKS,
            SSL_STORE_TYPE_VALIDATOR,
            Importance.MEDIUM,
            SSL_TRUSTSTORE_TYPE_DOC
        ).define(
            SSL_CLIENT_AUTHENTICATION_CONFIG,
            Type.STRING,
            SSL_CLIENT_AUTHENTICATION_NONE,
            SSL_CLIENT_AUTHENTICATION_VALIDATOR,
            Importance.MEDIUM,
            SSL_CLIENT_AUTHENTICATION_DOC
        ).define(
            SSL_CLIENT_AUTH_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            ""
        ).define(
            ADVERTISED_LISTENER_CONFIG,
            Type.STRING,
            null,
            ConfigValidators.nullsAllowed(ConfigValidators.validUrl()),
            Importance.HIGH,
            ADVERTISED_LISTENER_DOC
        ).define(
            INTERNAL_LISTENER_CONFIG,
            Type.STRING,
            null,
            ConfigValidators.nullsAllowed(ConfigValidators.validUrl()),
            Importance.HIGH,
            INTERNAL_LISTENER_DOC
        ).define(
            STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG,
            Type.LONG,
            1000L,
            Importance.LOW,
            STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC
        ).define(
            DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG,
            Type.LONG,
            5000L,
            Importance.LOW,
            DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC
        ).define(
            INSTALL_DIR_CONFIG,
            Type.STRING,
            "",
            Importance.LOW,
            INSTALL_DIR_DOC
        ).define(
            KSQL_WEBSOCKETS_NUM_THREADS,
            Type.INT,
            5,
            Importance.LOW,
            KSQL_WEBSOCKETS_NUM_THREADS_DOC
        ).define(
            KSQL_SERVER_PRECONDITIONS,
            Type.LIST,
            "",
            Importance.LOW,
            KSQL_SERVER_PRECONDITIONS_DOC
        ).define(
            KSQL_SERVER_ENABLE_UNCAUGHT_EXCEPTION_HANDLER,
            ConfigDef.Type.BOOLEAN,
            false,
            Importance.LOW,
            KSQL_SERVER_UNCAUGHT_EXCEPTION_HANDLER_DOC
        ).define(
            KSQL_HEALTHCHECK_INTERVAL_MS_CONFIG,
            Type.LONG,
            5000L,
            Importance.LOW,
            KSQL_HEALTHCHECK_INTERVAL_MS_DOC
        ).define(
            KSQL_COMMAND_RUNNER_BLOCKED_THRESHHOLD_ERROR_MS,
            Type.LONG,
            15000L,
            Importance.LOW,
            KSQL_COMMAND_RUNNER_BLOCKED_THRESHHOLD_ERROR_MS_DOC
        ).define(
            KSQL_SERVER_ERROR_MESSAGES,
            Type.CLASS,
            DefaultErrorMessages.class,
            Importance.LOW,
            KSQL_SERVER_ERRORS_DOC
        ).define(
            KSQL_HEARTBEAT_ENABLE_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_ENABLE_DOC
        ).define(
            KSQL_HEARTBEAT_SEND_INTERVAL_MS_CONFIG,
            Type.LONG,
            100L,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_SEND_INTERVAL_MS_DOC
        ).define(
            KSQL_HEARTBEAT_CHECK_INTERVAL_MS_CONFIG,
            Type.LONG,
            200L,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_CHECK_INTERVAL_MS_DOC
        ).define(
            KSQL_HEARTBEAT_WINDOW_MS_CONFIG,
            Type.LONG,
            2000L,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_WINDOW_MS_DOC
        ).define(
            KSQL_HEARTBEAT_MISSED_THRESHOLD_CONFIG,
            Type.LONG,
            3L,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_MISSED_THRESHOLD_DOC
        ).define(
            KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_CONFIG,
            Type.LONG,
            2000L,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_DISCOVER_CLUSTER_MS_DOC
        ).define(
            KSQL_HEARTBEAT_THREAD_POOL_SIZE_CONFIG,
            Type.INT,
            3,
            Importance.MEDIUM,
            KSQL_HEARTBEAT_THREAD_POOL_SIZE_CONFIG_DOC
        ).define(
            KSQL_LAG_REPORTING_ENABLE_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            KSQL_LAG_REPORTING_ENABLE_DOC
        ).define(
            KSQL_LAG_REPORTING_SEND_INTERVAL_MS_CONFIG,
            Type.LONG,
            5000L,
            Importance.MEDIUM,
            KSQL_LAG_REPORTING_SEND_INTERVAL_MS_DOC
        ).define(
            VERTICLE_INSTANCES,
            Type.INT,
            DEFAULT_VERTICLE_INSTANCES,
            oneOrMore(),
            Importance.MEDIUM,
            VERTICLE_INSTANCES_DOC
        ).define(
            WORKER_POOL_SIZE,
            Type.INT,
            DEFAULT_WORKER_POOL_SIZE,
            zeroOrPositive(),
            Importance.MEDIUM,
            WORKER_POOL_DOC
        ).define(
            MAX_PUSH_QUERIES,
            Type.INT,
            DEFAULT_MAX_PUSH_QUERIES,
            zeroOrPositive(),
            Importance.MEDIUM,
            MAX_PUSH_QUERIES_DOC
        ).define(
            KSQL_AUTHENTICATION_PLUGIN_CLASS,
            Type.CLASS,
            KSQL_AUTHENTICATION_PLUGIN_DEFAULT,
            ConfigDef.Importance.LOW,
            KSQL_AUTHENTICATION_PLUGIN_DOC
        );
  }

  public KsqlRestConfig(final Map<?, ?> props) {
    super(CONFIG_DEF, props);

    final List<String> listeners = getList(LISTENERS_CONFIG);
    if (listeners.isEmpty()) {
      throw new KsqlException(LISTENERS_CONFIG + " must be supplied.  " + LISTENERS_DOC);
    }

    listeners
        .forEach(listener -> ConfigValidators.validUrl().ensureValid(LISTENERS_CONFIG, listener));
  }

  // Bit of a hack to get around the fact that RestConfig.originals() is private for some reason
  Map<String, Object> getOriginals() {
    return originalsWithPrefix("");
  }

  private Map<String, Object> getPropertiesWithOverrides(final String prefix) {
    final Map<String, Object> result = getOriginals();
    result.putAll(originalsWithPrefix(prefix));
    return result;
  }

  Map<String, Object> getCommandConsumerProperties() {
    return getPropertiesWithOverrides(COMMAND_CONSUMER_PREFIX);
  }

  Map<String, Object> getCommandProducerProperties() {
    return getPropertiesWithOverrides(COMMAND_PRODUCER_PREFIX);
  }

  public Map<String, Object> getKsqlConfigProperties() {
    return getOriginals();
  }

  /**
   * Determines which URL should be used to contact this node from other KSQL nodes.
   *
   * <p>Uses {@code INTER_NODE_LISTENER_CONFIG} by default, or first listener defined in
   * {@code LISTENERS_CONFIG} if {@code INTER_NODE_LISTENER_CONFIG} not set.
   *
   * <p>Method takes a {@code portResolver} to resolve any auto-assigned port in
   * {@code LISTENERS_CONFIG}, (i.e. port {@code 0}.
   *
   * <p>Any loopback or localhost in {@code LISTENERS_CONFIG} will be replaced with local machine
   * name, though this is not guarenteed to work across machines in all circumstance.
   *
   * @param portResolver called to resolve port in first {@code LISTENERS_CONFIG} if {@code 0}.
   * @return the resolved inter-node endpoint to use.
   */
  public URL getInterNodeListener(
      final Function<URL, Integer> portResolver
  ) {
    return getInterNodeListener(portResolver, LOGGER);
  }

  @VisibleForTesting
  URL getInterNodeListener(
      final Function<URL, Integer> portResolver,
      final Logger logger
  ) {
    if (getString(ADVERTISED_LISTENER_CONFIG) == null) {
      return getString(INTERNAL_LISTENER_CONFIG) == null
          ? getInterNodeListenerFromFirstListener(portResolver, logger)
          : getInterNodeListenerFromInternalListener(portResolver, logger);
    } else {
      return getInterNodeListenerFromExplicitConfig(logger);
    }
  }

  private URL getInterNodeListenerFromFirstListener(
      final Function<URL, Integer> portResolver,
      final Logger logger
  ) {
    final List<String> configValue = getList(LISTENERS_CONFIG);

    final URL firstListener = parseUrl(configValue.get(0), LISTENERS_CONFIG);

    final InetAddress address = parseInetAddress(firstListener.getHost())
        .orElseThrow(() -> new ConfigException(
            LISTENERS_CONFIG,
            configValue,
            "Could not resolve first host"
        ));

    final URL listener = sanitizeInterNodeListener(
        firstListener,
        portResolver,
        address.isAnyLocalAddress()
    );

    logInterNodeListener(
        logger,
        listener,
        Optional.of(address),
        "first '" + LISTENERS_CONFIG + "'"
    );

    return listener;
  }

  private URL getInterNodeListenerFromInternalListener(
      final Function<URL, Integer> portResolver,
      final Logger logger
  ) {
    final String configValue = getString(INTERNAL_LISTENER_CONFIG);

    final URL internalListener = parseUrl(configValue, INTERNAL_LISTENER_CONFIG);

    final InetAddress address = parseInetAddress(internalListener.getHost())
        .orElseThrow(() -> new ConfigException(
            INTERNAL_LISTENER_CONFIG,
            configValue,
            "Could not resolve internal host"
        ));

    final URL listener = sanitizeInterNodeListener(
        internalListener,
        portResolver,
        address.isAnyLocalAddress()
    );

    logInterNodeListener(
        logger,
        listener,
        Optional.of(address),
        "'" + INTERNAL_LISTENER_CONFIG + "'"
    );

    return listener;
  }

  private URL getInterNodeListenerFromExplicitConfig(final Logger logger) {
    final String configValue = getString(ADVERTISED_LISTENER_CONFIG);

    final URL listener = parseUrl(configValue, ADVERTISED_LISTENER_CONFIG);

    if (listener.getPort() <= 0) {
      throw new ConfigException(ADVERTISED_LISTENER_CONFIG, configValue, "Must have valid port");
    }

    // Valid for address to not be resolvable, as may be _externally_ resolvable:
    final Optional<InetAddress> address = parseInetAddress(listener.getHost());

    address.ifPresent(a -> {
      if (a.isAnyLocalAddress()) {
        throw new ConfigException(ADVERTISED_LISTENER_CONFIG, configValue, "Can not be wildcard");
      }
    });

    final URL sanitized = sanitizeInterNodeListener(
        listener,
        foo -> listener.getPort(),
        false
    );

    logInterNodeListener(
        logger,
        sanitized,
        address,
        "'" + ADVERTISED_LISTENER_CONFIG + "'"
    );

    return sanitized;
  }

  private static void logInterNodeListener(
      final Logger logger,
      final URL listener,
      final Optional<InetAddress> address,
      final String sourceConfigName
  ) {
    address.ifPresent(a -> {
      if (a.isLoopbackAddress()) {
        logger.warn(
            "{} config is set to a loopback address: {}. Intra-node communication will only work "
                + "between nodes running on the same machine.",
            sourceConfigName, listener
        );
      }

      if (a.isAnyLocalAddress()) {
        logger.warn(
            "{} config uses wildcard address: {}. Intra-node communication will only work "
                + "between nodes running on the same machine.",
            sourceConfigName, listener
        );
      }
    });

    logger.info("Using {} config for intra-node communication: {}", sourceConfigName, listener);
  }

  public ClientAuth getClientAuth() {

    String clientAuth = getString(SSL_CLIENT_AUTHENTICATION_CONFIG);
    if (originals().containsKey(SSL_CLIENT_AUTH_CONFIG)) {
      if (originals().containsKey(SSL_CLIENT_AUTHENTICATION_CONFIG)) {
        log.warn(
            "The {} configuration is deprecated. Since a value has been supplied for the {} "
                + "configuration, that will be used instead",
            SSL_CLIENT_AUTH_CONFIG,
            SSL_CLIENT_AUTHENTICATION_CONFIG
        );
      } else {
        log.warn(
            "The configuration {} is deprecated and should be replaced with {}",
            SSL_CLIENT_AUTH_CONFIG,
            SSL_CLIENT_AUTHENTICATION_CONFIG
        );
        clientAuth = getBoolean(SSL_CLIENT_AUTH_CONFIG)
            ? SSL_CLIENT_AUTHENTICATION_REQUIRED
            : SSL_CLIENT_AUTHENTICATION_NONE;
      }
    }

    switch (clientAuth) {
      case SSL_CLIENT_AUTHENTICATION_NONE:
        return ClientAuth.NONE;
      case SSL_CLIENT_AUTHENTICATION_REQUESTED:
        return ClientAuth.REQUEST;
      case SSL_CLIENT_AUTHENTICATION_REQUIRED:
        return ClientAuth.REQUIRED;
      default:
        throw new ConfigException("Unknown client auth: " + clientAuth);
    }
  }

  /**
   * Used to sanitize the first `listener` config.
   *
   * <p>It will:
   * <ul>
   *    <li>resolve any auto-port assignment to the actual port the server is listening on</li>
   *    <li>potentially, replace the host with localhost. This can be useful where the first
   *    listener is a wildcard address, e.g. {@code 0.0.0.0}/li>
   * </ul>
   *
   * @param listener the URL to sanitize
   * @param portResolver the function to call to resolve the port.
   * @param replaceHost flag indicating if the host in the URL should be replaced with localhost.
   * @return the sanitized URL.
   */
  private static URL sanitizeInterNodeListener(
      final URL listener,
      final Function<URL, Integer> portResolver,
      final boolean replaceHost
  ) {
    final String host = replaceHost
        ? getLocalHostName()
        : listener.getHost();

    final int port = listener.getPort() == 0
        ? portResolver.apply(listener)
        : listener.getPort();

    try {
      return new URL(listener.getProtocol(), host, port, "");
    } catch (final MalformedURLException e) {
      throw new KsqlServerException("Resolved first listener to malformed URL", e);
    }
  }

  private static URL parseUrl(final String address, final String configName) {
    try {
      return new URL(address);
    } catch (final MalformedURLException e) {
      throw new ConfigException(configName, address, e.getMessage());
    }
  }


  private static Optional<InetAddress> parseInetAddress(final String address) {
    try {
      return Optional.of(InetAddress.getByName(address));
    } catch (final UnknownHostException e) {
      return Optional.empty();
    }
  }

  private static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (final UnknownHostException e) {
      throw new KsqlServerException("Failed to obtain local host info", e);
    }
  }

}
