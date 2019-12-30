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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.ErrorMessages;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.rest.RestConfig;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlRestConfig extends RestConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(KsqlRestConfig.class);

  private static final String KSQL_CONFIG_PREFIX = "ksql.";

  private static final String COMMAND_CONSUMER_PREFIX =
      KSQL_CONFIG_PREFIX + "server.command.consumer.";
  private static final String COMMAND_PRODUCER_PREFIX =
      KSQL_CONFIG_PREFIX + "server.command.producer.";

  public static final String ADVERTISED_LISTENER_CONFIG =
      KSQL_CONFIG_PREFIX + "advertised.listener";
  private static final String ADVERTISED_LISTENER_DOC =
      "The listener used for communication between KSQL nodes in the cluster, if different to the '"
          + LISTENERS_CONFIG + "' config property. "
          + "In IaaS environments, this may need to be different from the interface to which "
          + "the server binds. If this is not set, the first value from listeners will be used. "
          + "Unlike listeners, it is not valid to use the 0.0.0.0 (IPv4) or [::] (IPv6) "
          + "wildcard addresses.";

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

  static final String COMMAND_TOPIC_SUFFIX = "command_topic";

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

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = baseConfigDef().define(
        ADVERTISED_LISTENER_CONFIG,
        Type.STRING,
        null,
        ConfigValidators.nullsAllowed(ConfigValidators.validUrl()),
        Importance.HIGH,
        ADVERTISED_LISTENER_DOC
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
    )
    .define(
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
    return getString(ADVERTISED_LISTENER_CONFIG) == null
        ? getInterNodeListenerFromFirstListener(portResolver, logger)
        : getInterNodeListenerFromExplicitConfig(logger);
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

    logInterNodeListener(
        logger,
        listener,
        address,
        "'" + ADVERTISED_LISTENER_CONFIG + "'"
    );

    return listener;
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
    if (!replaceHost && listener.getPort() > 0) {
      return listener;
    }

    final String host = replaceHost
        ? getLocalHostName()
        : listener.getHost();

    final int port = listener.getPort() == 0
        ? portResolver.apply(listener)
        : listener.getPort();

    try {
      return new URL(listener.getProtocol(), host, port, listener.getFile());
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
