/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.services;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.connect.ConnectRequestHeadersExtension;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.util.FileWatcher;
import io.confluent.ksql.util.KsqlConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for managing logic for creating Connect clients, including the auth header
 * that should be sent with connector requests.
 *
 * <p>If a custom auth header is specified
 * as part of the ksqlDB server config, then:
 * <ul>
 *  <li>the header is loaded into {@code connectAuthHeader} the first time {@code get()}
 *      is called.</li>
 *  <li>if configured, a file watcher thread will be started to monitor for changes
 *      to the auth credentials. This file watcher will be started when the credentials
 *      are first loaded.</li>
 * </ul>
 *
 * <p>If no custom auth header is specified, then the auth header of the incoming ksql
 * request, if present, will be sent with the connector request instead.
 */
public class DefaultConnectClientFactory implements ConnectClientFactory {

  private static final Logger log = LoggerFactory.getLogger(DefaultConnectClientFactory.class);

  private final KsqlConfig ksqlConfig;
  private final Optional<ConnectRequestHeadersExtension> requestHeadersExtension;
  private volatile Optional<String> defaultConnectAuthHeader;
  private FileWatcher credentialsFileWatcher;


  public DefaultConnectClientFactory(
      final KsqlConfig ksqlConfig
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.requestHeadersExtension = Optional.ofNullable(
        ksqlConfig.getConfiguredInstance(
            KsqlConfig.CONNECT_REQUEST_HEADERS_PLUGIN,
            ConnectRequestHeadersExtension.class
        ));
  }

  @Override
  public synchronized DefaultConnectClient get(
      final Optional<String> ksqlAuthHeader,
      final List<Entry<String, String>> incomingRequestHeaders,
      final Optional<KsqlPrincipal> userPrincipal
  ) {
    if (defaultConnectAuthHeader == null) {
      defaultConnectAuthHeader = buildDefaultAuthHeader();
    }

    final Map<String, Object> configWithPrefixOverrides =
        ksqlConfig.valuesWithPrefixOverride(KsqlConfig.KSQL_CONNECT_PREFIX);

    return new DefaultConnectClient(
        ksqlConfig.getString(KsqlConfig.CONNECT_URL_PROPERTY),
        buildAuthHeader(ksqlAuthHeader, incomingRequestHeaders),
        requestHeadersExtension
            .map(extension -> extension.getHeaders(userPrincipal))
            .orElse(Collections.emptyMap()),
        Optional.ofNullable(newSslContext(configWithPrefixOverrides)),
        shouldVerifySslHostname(configWithPrefixOverrides),
        ksqlConfig.getLong(KsqlConfig.CONNECT_REQUEST_TIMEOUT_MS)
    );
  }

  @Override
  public synchronized void close() {
    if (credentialsFileWatcher != null) {
      credentialsFileWatcher.shutdown();
    }
  }

  private Optional<String> buildDefaultAuthHeader() {
    if (isCustomBasicAuthConfigured()) {
      final String credentialsFile =
          ksqlConfig.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY);

      if (ksqlConfig.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY)) {
        startBasicAuthFileWatcher(credentialsFile);
      }

      return buildBasicAuthHeader(credentialsFile);
    } else {
      return Optional.empty();
    }
  }

  private Optional<String> buildAuthHeader(
      final Optional<String> ksqlAuthHeader,
      final List<Entry<String, String>> incomingRequestHeaders) {
    // custom extension takes priority
    if (requestHeadersExtension.isPresent()) {
      final ConnectRequestHeadersExtension extension = requestHeadersExtension.get();
      if (extension.shouldUseCustomAuthHeader()) {
        return extension.getAuthHeader(incomingRequestHeaders);
      }
    }

    // check for custom basic auth header
    if (isCustomBasicAuthConfigured()) {
      return defaultConnectAuthHeader;
    }

    // if no custom auth is configured, then forward incoming request header
    return ksqlAuthHeader;
  }

  private void startBasicAuthFileWatcher(final String filePath) {
    try {
      credentialsFileWatcher = new FileWatcher(Paths.get(filePath), () -> {
        defaultConnectAuthHeader = buildBasicAuthHeader(filePath);
      });
      credentialsFileWatcher.start();
      log.info("Enabled automatic connector credentials reload for location: " + filePath);
    } catch (java.io.IOException e) {
      log.error("Failed to enable automatic connector credentials reload", e);
    }
  }

  private boolean isCustomBasicAuthConfigured() {
    return ksqlConfig.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)
        .equalsIgnoreCase(KsqlConfig.BASIC_AUTH_CREDENTIALS_SOURCE_FILE);
  }

  private static Optional<String> buildBasicAuthHeader(final String credentialsPath) {
    if (credentialsPath == null || credentialsPath.isEmpty()) {
      throw new ConfigException(String.format("'%s' cannot be empty if '%s' is set to '%s'",
          KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY,
          KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY,
          KsqlConfig.BASIC_AUTH_CREDENTIALS_SOURCE_FILE));
    }

    final Properties credentials = new Properties();
    try (FileInputStream inputStream = new FileInputStream(credentialsPath)) {
      credentials.load(inputStream);

      if (credentials.containsKey(KsqlConfig.BASIC_AUTH_CREDENTIALS_USERNAME)
          && credentials.containsKey(KsqlConfig.BASIC_AUTH_CREDENTIALS_PASSWORD)) {
        final String userInfo = credentials.getProperty(KsqlConfig.BASIC_AUTH_CREDENTIALS_USERNAME)
            + ":" + credentials.getProperty(KsqlConfig.BASIC_AUTH_CREDENTIALS_PASSWORD);
        return Optional.of("Basic " + Base64.getEncoder()
            .encodeToString(userInfo.getBytes(Charset.defaultCharset())));
      } else {
        log.error("Provided credentials file doesn't provide username and password");
        return Optional.empty();
      }
    } catch (IOException e) {
      log.error("Failed to load credentials file: " + e.getMessage());
      return Optional.empty();
    }
  }

  private static SSLContext newSslContext(final Map<String, Object> config) {
    final DefaultSslEngineFactory sslFactory = new DefaultSslEngineFactory();
    sslFactory.configure(config);
    return sslFactory.sslContext();
  }

  @VisibleForTesting
  static boolean shouldVerifySslHostname(final Map<String, Object> config) {
    final Object endpointIdentificationAlgoConfig =
        config.get(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    if (endpointIdentificationAlgoConfig == null) {
      return false;
    }
    final String endpointIdentificationAlgo = endpointIdentificationAlgoConfig.toString();
    if (endpointIdentificationAlgo.isEmpty() || endpointIdentificationAlgo
        .equalsIgnoreCase(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_NONE)) {
      return false;
    } else if (endpointIdentificationAlgo
        .equalsIgnoreCase(KsqlConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_HTTPS)) {
      return true;
    } else {
      throw new ConfigException("Endpoint identification algorithm not supported: "
          + endpointIdentificationAlgo);
    }
  }
}
