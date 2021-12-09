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

import io.confluent.ksql.services.ConnectClient.ConnectClientFactory;
import io.confluent.ksql.util.FileWatcher;
import io.confluent.ksql.util.KsqlConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConnectClientFactory implements ConnectClientFactory {

  private static final Logger log = LoggerFactory.getLogger(DefaultConnectClientFactory.class);

  private final KsqlConfig ksqlConfig;
  private Optional<String> connectAuthHeader;
  private FileWatcher credentialsFileWatcher;

  public DefaultConnectClientFactory(
      final KsqlConfig ksqlConfig
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
  }

  @Override
  public DefaultConnectClient get(final Optional<String> ksqlAuthHeader) {
    if (connectAuthHeader == null) {
      connectAuthHeader = buildAuthHeader();
    }

    return new DefaultConnectClient(
        ksqlConfig.getString(KsqlConfig.CONNECT_URL_PROPERTY),
        // if no explicit header specified, then forward incoming request header
        connectAuthHeader.isPresent() ? connectAuthHeader : ksqlAuthHeader
    );
  }

  @Override
  public void close() {
    if (credentialsFileWatcher != null) {
      credentialsFileWatcher.shutdown();
    }
  }

  private Optional<String> buildAuthHeader() {
    // custom basic auth credentials
    if (ksqlConfig.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY)
        .equalsIgnoreCase(KsqlConfig.BASIC_AUTH_CREDENTIALS_SOURCE_FILE)) {
      final String credentialsFile =
          ksqlConfig.getString(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY);
      final boolean failOnUnreadableCreds =
          ksqlConfig.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_FAIL_ON_UNREADABLE_CREDENTIALS);

      if (ksqlConfig.getBoolean(KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_RELOAD_PROPERTY)) {
        startBasicAuthFileWatcher(credentialsFile, failOnUnreadableCreds);
      }

      return buildBasicAuthHeader(credentialsFile, failOnUnreadableCreds);
    } else {
      return Optional.empty();
    }
  }

  private void startBasicAuthFileWatcher(
      final String filePath,
      final boolean failOnUnreadableCreds
  ) {
    try {
      credentialsFileWatcher = new FileWatcher(Paths.get(filePath), () -> {
        connectAuthHeader = buildBasicAuthHeader(filePath, failOnUnreadableCreds);
      });
      credentialsFileWatcher.start();
      log.info("Enabled automatic connector credentials reload for location: " + filePath);
    } catch (java.io.IOException e) {
      log.error("Failed to enable automatic connector credentials reload.");
    }
  }

  private static Optional<String> buildBasicAuthHeader(
      final String credentialsPath,
      final boolean failOnUnreadableCredentials
  ) {
    if (credentialsPath == null || credentialsPath.isEmpty()) {
      throw new ConfigException(String.format("'%s' cannot be empty if '%s' is set to '%s'",
          KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_FILE_PROPERTY,
          KsqlConfig.CONNECT_BASIC_AUTH_CREDENTIALS_SOURCE_PROPERTY,
          KsqlConfig.BASIC_AUTH_CREDENTIALS_SOURCE_FILE));
    }

    final Properties credentials = new Properties();
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(credentialsPath);
      credentials.load(inputStream);

      if (credentials.containsKey(KsqlConfig.BASIC_AUTH_CREDENTIALS_USERNAME)
          && credentials.containsKey(KsqlConfig.BASIC_AUTH_CREDENTIALS_PASSWORD)) {
        final String userInfo = credentials.getProperty(KsqlConfig.BASIC_AUTH_CREDENTIALS_USERNAME)
            + ":" + credentials.getProperty(KsqlConfig.BASIC_AUTH_CREDENTIALS_PASSWORD);
        return Optional.of("Basic " + Base64.getEncoder()
            .encodeToString(userInfo.getBytes(Charset.defaultCharset())));
      } else {
        if (failOnUnreadableCredentials) {
          throw new ConfigException(
              "Provided credentials file doesn't provide username and password");
        } else {
          log.warn("Provided credentials file doesn't provide username and password");
          return Optional.empty();
        }
      }
    } catch (IOException e) {
      if (failOnUnreadableCredentials) {
        throw new ConfigException(e.getMessage());
      } else {
        log.warn("Failed to load credentials file: " + e.getMessage());
        return Optional.empty();
      }
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          log.error("Failed to close credentials input stream: " + e.getMessage());
        }
      }
    }
  }
}
