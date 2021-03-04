/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.util;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.net.MalformedURLException;
import java.net.URL;

public final class MigrationsUtil {

  private MigrationsUtil() {
  }

  public static final String MIGRATIONS_COMMAND = "ksql-migrations";

  public static Client getKsqlClient(final MigrationConfig config) throws MigrationException {
    return getKsqlClient(
        config.getString(MigrationConfig.KSQL_SERVER_URL),
        config.getString(MigrationConfig.KSQL_BASIC_AUTH_USERNAME),
        config.getString(MigrationConfig.KSQL_BASIC_AUTH_PASSWORD),
        config.getString(MigrationConfig.SSL_TRUSTSTORE_LOCATION),
        config.getString(MigrationConfig.SSL_TRUSTSTORE_PASSWORD),
        config.getString(MigrationConfig.SSL_KEYSTORE_LOCATION),
        config.getString(MigrationConfig.SSL_KEYSTORE_PASSWORD),
        config.getString(MigrationConfig.SSL_KEY_PASSWORD),
        config.getString(MigrationConfig.SSL_KEY_ALIAS),
        config.getBoolean(MigrationConfig.SSL_ALPN),
        config.getBoolean(MigrationConfig.SSL_VERIFY_HOST)
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public static Client getKsqlClient(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String ksqlServerUrl,
      final String username,
      final String password,
      final String sslTrustStoreLocation,
      final String sslTrustStorePassword,
      final String sslKeystoreLocation,
      final String sslKeystorePassword,
      final String sslKeyPassword,
      final String sslKeyAlias,
      final boolean sslAlpn,
      final boolean sslVerifyHost
  ) {
    return Client.create(createClientOptions(ksqlServerUrl, username, password,
        sslTrustStoreLocation, sslTrustStorePassword, sslKeystoreLocation, sslKeystorePassword,
        sslKeyPassword, sslKeyAlias, sslAlpn, sslVerifyHost));
  }

  @VisibleForTesting
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  static ClientOptions createClientOptions(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String ksqlServerUrl,
      final String username,
      final String password,
      final String sslTrustStoreLocation,
      final String sslTrustStorePassword,
      final String sslKeystoreLocation,
      final String sslKeystorePassword,
      final String sslKeyPassword,
      final String sslKeyAlias,
      final boolean useAlpn,
      final boolean verifyHost
  ) {
    final URL url;
    try {
      url = new URL(ksqlServerUrl);
    } catch (MalformedURLException e) {
      throw new MigrationException("Invalid ksql server URL: " + ksqlServerUrl);
    }

    final ClientOptions options = ClientOptions
        .create()
        .setHost(url.getHost())
        .setPort(url.getPort());

    if (username != null || password != null) {
      options.setBasicAuthCredentials(username, password);
    }

    final boolean useTls = ksqlServerUrl.trim().toLowerCase().startsWith("https://");
    options.setUseTls(useTls);

    if (useTls) {
      options.setTrustStore(sslTrustStoreLocation);
      options.setTrustStorePassword(sslTrustStorePassword);
      options.setKeyStore(sslKeystoreLocation);
      options.setKeyStorePassword(sslKeystorePassword);
      options.setKeyPassword(sslKeyPassword);
      options.setKeyAlias(sslKeyAlias);
      options.setUseAlpn(useAlpn);
      options.setVerifyHost(verifyHost);
    }
    return options;
  }
}
