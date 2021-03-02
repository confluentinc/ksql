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
        config.getString(MigrationConfig.KSQL_USERNAME),
        config.getString(MigrationConfig.KSQL_PASSWORD),
        config.getString(MigrationConfig.SSL_TRUSTSTORE_LOCATION),
        config.getString(MigrationConfig.SSL_TRUSTSTORE_PASSWORD),
        config.getString(MigrationConfig.SSL_KEYSTORE_LOCATION),
        config.getString(MigrationConfig.SSL_KEYSTORE_PASSWORD),
        config.getString(MigrationConfig.SSL_KEY_PASSWORD),
        config.getString(MigrationConfig.SSL_KEY_ALIAS),
        config.getBoolean(MigrationConfig.SSL_ALPN)
    );
  }

  public static Client getKsqlClient(
      final String ksqlServerUrl,
      final String username,
      final String password,
      final String sslTrustStoreLocation,
      final String sslTrustStorePassword,
      final String sslKeystoreLocation,
      final String sslKeystorePassword,
      final String sslKeyPassword,
      final String sslKeyAlias,
      final boolean sslAlpn
  ) throws MigrationException {
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

    if (sslTrustStoreLocation != null
        || sslKeystoreLocation != null
        || sslKeyPassword != null
        || sslKeyAlias != null
    ) {
      options.setUseTls(true);
    }

    options.setTrustStore(sslTrustStoreLocation);
    options.setTrustStorePassword(sslTrustStorePassword);
    options.setKeyStore(sslKeystoreLocation);
    options.setKeyStorePassword(sslKeystorePassword);
    options.setKeyPassword(sslKeyPassword);
    options.setKeyAlias(sslKeyAlias);
    options.setUseAlpn(sslAlpn);

    return Client.create(options);
  }
}
