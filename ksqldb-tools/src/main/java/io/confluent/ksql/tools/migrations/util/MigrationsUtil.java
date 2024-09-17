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
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.security.AuthType;
import io.confluent.ksql.security.oauth.IdpConfig;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.util.KsqlException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public final class MigrationsUtil {

  private MigrationsUtil() {
  }

  public static final String MIGRATIONS_COMMAND = "ksql-migrations";

  public static Client getKsqlClient(final MigrationConfig config) throws MigrationException {
    return getKsqlClient(config, null);
  }

  public static Client getKsqlClient(final MigrationConfig config, final String headersFile)
      throws MigrationException {
    return getKsqlClient(
        config.getString(MigrationConfig.KSQL_SERVER_URL),
        config.getString(MigrationConfig.KSQL_BASIC_AUTH_USERNAME),
        config.getPassword(MigrationConfig.KSQL_BASIC_AUTH_PASSWORD).value(),
        config.getString(MigrationConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL),
        config.getString(MigrationConfig.BEARER_AUTH_CLIENT_ID),
        config.getPassword(MigrationConfig.BEARER_AUTH_CLIENT_SECRET).value(),
        config.getString(MigrationConfig.BEARER_AUTH_SCOPE),
        config.getString(MigrationConfig.BEARER_AUTH_SCOPE_CLAIM_NAME),
        config.getString(MigrationConfig.BEARER_AUTH_SUB_CLAIM_NAME),
        config.getShort(MigrationConfig.BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS),
        config.getString(MigrationConfig.SSL_TRUSTSTORE_LOCATION),
        config.getPassword(MigrationConfig.SSL_TRUSTSTORE_PASSWORD).value(),
        config.getString(MigrationConfig.SSL_KEYSTORE_LOCATION),
        config.getPassword(MigrationConfig.SSL_KEYSTORE_PASSWORD).value(),
        config.getPassword(MigrationConfig.SSL_KEY_PASSWORD).value(),
        config.getString(MigrationConfig.SSL_KEY_ALIAS),
        config.getBoolean(MigrationConfig.SSL_ALPN),
        config.getBoolean(MigrationConfig.SSL_VERIFY_HOST),
        loadRequestHeaders(headersFile)
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public static Client getKsqlClient(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String ksqlServerUrl,
      final String username,
      final String password,
      final String idpTokenEndpointUrl,
      final String idpClientId,
      final String idpClientSecret,
      final String idpScope,
      final String idpScopeClaimName,
      final String idpSubClaimName,
      final Short idpCacheExpiryBufferSeconds,
      final String sslTrustStoreLocation,
      final String sslTrustStorePassword,
      final String sslKeystoreLocation,
      final String sslKeystorePassword,
      final String sslKeyPassword,
      final String sslKeyAlias,
      final boolean sslAlpn,
      final boolean sslVerifyHost,
      final Map<String, String> requestHeaders
  ) {
    return Client.create(createClientOptions(ksqlServerUrl, username, password,
        idpTokenEndpointUrl, idpClientId, idpClientSecret,
        idpScope, idpScopeClaimName, idpSubClaimName, idpCacheExpiryBufferSeconds,
        sslTrustStoreLocation, sslTrustStorePassword, sslKeystoreLocation, sslKeystorePassword,
        sslKeyPassword, sslKeyAlias, sslAlpn, sslVerifyHost, requestHeaders));
  }

  @VisibleForTesting
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @SuppressWarnings(value = {"NPathComplexity", "CyclomaticComplexity"})
  static ClientOptions createClientOptions(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String ksqlServerUrl,
      final String username,
      final String password,
      final String idpTokenEndpointUrl,
      final String idpClientId,
      final String idpClientSecret,
      final String idpScope,
      final String idpScopeClaimName,
      final String idpSubClaimName,
      final Short idpCacheExpiryBufferSeconds,
      final String sslTrustStoreLocation,
      final String sslTrustStorePassword,
      final String sslKeystoreLocation,
      final String sslKeystorePassword,
      final String sslKeyPassword,
      final String sslKeyAlias,
      final boolean useAlpn,
      final boolean verifyHost,
      final Map<String, String> requestHeaders
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

    if (!(username == null || username.isEmpty())
        || !(password == null || password.isEmpty())) {
      options.setBasicAuthCredentials(username, password);
    }

    // If basic auth hasn't been configured
    if (options.getAuthType() != AuthType.BASIC) {
      final IdpConfig idpConfig = new IdpConfig.Builder()
          .withTokenEndpointUrl(idpTokenEndpointUrl)
          .withClientId(idpClientId)
          .withClientSecret(idpClientSecret)
          .withScope(idpScope)
          .withScopeClaimName(idpScopeClaimName)
          .withSubClaimName(idpSubClaimName)
          .withCacheExpiryBufferSeconds(idpCacheExpiryBufferSeconds)
          .build();
      options.setIdpConfig(idpConfig);
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

    if (requestHeaders != null) {
      options.setRequestHeaders(requestHeaders);
    }

    return options;
  }

  private static Map<String, String> loadRequestHeaders(final String headersFile) {
    if (headersFile == null || headersFile.trim().isEmpty()) {
      return null;
    }

    try {
      return PropertiesUtil.loadProperties(new File(headersFile.trim()));
    } catch (KsqlException e) {
      throw new MigrationException("Could not parse headers file '" + headersFile + "'.");
    }
  }
}
