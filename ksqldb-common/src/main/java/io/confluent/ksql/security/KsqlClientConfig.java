/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.security;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;

public final class KsqlClientConfig extends AbstractConfig {
  public static final String KSQL_BASIC_AUTH_USERNAME = "ksql.auth.basic.username";
  public static final String KSQL_BASIC_AUTH_PASSWORD = "ksql.auth.basic.password";

  // Static token related configs
  public static final String BEARER_AUTH_TOKEN_CONFIG = "bearer.auth.token";

  // OAuth AUTHORIZATION SERVER related configs
  public static final String BEARER_AUTH_TOKEN_ENDPOINT_URL = "bearer.auth.issuer.endpoint.url";
  public static final String BEARER_AUTH_CLIENT_ID = "bearer.auth.client.id";
  public static final String BEARER_AUTH_CLIENT_SECRET = "bearer.auth.client.secret";
  public static final String BEARER_AUTH_SCOPE = "bearer.auth.scope";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME = "bearer.auth.scope.claim.name";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
  public static final String BEARER_AUTH_SUB_CLAIM_NAME = "bearer.auth.sub.claim.name";
  public static final String BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;

  // OAuth config related to token cache
  public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS =
      "bearer.auth.cache.expiry.buffer.seconds";
  public static final short BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT = 300;

  // SSL related configs
  public static final String SSL_PREFIX = "ssl.";

  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_KEY_PASSWORD = "ssl.key.password";
  public static final String SSL_KEY_ALIAS = "ssl.key.alias";
  public static final String SSL_ALPN = "ssl.alpn";
  public static final String SSL_VERIFY_HOST = "ssl.verify.host";

  private KsqlClientConfig(final Map<String, String> configs) {
    super(new ConfigDef().define(
            KSQL_BASIC_AUTH_USERNAME,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The username for the KSQL server"
        ).define(
            KSQL_BASIC_AUTH_PASSWORD,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The password for the KSQL server"
        ).define(
            BEARER_AUTH_TOKEN_CONFIG,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The static bearer token for the IDP Authorization server"
        ).define(
        BEARER_AUTH_TOKEN_ENDPOINT_URL,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The issuer endpoint URL for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CLIENT_ID,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The client ID for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CLIENT_SECRET,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The client secret for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SCOPE,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The scope for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SCOPE_CLAIM_NAME,
            ConfigDef.Type.STRING,
            BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "The scope claim name for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SUB_CLAIM_NAME,
            ConfigDef.Type.STRING,
            BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "The sub claim name for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,
            ConfigDef.Type.SHORT,
            BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "The expiry buffer for token cache"
        ).define(
            SSL_TRUSTSTORE_LOCATION,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The trust store path"
        ).define(
            SSL_TRUSTSTORE_PASSWORD,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The trust store password"
        ).define(
            SSL_KEYSTORE_LOCATION,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The key store path"
        ).define(
            SSL_KEYSTORE_PASSWORD,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The key store password"
        ).define(
            SSL_KEY_PASSWORD,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.MEDIUM,
            "The key password"
        ).define(
            SSL_KEY_ALIAS,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            "The key alias"
        ).define(
            SSL_ALPN,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            "Whether ALPN should be used. It defaults to false."
        ).define(
            SSL_VERIFY_HOST,
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "Whether hostname verification is enabled. It defaults to true."
        ), configs, false);
  }

  public static String getBearerAuthScopeClaimName(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SCOPE_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SCOPE_CLAIM_NAME)
        : BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT;
  }

  public static String getBearerAuthSubClaimName(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SUB_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SUB_CLAIM_NAME)
        : BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT;
  }

  public static short getBearerAuthCacheExpiryBufferSeconds(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        ? (Short) configs.get(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        : BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT;
  }

  public static Map<String, Object> getClientSslConfig(final Map<String, ?> configs) {
    return configs.entrySet().stream()
        .filter(e -> e.getKey().startsWith(SSL_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
