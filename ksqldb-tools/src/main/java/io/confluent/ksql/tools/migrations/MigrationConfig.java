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

package io.confluent.ksql.tools.migrations;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.tools.migrations.util.ServerVersionUtil;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.SaslConfigs;

public final class MigrationConfig extends AbstractConfig {

  public static final String KSQL_SERVER_URL = "ksql.server.url";

  public static final String KSQL_BASIC_AUTH_USERNAME = "ksql.auth.basic.username";
  public static final String KSQL_BASIC_AUTH_PASSWORD = "ksql.auth.basic.password";

  //OAuth AUTHORIZATION SERVER related configs
  public static final String BEARER_AUTH_ISSUER_ENDPOINT_URL = "bearer.auth.issuer.endpoint.url";
  public static final String BEARER_AUTH_CLIENT_ID = "bearer.auth.client.id";
  public static final String BEARER_AUTH_CLIENT_SECRET = "bearer.auth.client.secret";
  public static final String BEARER_AUTH_SCOPE = "bearer.auth.scope";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME = "bearer.auth.scope.claim.name";
  public static final String BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
  public static final String BEARER_AUTH_SUB_CLAIM_NAME = "bearer.auth.sub.claim.name";
  public static final String BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT =
      SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;

  //OAuth config related to token cache
  public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS =
      "bearer.auth.cache.expiry.buffer.seconds";
  public static final short BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT = 300;

  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_KEY_PASSWORD = "ssl.key.password";
  public static final String SSL_KEY_ALIAS = "ssl.key.alias";
  public static final String SSL_ALPN = "ssl.alpn";
  public static final String SSL_VERIFY_HOST = "ssl.verify.host";

  public static final String KSQL_MIGRATIONS_STREAM_NAME = "ksql.migrations.stream.name";
  public static final String KSQL_MIGRATIONS_STREAM_NAME_DEFAULT = "MIGRATION_EVENTS";
  public static final String KSQL_MIGRATIONS_TABLE_NAME = "ksql.migrations.table.name";
  public static final String KSQL_MIGRATIONS_TABLE_NAME_DEFAULT = "MIGRATION_SCHEMA_VERSIONS";
  public static final String KSQL_MIGRATIONS_STREAM_TOPIC_NAME =
      "ksql.migrations.stream.topic.name";
  public static final String KSQL_MIGRATIONS_TABLE_TOPIC_NAME = "ksql.migrations.table.topic.name";
  public static final String KSQL_MIGRATIONS_TOPIC_REPLICAS = "ksql.migrations.topic.replicas";
  public static final int KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT = 1;

  public static final String KSQL_MIGRATIONS_DIR_OVERRIDE = "ksql.migrations.dir.override";

  public static final MigrationConfig DEFAULT_CONFIG =
      new MigrationConfig(Collections.emptyMap(), "ksql-service-id");

  public static MigrationConfig load(final String configFile) {
    final Map<String, String> configsMap =
        PropertiesUtil.loadProperties(new File(configFile));
    return new MigrationConfig(configsMap, getServiceId(configsMap));
  }

  @SuppressWarnings(value = "MethodLength")
  private MigrationConfig(final Map<String, String> configs, final String id) {
    super(new ConfigDef()
        .define(
            KSQL_SERVER_URL,
            Type.STRING,
            "",
            Importance.HIGH,
            "The URL for the KSQL server"
        ).define(
            KSQL_BASIC_AUTH_USERNAME,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The username for the KSQL server"
        ).define(
            KSQL_BASIC_AUTH_PASSWORD,
            Type.PASSWORD,
            "",
            Importance.MEDIUM,
            "The password for the KSQL server"
        ).define(
            BEARER_AUTH_ISSUER_ENDPOINT_URL,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The issuer endpoint URL for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CLIENT_ID,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The client ID for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CLIENT_SECRET,
            Type.PASSWORD,
            "",
            Importance.MEDIUM,
            "The client secret for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SCOPE,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The scope for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SCOPE_CLAIM_NAME,
            Type.STRING,
            BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT,
            Importance.MEDIUM,
            "The scope claim name for the IDP Authorization server"
        ).define(
            BEARER_AUTH_SUB_CLAIM_NAME,
            Type.STRING,
            BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT,
            Importance.MEDIUM,
            "The sub claim name for the IDP Authorization server"
        ).define(
            BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,
            Type.SHORT,
            BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "The expiry buffer for token cache"
        ).define(
            SSL_TRUSTSTORE_LOCATION,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The trust store path"
        ).define(
            SSL_TRUSTSTORE_PASSWORD,
            Type.PASSWORD,
            "",
            Importance.MEDIUM,
            "The trust store password"
        ).define(
            SSL_KEYSTORE_LOCATION,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The key store path"
        ).define(
            SSL_KEYSTORE_PASSWORD,
            Type.PASSWORD,
            "",
            Importance.MEDIUM,
            "The key store password"
        ).define(
            SSL_KEY_PASSWORD,
            Type.PASSWORD,
            "",
            Importance.MEDIUM,
            "The key password"
        ).define(
            SSL_KEY_ALIAS,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "The key alias"
        ).define(
            SSL_ALPN,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            "Whether ALPN should be used. It defaults to false."
        ).define(
            SSL_VERIFY_HOST,
            Type.BOOLEAN,
            true,
            Importance.MEDIUM,
            "Whether hostname verification is enabled. It defaults to true."
        ).define(
            KSQL_MIGRATIONS_STREAM_NAME,
            Type.STRING,
            KSQL_MIGRATIONS_STREAM_NAME_DEFAULT,
            Importance.MEDIUM,
            "The name of the migration stream. It defaults to "
                + KSQL_MIGRATIONS_STREAM_NAME_DEFAULT
        ).define(
            KSQL_MIGRATIONS_TABLE_NAME,
            Type.STRING,
            KSQL_MIGRATIONS_TABLE_NAME_DEFAULT,
            Importance.MEDIUM,
            "The name of the migration table. It defaults to "
                + KSQL_MIGRATIONS_TABLE_NAME_DEFAULT
        ).define(
            KSQL_MIGRATIONS_STREAM_TOPIC_NAME,
            Type.STRING,
            id + "ksql_" + configs
                .getOrDefault(KSQL_MIGRATIONS_STREAM_NAME, KSQL_MIGRATIONS_STREAM_NAME_DEFAULT),
            Importance.MEDIUM,
            "The name of the migration stream topic. It defaults to "
                + "'<ksql_service_id>ksql_<migrations_stream_name>'"
        ).define(
            KSQL_MIGRATIONS_TABLE_TOPIC_NAME,
            Type.STRING,
            id + "ksql_" + configs
                .getOrDefault(KSQL_MIGRATIONS_TABLE_NAME, KSQL_MIGRATIONS_TABLE_NAME_DEFAULT),
            Importance.MEDIUM,
            "The name of the migration table topic. It defaults to "
                + "'<ksql_service_id>ksql_<migrations_table_name>'"
        ).define(
            KSQL_MIGRATIONS_TOPIC_REPLICAS,
            Type.INT,
            KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT,
            Importance.MEDIUM,
            "The number of replicas for the migration stream topic. It defaults to "
                + KSQL_MIGRATIONS_TOPIC_REPLICAS_DEFAULT
        ).define(
            KSQL_MIGRATIONS_DIR_OVERRIDE,
            Type.STRING,
            "",
            Importance.MEDIUM,
            "An optional config that allows users to specify the path to the directory "
                + "containing migrations files to be applied. If empty, the migrations directory "
                + "will be inferred as relative to the migrations configuration file "
                + "passed when using the ksql-migrations tool. Specifically, the migrations "
                + "directory will be inferred as a directory with name 'migrations' contained in "
                + "the same directory as the migrations configuration file. This is the "
                + "default file structure created by the 'ksql-migrations new-project' command."
        ), configs, false);
  }

  private static Short getBearerAuthCacheExpiryBufferSeconds(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        ? (Short) configs.get(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS)
        : BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DEFAULT;
  }

  private static String getBearerAuthScopeClaimName(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SCOPE_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SCOPE_CLAIM_NAME)
        : BEARER_AUTH_SCOPE_CLAIM_NAME_DEFAULT;
  }

  private static String getBearerAuthSubClaimName(final Map<String, ?> configs) {
    return configs != null && configs.containsKey(BEARER_AUTH_SUB_CLAIM_NAME)
        ? (String) configs.get(BEARER_AUTH_SUB_CLAIM_NAME)
        : BEARER_AUTH_SUB_CLAIM_NAME_DEFAULT;
  }

  private static String getServiceId(final Map<String, String> configs) throws MigrationException {
    final String ksqlServerUrl = configs.get(KSQL_SERVER_URL);
    if (ksqlServerUrl == null) {
      throw new MigrationException("Missing required property: " + MigrationConfig.KSQL_SERVER_URL);
    }

    final Client ksqlClient = MigrationsUtil.getKsqlClient(
        ksqlServerUrl,
        configs.get(KSQL_BASIC_AUTH_USERNAME),
        configs.get(KSQL_BASIC_AUTH_PASSWORD),
        configs.get(BEARER_AUTH_ISSUER_ENDPOINT_URL),
        configs.get(BEARER_AUTH_CLIENT_ID),
        configs.get(BEARER_AUTH_CLIENT_SECRET),
        configs.get(BEARER_AUTH_SCOPE),
        getBearerAuthScopeClaimName(configs),
        getBearerAuthSubClaimName(configs),
        getBearerAuthCacheExpiryBufferSeconds(configs),
        configs.get(SSL_TRUSTSTORE_LOCATION),
        configs.get(SSL_TRUSTSTORE_PASSWORD),
        configs.get(SSL_KEYSTORE_LOCATION),
        configs.get(SSL_KEYSTORE_PASSWORD),
        configs.get(SSL_KEY_PASSWORD),
        configs.get(SSL_KEY_ALIAS),
        configs.getOrDefault(SSL_ALPN, "false").equalsIgnoreCase("true"),
        configs.getOrDefault(SSL_VERIFY_HOST, "true").equalsIgnoreCase("true"),
        null
    );
    final String serviceId;
    try {
      serviceId = ServerVersionUtil.getServerInfo(ksqlClient, ksqlServerUrl).getKsqlServiceId();
      ksqlClient.close();
      return serviceId;
    } catch (MigrationException e) {
      ksqlClient.close();
      throw e;
    }
  }
}
