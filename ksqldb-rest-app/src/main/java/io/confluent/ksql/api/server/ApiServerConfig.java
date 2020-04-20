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

import static io.confluent.ksql.configdef.ConfigValidators.oneOrMore;
import static io.confluent.ksql.configdef.ConfigValidators.zeroOrPositive;

import io.confluent.ksql.configdef.ConfigValidators;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.http.ClientAuth;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Config for the API server
 */
public class ApiServerConfig extends AbstractConfig {

  private static final String PROPERTY_PREFIX = "apiserver.";

  public static final String VERTICLE_INSTANCES = propertyName("verticle.instances");
  public static final int DEFAULT_VERTICLE_INSTANCES =
      2 * Runtime.getRuntime().availableProcessors();
  public static final String VERTICLE_INSTANCES_DOC =
      "The number of server verticle instances to start per listener. Usually you want at least "
          + "many instances as there are cores you want to use, as each instance is single "
          + "threaded.";

  public static final String LISTENERS = propertyName("listeners");
  public static final String DEFAULT_LISTENERS = "http://0.0.0.0:8088";
  public static final String LISTENERS_DOC = "List of listeners to listen for connections on";

  public static final String TLS_KEY_STORE_PATH = propertyName("tls.keystore.path");
  public static final String TLS_KEY_STORE_PATH_DOC =
      "Path to server key store";

  public static final String TLS_KEY_STORE_PASSWORD = propertyName("tls.keystore.password");
  public static final String TLS_KEY_STORE_PASSWORD_DOC =
      "Password for server key store";

  public static final String TLS_TRUST_STORE_PATH = propertyName("tls.truststore.path");
  public static final String TLS_TRUST_STORE_PATH_DOC =
      "Path to client trust store";

  public static final String TLS_TRUST_STORE_PASSWORD = propertyName("tls.truststore.password");
  public static final String TLS_TRUST_STORE_PASSWORD_DOC =
      "Password for client trust store";

  public static final String TLS_CLIENT_AUTH_REQUIRED = propertyName("tls.client.auth.required");
  public static final String DEFAULT_TLS_CLIENT_AUTH_REQUIRED = "none";
  public static final String TLS_CLIENT_AUTH_REQUIRED_DOC =
      "Is client auth required? One of none, request or required";

  public static final String AUTHENTICATION_METHOD_CONFIG = propertyName("authentication.method");
  public static final String AUTHENTICATION_METHOD_NONE = "NONE";
  public static final String AUTHENTICATION_METHOD_BASIC = "BASIC";
  public static final String AUTHENTICATION_METHOD_DOC =
      "Method of authentication. Acceptable values are BASIC or NONE. "
          + "For BASIC authentication, you must supply a valid JAAS config file for the "
          + "'java.security.auth.login.config' system property for the appropriate authentication "
          + "provider.";
  public static final ConfigDef.ValidString AUTHENTICATION_METHOD_VALIDATOR =
      ConfigDef.ValidString.in(
          AUTHENTICATION_METHOD_NONE,
          AUTHENTICATION_METHOD_BASIC
      );
  public static final String AUTHENTICATION_REALM_CONFIG = propertyName("authentication.realm");
  public static final String AUTHENTICATION_REALM_DOC =
      "Security realm to be used in authentication.";
  public static final String AUTHENTICATION_ROLES_CONFIG = propertyName("authentication.roles");
  public static final String AUTHENTICATION_ROLES_DOC =
      "Comma-delimited list of JAAS roles authorized to access this cluster. "
          + "Defaults to an empty list meaning no roles will be allowed. "
          + "Specify '*' to indicate all roles should be allowed.";
  public static final List<String> AUTHENTICATION_ROLES_DEFAULT =
      Collections.unmodifiableList(Collections.emptyList());
  public static final String AUTHENTICATION_SKIP_PATHS_CONFIG = propertyName(
      "authentication.skip.paths");
  public static final String AUTHENTICATION_SKIP_PATHS_DOC =
      "Comma separated list of paths where authentication is not required. "
          + "Wildcards are supported.";
  public static final List<String> AUTHENTICATION_SKIP_PATHS_DEFAULT =
      Collections.unmodifiableList(Collections.emptyList());

  public static final String WORKER_POOL_SIZE = propertyName("worker.pool.size");
  public static final String WORKER_POOL_DOC =
      "Max number of worker threads for executing blocking code";
  public static final int DEFAULT_WORKER_POOL_SIZE = 100;

  public static final String MAX_PUSH_QUERIES = propertyName("max.push.queries");
  public static final int DEFAULT_MAX_PUSH_QUERIES = 100;
  public static final String MAX_PUSH_QUERIES_DOC =
      "The maximum number of push queries allowed on the server at any one time";

  public static final String CORS_ALLOWED_ORIGINS = propertyName("cors.allowed.origins");
  public static final String CORS_ALLOWED_ORIGINS_DOC =
      "CORS support: Comma separated list of allowed origins, can contain wildcards";

  public static final String CORS_ALLOWED_HEADERS = propertyName("cors.allowed.headers");
  public static final String CORS_ALLOWED_HEADERS_DOC =
      "CORS support: Comma separated list of allowed headers";

  public static final String CORS_ALLOWED_METHODS = propertyName("cors.allowed.methods");
  public static final String CORS_ALLOWED_METHODS_DOC =
      "CORS support: Comma separated list of allowed methods";

  private static String propertyName(final String name) {
    return KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + PROPERTY_PREFIX + name;
  }

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          VERTICLE_INSTANCES,
          Type.INT,
          DEFAULT_VERTICLE_INSTANCES,
          oneOrMore(),
          Importance.MEDIUM,
          VERTICLE_INSTANCES_DOC)
      .define(
          LISTENERS,
          Type.LIST,
          DEFAULT_LISTENERS,
          Importance.MEDIUM,
          LISTENERS_DOC)
      .define(
          TLS_KEY_STORE_PATH,
          Type.STRING,
          "",
          Importance.MEDIUM,
          TLS_KEY_STORE_PATH_DOC)
      .define(
          TLS_KEY_STORE_PASSWORD,
          Type.STRING,
          "",
          Importance.MEDIUM,
          TLS_KEY_STORE_PASSWORD_DOC)
      .define(
          TLS_TRUST_STORE_PATH,
          Type.STRING,
          "",
          Importance.MEDIUM,
          TLS_TRUST_STORE_PATH_DOC)
      .define(
          TLS_TRUST_STORE_PASSWORD,
          Type.STRING,
          "",
          Importance.MEDIUM,
          TLS_TRUST_STORE_PASSWORD_DOC)
      .define(
          TLS_CLIENT_AUTH_REQUIRED,
          Type.STRING,
          DEFAULT_TLS_CLIENT_AUTH_REQUIRED,
          ConfigValidators.enumValues(ClientAuth.class),
          Importance.MEDIUM,
          TLS_CLIENT_AUTH_REQUIRED_DOC)
      .define(
          AUTHENTICATION_METHOD_CONFIG,
          Type.STRING,
          AUTHENTICATION_METHOD_NONE,
          AUTHENTICATION_METHOD_VALIDATOR,
          Importance.LOW,
          AUTHENTICATION_METHOD_DOC)
      .define(
          AUTHENTICATION_REALM_CONFIG,
          Type.STRING,
          "",
          Importance.LOW,
          AUTHENTICATION_REALM_DOC)
      .define(
          AUTHENTICATION_ROLES_CONFIG,
          Type.LIST,
          AUTHENTICATION_ROLES_DEFAULT,
          Importance.LOW,
          AUTHENTICATION_ROLES_DOC)
      .define(
          AUTHENTICATION_SKIP_PATHS_CONFIG,
          Type.LIST,
          AUTHENTICATION_SKIP_PATHS_DEFAULT,
          Importance.LOW,
          AUTHENTICATION_SKIP_PATHS_DOC)
      .define(
          WORKER_POOL_SIZE,
          Type.INT,
          DEFAULT_WORKER_POOL_SIZE,
          zeroOrPositive(),
          Importance.MEDIUM,
          WORKER_POOL_DOC)
      .define(
          MAX_PUSH_QUERIES,
          Type.INT,
          DEFAULT_MAX_PUSH_QUERIES,
          zeroOrPositive(),
          Importance.MEDIUM,
          MAX_PUSH_QUERIES_DOC)
      .define(
          CORS_ALLOWED_ORIGINS,
          Type.STRING,
          "",
          Importance.MEDIUM,
          CORS_ALLOWED_ORIGINS_DOC
      )
      .define(
          CORS_ALLOWED_HEADERS,
          Type.LIST,
          "",
          Importance.MEDIUM,
          CORS_ALLOWED_HEADERS_DOC
      )
      .define(
          CORS_ALLOWED_METHODS,
          Type.LIST,
          "",
          Importance.MEDIUM,
          CORS_ALLOWED_METHODS_DOC
      );

  public ApiServerConfig(final Map<?, ?> map) {
    super(CONFIG_DEF, map);
  }

  // Note that this expects config value that is not standard Confluent as defined in BaseConfig.
  // For "request" client auth, confluent value is "REQUESTED" where value expected here is
  // "REQUEST"
  public ClientAuth getClientAuth() {
    return ClientAuth.valueOf(getString(TLS_CLIENT_AUTH_REQUIRED).toUpperCase());
  }
}
