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

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.json.JsonObject;
import java.util.Map;

/**
 * Config for the API server
 */
public class ApiServerConfig extends AbstractConfig {

  private static final String PROPERTY_PREFIX = "apiserver.";

  public static final String VERTICLE_INSTANCES = propertyName("verticle-instances");
  public static final int DEFAULT_VERTICLE_INSTANCES =
      2 * Runtime.getRuntime().availableProcessors();
  public static final String VERTICLE_INSTANCES_DOC =
      "The number of server verticle instances to start. Usually you want at least many instances"
          + " as there are cores you want to use, as each instance is single threaded.";

  public static final String LISTEN_HOST = propertyName("listen-host");
  public static final String DEFAULT_LISTEN_HOST = "0.0.0.0";
  public static final String LISTEN_HOST_DOC =
      "The hostname to listen on";

  public static final String LISTEN_PORT = propertyName("listen-port");
  public static final int DEFAULT_LISTEN_PORT = 8089;
  public static final String LISTEN_PORT_DOC =
      "The port to listen on";

  public static final String KEY_PATH = propertyName("key-path");
  public static final String KEY_PATH_DOC =
      "Path to key file";

  public static final String CERT_PATH = propertyName("cert-path");
  public static final String CERT_PATH_DOC =
      "Path to cert file";

  private static String propertyName(final String name) {
    return KsqlConfig.KSQL_CONFIG_PROPERTY_PREFIX + PROPERTY_PREFIX + name;
  }

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          VERTICLE_INSTANCES,
          Type.INT,
          DEFAULT_VERTICLE_INSTANCES,
          Importance.LOW,
          VERTICLE_INSTANCES_DOC)
      .define(
          LISTEN_HOST,
          Type.STRING,
          DEFAULT_LISTEN_HOST,
          Importance.MEDIUM,
          LISTEN_HOST_DOC)
      .define(
          LISTEN_PORT,
          Type.INT,
          DEFAULT_LISTEN_PORT,
          Importance.MEDIUM,
          LISTEN_PORT_DOC)
      .define(
          KEY_PATH,
          Type.STRING,
          null,
          Importance.MEDIUM,
          KEY_PATH_DOC)
      .define(
          CERT_PATH,
          Type.STRING,
          null,
          Importance.MEDIUM,
          CERT_PATH_DOC);

  public ApiServerConfig(final Map<?, ?> map) {
    super(CONFIG_DEF, map);
  }

  public ApiServerConfig(final JsonObject jsonObject) {
    super(CONFIG_DEF, jsonObject.getMap());
  }

  public JsonObject toJsonObject() {
    return new JsonObject(originalsWithPrefix(""));
  }

}
