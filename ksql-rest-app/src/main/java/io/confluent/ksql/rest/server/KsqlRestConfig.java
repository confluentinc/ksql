/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server;

import io.confluent.common.config.ConfigDef;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;

import java.util.Map;

public class KsqlRestConfig extends RestConfig {

  public static final String COMMAND_CONSUMER_PREFIX  = "ksql.server.command.consumer.";
  public static final String COMMAND_PRODUCER_PREFIX  = "ksql.server.command.producer.";

  public static final String
      STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG = "query.stream.disconnect.check";
  public static final ConfigDef.Type
      STREAMED_QUERY_DISCONNECT_CHECK_MS_TYPE = ConfigDef.Type.LONG;
  public static final Long
      STREAMED_QUERY_DISCONNECT_CHECK_MS_DEFAULT = 1000L;
  public static final ConfigDef.Importance
      STREAMED_QUERY_DISCONNECT_CHECK_MS_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
      STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC =
          "How often to send an empty line as part of the response while streaming queries as "
              + "JSON; this helps proactively determine if the connection has been terminated in "
              + "order to avoid keeping the created streams job alive longer than necessary";

  public static final String
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG = "ksql.server.command.response.timeout.ms";
  public static final ConfigDef.Type
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_TYPE = ConfigDef.Type.LONG;
  public static final Long
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DEFAULT = 5000L;
  public static final ConfigDef.Importance
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC =
          "How long to wait for a distributed command to be executed by the local node before "
              + "returning a response";

  public static final String
          UI_ENABLED_CONFIG = "ksql.server.ui.enabled";
  public static final ConfigDef.Type
          UI_ENABLED_TYPE = ConfigDef.Type.BOOLEAN;
  public static final String
          UI_ENABLED_DEFAULT = "true";
  public static final ConfigDef.Importance
          UI_ENABLED_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
          UI_ENABLED_DOC =
          "Flag to disable the KQL UI. It is enabled by default";
  public static final String INSTALL_DIR_CONFIG = "ksql.server.install.dir";
  public static final String INSTALL_DIR_DOC
      = "The directory that ksql is installed in. This is set in the ksql-server-start script.";

  public static final String COMMAND_TOPIC_SUFFIX = "command_topic";

  public static final String KSQL_WEBSOCKETS_NUM_THREADS = "ksql.server.websockets.num.threads";

  private static final int DEFAULT_WEBSOCKETS_THREADS = 5;


  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = baseConfigDef().define(
        STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_TYPE,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_DEFAULT,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_IMPORTANCE,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC
    ).define(
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_TYPE,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DEFAULT,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_IMPORTANCE,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC
    ).define(
        UI_ENABLED_CONFIG,
        UI_ENABLED_TYPE,
        UI_ENABLED_DEFAULT,
        UI_ENABLED_IMPORTANCE,
        UI_ENABLED_DOC
    ).define(
        INSTALL_DIR_CONFIG,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.LOW,
        INSTALL_DIR_DOC
    ).define(
        KSQL_WEBSOCKETS_NUM_THREADS,
        ConfigDef.Type.INT,
        DEFAULT_WEBSOCKETS_THREADS,
        ConfigDef.Importance.LOW,
        "The number of websocket threads to handle query results"
    );
  }

  public KsqlRestConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    if (getList(RestConfig.LISTENERS_CONFIG).isEmpty()) {
      throw new KsqlException(RestConfig.LISTENERS_CONFIG + " must be supplied.  "
          + RestConfig.LISTENERS_DOC);
    }
  }

  // Bit of a hack to get around the fact that RestConfig.originals() is private for some reason
  public Map<String, Object> getOriginals() {
    return originalsWithPrefix("");
  }

  private Map<String, Object> getPropertiesWithOverrides(String prefix) {
    Map<String, Object> result = getOriginals();
    result.putAll(originalsWithPrefix(prefix));
    return result;
  }

  public Map<String, Object> getCommandConsumerProperties() {
    return getPropertiesWithOverrides(COMMAND_CONSUMER_PREFIX);
  }

  public Map<String, Object> getCommandProducerProperties() {
    return getPropertiesWithOverrides(COMMAND_PRODUCER_PREFIX);
  }

  public Map<String, Object> getKsqlConfigProperties() {
    return getOriginals();
  }

  public String getCommandTopic(String ksqlServiceId) {
    return String.format(
        "%s%s_%s",
        KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlServiceId,
        COMMAND_TOPIC_SUFFIX
    );
  }

  public boolean isUiEnabled() {
    return getBoolean(UI_ENABLED_CONFIG);
  }
}
