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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;

import java.util.Map;

// Although it would be nice to somehow extend the functionality of this class to encompass that of
// the KsqlConfig, there is no clean way to do so since the KsqlConfig inherits from the Kafka
// AbstractConfig class, and the RestConfig inherits from the Confluent AbstractConfig class. Making
// the two get along and play nicely together in one class is more work than it's worth, so any and
// all validation to be performed by the KsqlConfig class will be handled outside of this one.
public class KsqlRestConfig extends RestConfig {

  public static final String KSQL_STREAMS_PREFIX       = "ksql.core.streams.";
  public static final String COMMAND_CONSUMER_PREFIX  = "ksql.command.consumer.";
  public static final String COMMAND_PRODUCER_PREFIX  = "ksql.command.producer.";

  public static final String
      COMMAND_TOPIC_SUFFIX_CONFIG = "ksql.command.topic.suffix";
  public static final ConfigDef.Type
      COMMAND_TOPIC_SUFFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      COMMAND_TOPIC_SUFFIX_DEFAULT = "commands";
  public static final ConfigDef.Importance
      COMMAND_TOPIC_SUFFIX_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
      COMMAND_TOPIC_SUFFIX_DOC =
          "A suffix to append to the end of the name of the Kafka topic to use for distributing "
              + "commands";

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
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG = "command.response.timeout.ms";
  public static final ConfigDef.Type
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_TYPE = ConfigDef.Type.LONG;
  public static final Long
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DEFAULT = 1000L;
  public static final ConfigDef.Importance
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
      DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC =
          "How long to wait for a distributed command to be executed by the local node before "
              + "returning a response";

  public static final String
          UI_ENABLED_CONFIG = "ui.enabled";
  public static final ConfigDef.Type
          UI_ENABLED_TYPE = ConfigDef.Type.BOOLEAN;
  public static final String
          UI_ENABLED_DEFAULT = "true";
  public static final ConfigDef.Importance
          UI_ENABLED_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String
          UI_ENABLED_DOC =
          "Flag to disable the KQL UI. It is enabled by default";

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = baseConfigDef().define(
        COMMAND_TOPIC_SUFFIX_CONFIG,
        COMMAND_TOPIC_SUFFIX_TYPE,
        COMMAND_TOPIC_SUFFIX_DEFAULT,
        COMMAND_TOPIC_SUFFIX_IMPORTANCE,
        COMMAND_TOPIC_SUFFIX_DOC
    ).define(
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

  public Map<String, Object> getKsqlStreamsProperties() {
    return getPropertiesWithOverrides(KSQL_STREAMS_PREFIX);
  }

  public String getCommandTopic() {
    return String.format(
        "%s_%s",
        KsqlConfig.KSQL_SERVICE_ID_DEFAULT,
        getString(COMMAND_TOPIC_SUFFIX_CONFIG)
    );
  }

  public boolean isUiEnabled() {
    return getBoolean(UI_ENABLED_CONFIG);
  }
}
