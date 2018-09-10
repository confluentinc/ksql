/*
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
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;
import java.util.Map;

public class KsqlRestConfig extends RestConfig {

  private static final String KSQL_CONFIG_PREFIX = "ksql.";

  private static final String COMMAND_CONSUMER_PREFIX  =
      KSQL_CONFIG_PREFIX + "server.command.consumer.";
  private static final String COMMAND_PRODUCER_PREFIX  =
      KSQL_CONFIG_PREFIX + "server.command.producer.";

  static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG =
      "query.stream.disconnect.check";

  private static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC =
          "How often to send an empty line as part of the response while streaming queries as "
              + "JSON; this helps proactively determine if the connection has been terminated in "
              + "order to avoid keeping the created streams job alive longer than necessary";

  static final String DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG =
      KSQL_CONFIG_PREFIX + "server.command.response.timeout.ms";

  private static final String DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC =
            "How long to wait for a distributed command to be executed by the local node before "
              + "returning a response";

  public static final String INSTALL_DIR_CONFIG = KSQL_CONFIG_PREFIX + "server.install.dir";
  private static final String INSTALL_DIR_DOC
      = "The directory that ksql is installed in. This is set in the ksql-server-start script.";

  static final String COMMAND_TOPIC_SUFFIX = "command_topic";

  static final String KSQL_WEBSOCKETS_NUM_THREADS =
      KSQL_CONFIG_PREFIX + "server.websockets.num.threads";
  private static final String KSQL_WEBSOCKETS_NUM_THREADS_DOC =
      "The number of websocket threads to handle query results";

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = baseConfigDef().define(
        STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG,
        Type.LONG,
        1000L,
        Importance.LOW,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC
    ).define(
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_CONFIG,
        Type.LONG,
        5000L,
        Importance.LOW,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT_MS_DOC
    ).define(
        INSTALL_DIR_CONFIG,
        Type.STRING,
        "",
        Importance.LOW,
        INSTALL_DIR_DOC
    ).define(
        KSQL_WEBSOCKETS_NUM_THREADS,
        Type.INT,
        5,
        Importance.LOW,
        KSQL_WEBSOCKETS_NUM_THREADS_DOC
    );
  }

  public KsqlRestConfig(final Map<?, ?> props) {
    super(CONFIG_DEF, props);
    if (getList(RestConfig.LISTENERS_CONFIG).isEmpty()) {
      throw new KsqlException(RestConfig.LISTENERS_CONFIG + " must be supplied.  "
          + RestConfig.LISTENERS_DOC);
    }
  }

  // Bit of a hack to get around the fact that RestConfig.originals() is private for some reason
  Map<String, Object> getOriginals() {
    return originalsWithPrefix("");
  }

  private Map<String, Object> getPropertiesWithOverrides(final String prefix) {
    final Map<String, Object> result = getOriginals();
    result.putAll(originalsWithPrefix(prefix));
    return result;
  }

  Map<String, Object> getCommandConsumerProperties() {
    return getPropertiesWithOverrides(COMMAND_CONSUMER_PREFIX);
  }

  Map<String, Object> getCommandProducerProperties() {
    return getPropertiesWithOverrides(COMMAND_PRODUCER_PREFIX);
  }

  public Map<String, Object> getKsqlConfigProperties() {
    return getOriginals();
  }

  public static String getCommandTopic(final String ksqlServiceId) {
    return String.format(
        "%s%s_%s",
        KsqlConstants.KSQL_INTERNAL_TOPIC_PREFIX,
        ksqlServiceId,
        COMMAND_TOPIC_SUFFIX
    );
  }
}
