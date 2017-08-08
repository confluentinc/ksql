/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server;

import io.confluent.common.config.ConfigDef;
import io.confluent.rest.RestConfig;
import org.apache.kafka.streams.StreamsConfig;

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

  private static final String
      APPLICATION_ID_CONFIG = StreamsConfig.APPLICATION_ID_CONFIG;
  private static final ConfigDef.Type
      APPLICATION_ID_TYPE = ConfigDef.Type.STRING;
  private static final String
      APPLICATION_ID_DEFAULT = "";
  private static final ConfigDef.Importance
      APPLICATION_ID_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String
      APPLICATION_ID_DOC =
          "Ugly hack to be able to access the application ID property for the StreamsConfig class "
              + "via getString()";

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
        APPLICATION_ID_CONFIG,
        APPLICATION_ID_TYPE,
        APPLICATION_ID_DEFAULT,
        APPLICATION_ID_IMPORTANCE,
        APPLICATION_ID_DOC
    );
  }

  public KsqlRestConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
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
        getString(APPLICATION_ID_CONFIG),
        getString(COMMAND_TOPIC_SUFFIX_CONFIG)
    );
  }
}
