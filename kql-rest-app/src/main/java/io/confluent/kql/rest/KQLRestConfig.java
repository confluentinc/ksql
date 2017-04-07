/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest;

import io.confluent.common.config.ConfigDef;
import io.confluent.rest.RestConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

// Although it would be nice to somehow extend the functionality of this class to encompass that of the KQLConfig, there
// is no clean way to do so since the KQLConfig inherits from the Kafka AbstractConfig class, and the RestConfig
// inherits from the Confluent AbstractConfig class. Making the two get along and play nicely together in one class is
// more work than it's worth, so any and all validation to be performed by the KQLConfig class will be handled outside
// of this one.
public class KQLRestConfig extends RestConfig {

  public static final String KQL_STREAMS_PREFIX            = "kql.core.streams.";
  public static final String COMMAND_CONSUMER_PREFIX       = "kql.command.consumer.";
  public static final String COMMAND_PRODUCER_PREFIX       = "kql.command.producer.";

  public static final String NODE_ID_CONFIG = "node.id";
  public static final ConfigDef.Type NODE_ID_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance NODE_ID_IMPORTANCE = ConfigDef.Importance.HIGH;
  public static final String NODE_ID_DOC =
      "A (case-insensitive) unique identifier for the node to add to the cluster";

  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  public static final ConfigDef.Type ZOOKEEPER_CONNECT_TYPE = ConfigDef.Type.STRING;
  public static final ConfigDef.Importance ZOOKEEPER_CONNECT_IMPORTANCE = ConfigDef.Importance.HIGH;
  public static final String ZOOKEEPER_CONNECT_DOC =
      "The Zookeeper connection to use when verifying topics exist";

  public static final String COMMAND_TOPIC_CONFIG = "command.topic";
  public static final ConfigDef.Type COMMAND_TOPIC_TYPE = ConfigDef.Type.STRING;
  public static final String COMMAND_TOPIC_DEFAULT = "kql_commands";
  public static final ConfigDef.Importance COMMAND_TOPIC_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String COMMAND_TOPIC_DOC =
      "The name of the Kafka topic to use for reading/writing commands for persistent queries";

  public static final String COMMAND_POLL_TIMEOUT_CONFIG = "command.poll.ms";
  public static final ConfigDef.Type COMMAND_POLL_TIMEOUT_TYPE = ConfigDef.Type.LONG;
  public static final Long COMMAND_POLL_TIMEOUT_DEFAULT = 1000L;
  public static final ConfigDef.Importance COMMAND_POLL_TIMEOUT_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String COMMAND_POLL_TIMEOUT_DOC =
      "The timeout to use when polling for new writes to the command topic";

  public static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG = "query.stream.disconnect.check";
  public static final ConfigDef.Type STREAMED_QUERY_DISCONNECT_CHECK_MS_TYPE = ConfigDef.Type.LONG;
  public static final Long STREAMED_QUERY_DISCONNECT_CHECK_MS_DEFAULT = 1000L;
  public static final ConfigDef.Importance STREAMED_QUERY_DISCONNECT_CHECK_MS_IMPORTANCE = ConfigDef.Importance.LOW;
  public static final String STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC =
      "How often to send an empty line as part of the response while streaming queries as JSON; this is necessary in"
      + " order to check whether or not the user has terminated the connection";

  private static final String APPLICATION_ID_CONFIG = StreamsConfig.APPLICATION_ID_CONFIG;
  private static final ConfigDef.Type APPLICATION_ID_TYPE = ConfigDef.Type.STRING;
  private static final String APPLICATION_ID_DEFAULT = "";
  private static final ConfigDef.Importance APPLICATION_ID_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String APPLICATION_ID_DOC =
      "Ugly hack to be able to access the application ID property for the StreamsConfig class via getString()";

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF = baseConfigDef().define(
        NODE_ID_CONFIG,
        NODE_ID_TYPE,
        NODE_ID_IMPORTANCE,
        NODE_ID_DOC
    ).define(
        ZOOKEEPER_CONNECT_CONFIG,
        ZOOKEEPER_CONNECT_TYPE,
        ZOOKEEPER_CONNECT_IMPORTANCE,
        ZOOKEEPER_CONNECT_DOC
    ).define(
        COMMAND_TOPIC_CONFIG,
        COMMAND_TOPIC_TYPE,
        COMMAND_TOPIC_DEFAULT,
        COMMAND_TOPIC_IMPORTANCE,
        COMMAND_TOPIC_DOC
    ).define(
        COMMAND_POLL_TIMEOUT_CONFIG,
        COMMAND_POLL_TIMEOUT_TYPE,
        COMMAND_POLL_TIMEOUT_DEFAULT,
        COMMAND_POLL_TIMEOUT_IMPORTANCE,
        COMMAND_POLL_TIMEOUT_DOC
    ).define(
        STREAMED_QUERY_DISCONNECT_CHECK_MS_CONFIG,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_TYPE,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_DEFAULT,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_IMPORTANCE,
        STREAMED_QUERY_DISCONNECT_CHECK_MS_DOC
    ).define(
        APPLICATION_ID_CONFIG,
        APPLICATION_ID_TYPE,
        APPLICATION_ID_DEFAULT,
        APPLICATION_ID_IMPORTANCE,
        APPLICATION_ID_DOC
    );
  }

  public KQLRestConfig(Map<?, ?> props) {
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

  private String getInternalApplicationId(String suffix) {
    return String.format("%s_%s_%s", getString(APPLICATION_ID_CONFIG), getString(NODE_ID_CONFIG), suffix);
  }

  public Map<String, Object> getCommandConsumerProperties() {
    Map<String, Object> result = getPropertiesWithOverrides(COMMAND_CONSUMER_PREFIX);
    result.put(ConsumerConfig.GROUP_ID_CONFIG, getInternalApplicationId("command_consumer"));
    result.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return result;
  }

  public Map<String, Object> getCommandProducerProperties() {
    return getPropertiesWithOverrides(COMMAND_PRODUCER_PREFIX);
  }

  public Map<String, Object> getKqlStreamsProperties() {
    Map<String, Object> result = getPropertiesWithOverrides(KQL_STREAMS_PREFIX);
    result.put(StreamsConfig.APPLICATION_ID_CONFIG, getInternalApplicationId("kql_stream"));
    return result;
  }
}
