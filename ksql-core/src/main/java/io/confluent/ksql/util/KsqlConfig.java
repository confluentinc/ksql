/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;

public class KsqlConfig extends AbstractConfig {

  public static final String KSQL_TIMESTAMP_COLUMN_INDEX = "ksq.timestamp.column.index";
  public static final String SINK_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";

  public static final String SINK_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String DEFAULT_SINK_NUMBER_OF_PARTITIONS = "ksql.sink.partitions.default";
  public static final String SINK_NUMBER_OF_REPLICATIONS = "REPLICATIONS";
  public static final String DEFAULT_SINK_NUMBER_OF_REPLICATIONS = "ksql.sink.replications.default";
  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION";
  public static final String DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "ksql.sink.window.change.log.additional.retention.default";


  public static final String
      KSQL_SERVICE_ID_CONFIG = "ksql.service.id";
  public static final ConfigDef.Type
      KSQL_SERVICE_ID_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_SERVICE_ID_DEFAULT = "ksql_";
  public static final ConfigDef.Importance
      KSQL_SERVICE_ID_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_SERVICE_ID_DOC =
      "Indicates the ID of the ksql service. It will be used as prefix for all KSQL queires in "
      + "this service.";

  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG = "ksql.persistent.prefix";
  public static final ConfigDef.Type
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT = "query_";
  public static final ConfigDef.Importance
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DOC =
      "Second part of the prefix for persitent queries.";

  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG = "ksql.transient.prefix";
  public static final ConfigDef.Type
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT = "transient_";
  public static final ConfigDef.Importance
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DOC =
      "Second part of the prefix for transient queries.";


  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG = "ksql.statestore.suffix";
  public static final ConfigDef.Type
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT = "transient_";
  public static final ConfigDef.Importance
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_DOC =
      "Suffix for state store names in Tables.";

  public int defaultSinkNumberOfPartitions = 4;
  public short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public String defaultAutoOffsetRestConfig = "latest";
  public long defaultCommitIntervalMsConfig = 2000;
  public long defaultCacheMaxBytesBufferingConfig = 10000000;
  public int defaultNumberOfStreamsThreads = 4;

  Map<String, Object> ksqlConfigProps;

  private static final ConfigDef CONFIG_DEF = new ConfigDef(StreamsConfig.configDef());

  public KsqlConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);

    ksqlConfigProps = new HashMap<>();
    ksqlConfigProps.put(KSQL_SERVICE_ID_CONFIG, KSQL_SERVICE_ID_DEFAULT);
    ksqlConfigProps.put(KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT);
    ksqlConfigProps.put(KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG, KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT);
    ksqlConfigProps.put(KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG, KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT);

    if (props.containsKey(DEFAULT_SINK_NUMBER_OF_PARTITIONS)) {
      ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS, props.get(DEFAULT_SINK_NUMBER_OF_PARTITIONS));
    } else {
      ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS, defaultSinkNumberOfPartitions);
    }

    if (props.containsKey(DEFAULT_SINK_NUMBER_OF_REPLICATIONS)) {
      ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS, props.get(DEFAULT_SINK_NUMBER_OF_REPLICATIONS));
    } else {
      ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS, defaultSinkNumberOfReplications);
    }

    if (props.containsKey(DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION)) {
      ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION,
                          props.get(DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION));
    } else {
      ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION,
                          defaultSinkWindowChangeLogAdditionalRetention);
    }

    ksqlConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, defaultAutoOffsetRestConfig);
    ksqlConfigProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, defaultCommitIntervalMsConfig);
    ksqlConfigProps.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, defaultCacheMaxBytesBufferingConfig);
    ksqlConfigProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, defaultNumberOfStreamsThreads);




    for (Object propKey: props.keySet()) {
      ksqlConfigProps.put(propKey.toString(), props.get(propKey));
    }

  }

  protected KsqlConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }

  public Map<String, Object> getStreamsProperties() {
    return (Map<String, Object>) new StreamsConfig(originals()).values();
  }

  public Map<String, Object> getKsqlConfigProps() {
    return ksqlConfigProps;
  }

  public Object get(String propertyName) {
    return ksqlConfigProps.get(propertyName);
  }

  public void put(String propertyName, Object propertyValue) {
    ksqlConfigProps.put(propertyName, propertyValue);
  }

  public KsqlConfig clone() {
    return new KsqlConfig(this.ksqlConfigProps);
  }
}
