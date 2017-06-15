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

  public int defaultSinkNumberOfPartitions = 10;
  public short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public String defaultAutoOffsetRestConfig = "latest";
  public long defaultCommitIntervalMsConfig = 2000;
  public long defaultCacheMaxBytesBufferingConfig = 0;
  public int defaultNumberOfStreamsThreads = 4;

  Map<String, Object> ksqlConfigProps;


  private static final ConfigDef CONFIG_DEF = new ConfigDef(StreamsConfig.configDef());

  public KsqlConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);

    ksqlConfigProps = new HashMap<>();
    ksqlConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, defaultAutoOffsetRestConfig);
    ksqlConfigProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, defaultCommitIntervalMsConfig);
    ksqlConfigProps.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, defaultCacheMaxBytesBufferingConfig);
    ksqlConfigProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, defaultNumberOfStreamsThreads);

    ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS, defaultSinkNumberOfPartitions);
    ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS, defaultSinkNumberOfReplications);
    ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION,
                        defaultSinkWindowChangeLogAdditionalRetention);
    for (Object propKey: props.keySet()) {
      ksqlConfigProps.put(propKey.toString(), props.get(propKey));
    }

  }

  protected KsqlConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }

  public Map<String, Object> getResetStreamsProperties(String applicationId) {
    Map<String, Object> result = originals();
    result.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        applicationId
    );
    result.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        defaultCommitIntervalMsConfig
    );
    result.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        defaultCacheMaxBytesBufferingConfig
    );
    result.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, defaultNumberOfStreamsThreads);


    return result;
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
