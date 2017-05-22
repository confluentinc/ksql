/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;

public class KSQLConfig extends AbstractConfig {

  public static final String SINK_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String DEFAULT_SINK_NUMBER_OF_PARTITIONS = "ksql.sink.partitions.default";
  public static final String SINK_NUMBER_OF_REPLICATIONS = "REPLICATIONS";
  public static final String DEFAULT_SINK_NUMBER_OF_REPLICATIONS = "ksql.sink.replications.default";
  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION";
  public static final String DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION = "ksql.sink"
                                                                                  + ".window.change.log"
                                                                   + ".additional.retention"
                                                                   + ".default";

  public int defaultSinkNumberOfPartitions = 10;
  public short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  Map<String, Object> ksqlConfigProps;


  private static final ConfigDef CONFIG_DEF = new ConfigDef(StreamsConfig.configDef());

  public KSQLConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    ksqlConfigProps = new HashMap<>();
    for (Object propKey: props.keySet()) {
      ksqlConfigProps.put(propKey.toString(), props.get(propKey));
    }
    ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS, defaultSinkNumberOfPartitions);
    ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS, defaultSinkNumberOfReplications);
    ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION,
                        defaultSinkWindowChangeLogAdditionalRetention);
  }

  protected KSQLConfig(ConfigDef config, Map<?, ?> props) {
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
        0
    );
    result.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        0
    );
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

  public KSQLConfig clone() {
    return new KSQLConfig(this.ksqlConfigProps);
  }
}
