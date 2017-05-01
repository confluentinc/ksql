/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class KQLConfig extends AbstractConfig {

  private static final ConfigDef CONFIG = new ConfigDef(StreamsConfig.configDef());

  public KQLConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  protected KQLConfig(ConfigDef config, Map<?, ?> props) {
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

}
