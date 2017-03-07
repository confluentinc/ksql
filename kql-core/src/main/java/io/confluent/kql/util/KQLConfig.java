/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class KQLConfig extends AbstractConfig {

  private static final ConfigDef config = new ConfigDef(StreamsConfig.configDef());

  public KQLConfig(Map<?, ?> props) {
    super(config, props);
  }

  protected KQLConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }
}
