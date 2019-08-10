/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rocksdb;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class KsqlBoundedMemoryRocksDBConfig extends AbstractConfig {

  private static final String CONFIG_PREFIX = "ksql.plugins.rocksdb.";

  public static final String TOTAL_OFF_HEAP_MEMORY_CONFIG = CONFIG_PREFIX + "total.memory";
  private static final String TOTAL_OFF_HEAP_MEMORY_DOC = ""; // TODO

  public static final String N_BACKGROUND_THREADS_CONFIG =
      CONFIG_PREFIX + "num.background.threads";
  private static final String N_BACKGROUND_THREADS_DOC = ""; // TODO

  public static final String INDEX_FILTER_BLOCK_RATIO_CONFIG =
      CONFIG_PREFIX + "index.filter.block.ratio";
  private static final double INDEX_FILTER_BLOCK_RATIO_DEFAULT = 0.0;
  private static final String INDEX_FILTER_BLOCK_RATIO_DOC = ""; // TODO

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          TOTAL_OFF_HEAP_MEMORY_CONFIG,
          Type.LONG,
          ConfigDef.NO_DEFAULT_VALUE,
          Importance.HIGH,
          TOTAL_OFF_HEAP_MEMORY_DOC)
      .define(
          N_BACKGROUND_THREADS_CONFIG,
          Type.INT,
          1,
          Importance.MEDIUM,
          N_BACKGROUND_THREADS_DOC)
      .define(
          INDEX_FILTER_BLOCK_RATIO_CONFIG,
          Type.DOUBLE,
          INDEX_FILTER_BLOCK_RATIO_DEFAULT,
          Importance.LOW,
          INDEX_FILTER_BLOCK_RATIO_DOC
      );

  public KsqlBoundedMemoryRocksDBConfig(final Map<?, ?> properties) {
    super(CONFIG_DEF, properties);
  }
}
