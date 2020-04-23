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

  public static final String BLOCK_CACHE_SIZE = CONFIG_PREFIX + "cache.size";
  private static final String TOTAL_OFF_HEAP_MEMORY_DOC =
      "All RocksDB instances across all KSQL queries (i.e., all Kafka Streams applications) "
      + "will be limited to sharing this much memory (in bytes) for block cache.";

  public static final String WRITE_BUFFER_LIMIT = CONFIG_PREFIX + "write.buffer.size";
  private static final String WRITE_BUFFER_LIMIT_DOC =
      "All RocksDB instances across all KSQL queries (i.e. all Kafka Streams applications) "
          + "will be limited to sharing this much write buffer memory.";
  private static final int WRITE_BUFFER_LIMIT_DEFAULT = -1;

  public static final String STRICT_CACHE_LIMIT = CONFIG_PREFIX + "cache.limit.strict";
  private static final String STRICT_CACHE_LIMIT_DOC = "Apply a strict limit to the cache size";

  public static final String ACCOUNT_WRITE_BUFFER_AGAINST_CACHE
      = CONFIG_PREFIX + "write.buffer.cache.use";
  private static final String ACCOUNT_WRITE_BUFFER_AGAINST_CACHE_DOC =
      "Account write buffer usage against the block cache";

  public static final String N_BACKGROUND_THREADS_CONFIG =
      CONFIG_PREFIX + "num.background.threads";
  private static final String N_BACKGROUND_THREADS_DOC =
      "Number of low-priority RocksDB threads to be shared among instances for compaction.";

  public static final String INDEX_FILTER_BLOCK_RATIO_CONFIG =
      CONFIG_PREFIX + "index.filter.block.ratio";
  private static final double INDEX_FILTER_BLOCK_RATIO_DEFAULT = 0.0;
  private static final String INDEX_FILTER_BLOCK_RATIO_DOC =
      "Percentage of the RocksDB block cache to set aside for high-priority entries, i.e., "
      + "index and filter blocks.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          BLOCK_CACHE_SIZE,
          Type.LONG,
          ConfigDef.NO_DEFAULT_VALUE,
          Importance.HIGH,
          TOTAL_OFF_HEAP_MEMORY_DOC)
      .define(
          WRITE_BUFFER_LIMIT,
          Type.LONG,
          WRITE_BUFFER_LIMIT_DEFAULT,
          Importance.HIGH,
          WRITE_BUFFER_LIMIT_DOC)
      .define(
          ACCOUNT_WRITE_BUFFER_AGAINST_CACHE,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          ACCOUNT_WRITE_BUFFER_AGAINST_CACHE_DOC)
      .define(
          STRICT_CACHE_LIMIT,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          STRICT_CACHE_LIMIT_DOC)
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
