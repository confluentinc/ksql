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
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

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

  public static final String COMPACTION_STYLE = CONFIG_PREFIX + "compaction.style";
  private static final String COMPACTION_STYLE_DOC =
      "The RocksDB instance will apply this compaction style "
          + "to remove the invalidated records of the state store.";

  public static final String COMPRESSION_TYPE = CONFIG_PREFIX + "compression.type";
  private static final String COMPRESSION_TYPE_DOC =
      "The RocksDB instance will apply this compression strategy to compress the SST files.";

  public static final String MAX_BACKGROUND_JOBS = CONFIG_PREFIX + "max.background.jobs";
  private static final String MAX_BACKGROUND_JOBS_DOC =
      "The RocksDB instance will apply this as the number of background threads are used for "
          + "compaction and memtable flushes.";

  public static final String COMPACTION_TRIVIAL_MOVE =
      CONFIG_PREFIX + "compaction.trivial.move";
  private static final String COMPACTION_TRIVIAL_MOVE_DOC =
      "The RocksDB instance will enable trivial move if this flag is true. This feature is always "
          + "enabled for LEVELED compaction. Therefore, this is only relevant if the compaction "
          + "style is set to UNIVERSAL.";




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
          INDEX_FILTER_BLOCK_RATIO_DOC)
      .define(
          COMPACTION_STYLE,
          Type.STRING,
          CompactionStyle.UNIVERSAL.name(),
          ConfigDef.ValidString.in(
              CompactionStyle.UNIVERSAL.name(), CompactionStyle.LEVEL.name()),
          Importance.LOW,
          COMPACTION_STYLE_DOC)
      .define(
          COMPRESSION_TYPE,
          Type.STRING,
          CompressionType.NO_COMPRESSION.name(),
          ConfigDef.ValidString.in(
              CompressionType.NO_COMPRESSION.name(),
              CompressionType.SNAPPY_COMPRESSION.name(),
              CompressionType.ZLIB_COMPRESSION.name(),
              CompressionType.BZLIB2_COMPRESSION.name(),
              CompressionType.LZ4_COMPRESSION.name(),
              CompressionType.LZ4HC_COMPRESSION.name(),
              CompressionType.XPRESS_COMPRESSION.name(),
              CompressionType.ZSTD_COMPRESSION.name()),
          Importance.LOW,
          COMPRESSION_TYPE_DOC
      ).define(
          MAX_BACKGROUND_JOBS,
          Type.INT,
          -1,
          Importance.LOW,
          MAX_BACKGROUND_JOBS_DOC
      ).define(
          COMPACTION_TRIVIAL_MOVE,
          Type.BOOLEAN,
          false,
          Importance.LOW,
          COMPACTION_TRIVIAL_MOVE_DOC
      );

  public KsqlBoundedMemoryRocksDBConfig(final Map<?, ?> properties) {
    super(CONFIG_DEF, properties);
  }
}
