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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

/**
 * This {@code RocksDBConfigSetter} implementation limits the total memory used
 * across all RocksDB instances to the number of bytes passed via
 * "ksql.plugins.rocksdb.total.memory", and also configures
 * the shared RocksDB thread pool to use "ksql.plugins.rocksdb.num.background.threads" threads.
 * Moreover, it sets the RocksDB compaction and compression algorithms as well as maximum
 * background jobs for compaction and memtable flushes and the trivial move as they are configured
 * through "ksql.plugins.rocksdb.compaction.style", "ksql.plugins.rocksdb.compression.type",
 * "ksql.plugins.rocksdb.max.background.jobs", and "ksql.plugins.rocksdb.compaction.trivial.move"
 * respectively.
 * </p>
 * See https://docs.confluent.io/5.3.0/streams/developer-guide/memory-mgmt.html#rocksdb.
 */
public class KsqlBoundedMemoryRocksDBConfigSetter implements RocksDBConfigSetter, Configurable {

  private static org.rocksdb.Cache cache;
  private static org.rocksdb.WriteBufferManager writeBufferManager;
  private static final AtomicBoolean configured = new AtomicBoolean(false);
  private static CompactionStyle compactionStyle;
  private static CompressionType compressionType;
  private static int maxNumConcurrentJobs;
  private static boolean allowTrivialMove;


  @Override
  public void configure(final Map<String, ?> config) {
    configure(
        config,
        new Options(),
        org.rocksdb.LRUCache::new,
        org.rocksdb.WriteBufferManager::new
    );
  }

  @VisibleForTesting
  static void configure(
      final Map<String, ?> config,
      final Options options,
      final LruCacheFactory cacheFactory,
      final WriteBufferManagerFactory bufferManagerFactory) {
    if (configured.getAndSet(true)) {
      throw new IllegalStateException(
          "KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure.");
    }

    try {
      final KsqlBoundedMemoryRocksDBConfig pluginConfig =
          new KsqlBoundedMemoryRocksDBConfig(config);

      limitTotalMemory(pluginConfig, cacheFactory, bufferManagerFactory);
      configureNumThreads(pluginConfig, options);
      setCompactionStyle(pluginConfig);
      setCompressionType(pluginConfig);
      setCompactionTrivialMove(pluginConfig);
      setMaximumConcurrentBackgroundJobs(pluginConfig);
    } catch (final IllegalArgumentException e) {
      reset();
      throw e;
    }
  }

  private static void setCompactionStyle(final KsqlBoundedMemoryRocksDBConfig config) {
    compactionStyle =
        CompactionStyle.valueOf(config.getString(KsqlBoundedMemoryRocksDBConfig.COMPACTION_STYLE));
  }

  private static void setCompressionType(final KsqlBoundedMemoryRocksDBConfig config) {
    compressionType =
        CompressionType.valueOf(config.getString(KsqlBoundedMemoryRocksDBConfig.COMPRESSION_TYPE));
  }

  private static void setCompactionTrivialMove(final KsqlBoundedMemoryRocksDBConfig config) {
    allowTrivialMove = config.getBoolean(
        KsqlBoundedMemoryRocksDBConfig.COMPACTION_TRIVIAL_MOVE);
  }

  private static void setMaximumConcurrentBackgroundJobs(
      final KsqlBoundedMemoryRocksDBConfig config) {
    maxNumConcurrentJobs = config.getInt(
        KsqlBoundedMemoryRocksDBConfig.MAX_BACKGROUND_JOBS);
  }

  @VisibleForTesting
  static void reset() {
    configured.set(false);
  }

  private static void limitTotalMemory(
      final KsqlBoundedMemoryRocksDBConfig config,
      final LruCacheFactory cacheFactory,
      final WriteBufferManagerFactory bufferManagerFactory
  ) {
    final long blockCacheSize =
        config.getLong(KsqlBoundedMemoryRocksDBConfig.BLOCK_CACHE_SIZE);
    final long totalMemtableMemory =
        config.getLong(KsqlBoundedMemoryRocksDBConfig.WRITE_BUFFER_LIMIT) == -1
            ? blockCacheSize / 2
            : config.getLong(KsqlBoundedMemoryRocksDBConfig.WRITE_BUFFER_LIMIT);
    final boolean useCacheForMemtable =
        config.getBoolean(KsqlBoundedMemoryRocksDBConfig.ACCOUNT_WRITE_BUFFER_AGAINST_CACHE);
    final boolean strictCacheLimit =
        config.getBoolean(KsqlBoundedMemoryRocksDBConfig.STRICT_CACHE_LIMIT);

    final double indexFilterBlockRatio =
        config.getDouble(KsqlBoundedMemoryRocksDBConfig.INDEX_FILTER_BLOCK_RATIO_CONFIG);

    cache = cacheFactory.create(
        blockCacheSize,
        -1,
        strictCacheLimit,
        indexFilterBlockRatio
    );

    final Cache cacheForWriteBuffer = useCacheForMemtable
        ? cache : cacheFactory.create(totalMemtableMemory, -1, false, 0);
    writeBufferManager = bufferManagerFactory.create(
        totalMemtableMemory,
        cacheForWriteBuffer
    );
  }

  private static void configureNumThreads(
      final KsqlBoundedMemoryRocksDBConfig config,
      final Options options) {
    final int numBackgroundThreads =
        config.getInt(KsqlBoundedMemoryRocksDBConfig.N_BACKGROUND_THREADS_CONFIG);

    // All Options share the same Env, and share a thread pool as a result
    options.getEnv().setBackgroundThreads(numBackgroundThreads);
  }

  @Override
  public void setConfig(
      final String storeName,
      final Options options,
      final Map<String, Object> configs) {
    if (!configured.get()) {
      throw new IllegalStateException(
          "Cannot use KsqlBoundedMemoryRocksDBConfigSetter before it's been configured.");
    }

    final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig)options.tableFormatConfig();

    tableConfig.setBlockCache(cache);
    tableConfig.setCacheIndexAndFilterBlocks(true);
    options.setWriteBufferManager(writeBufferManager);
    options.setCompactionStyle(compactionStyle);
    options.setCompressionType(compressionType);
    if (maxNumConcurrentJobs != -1) {
      options.setMaxBackgroundJobs(maxNumConcurrentJobs);
    }
    if (compactionStyle.equals(CompactionStyle.UNIVERSAL)) {
      if (options.compactionOptionsUniversal() == null) {
        final CompactionOptionsUniversal compactionOptionsUniversal =
            new CompactionOptionsUniversal();
        compactionOptionsUniversal.setAllowTrivialMove(allowTrivialMove);
        options.setCompactionOptionsUniversal(compactionOptionsUniversal);
      } else {
        options.compactionOptionsUniversal().setAllowTrivialMove(allowTrivialMove);
      }
    }

    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    tableConfig.setPinTopLevelIndexAndFilter(true);

    options.setStatsDumpPeriodSec(0);

    options.setTableFormatConfig(tableConfig);
  }

  @Override
  public void close(final String storeName, final Options options) {
  }

  interface LruCacheFactory {
    org.rocksdb.LRUCache create(
        long size, int shardBits, boolean strict, double indexFilterBlockRatio);
  }

  interface WriteBufferManagerFactory {
    org.rocksdb.WriteBufferManager create(
        long maxMemory,
        org.rocksdb.Cache cache
    );
  }
}