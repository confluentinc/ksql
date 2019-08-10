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
import org.rocksdb.Options;

/**
 * This {@code RocksDBConfigSetter} implementation limits the total memory used
 * across all RocksDB instances to the number of bytes passed via
 * "ksql.plugins.rocksdb.total.memory", and also configures
 * the shared RocksDB thread pool to use "ksql.plugins.rocksdb.num.background.threads" threads.
 * </p>
 * See https://docs.confluent.io/5.3.0/streams/developer-guide/memory-mgmt.html#rocksdb.
 */
public class KsqlBoundedMemoryRocksDBConfigSetter implements RocksDBConfigSetter, Configurable {

  private static final double INDEX_FILTER_BLOCK_RATIO = 0.1;

  private static org.rocksdb.Cache cache;
  private static org.rocksdb.WriteBufferManager writeBufferManager;

  private static final AtomicBoolean configured = new AtomicBoolean(false);

  @Override
  public void configure(final Map<String, ?> config) {
    configure(config, new Options());
  }

  @VisibleForTesting
  static void configure(final Map<String, ?> config, final Options options) {
    if (configured.getAndSet(true)) {
      throw new IllegalStateException(
          "KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure.");
    }

    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(config);

    try {
      limitTotalMemory(pluginConfig);
      configureNumThreads(pluginConfig, options);
    } catch (IllegalArgumentException e) {
      configured.set(false);
      throw e;
    }
  }

  @VisibleForTesting
  static void reset() {
    configured.set(false);
  }

  private static void limitTotalMemory(final KsqlBoundedMemoryRocksDBConfig config) {
    final long totalOffHeapMemory =
        config.getLong(KsqlBoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_MEMORY_CONFIG);
    final long totalMemtableMemory = totalOffHeapMemory / 2;

    cache = new org.rocksdb.LRUCache(totalOffHeapMemory, -1, false, INDEX_FILTER_BLOCK_RATIO);
    writeBufferManager = new org.rocksdb.WriteBufferManager(totalMemtableMemory, cache);
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

    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    tableConfig.setPinTopLevelIndexAndFilter(true);
    options.setMaxWriteBufferNumber(Integer.MAX_VALUE);

    options.setStatsDumpPeriodSec(0);

    options.setTableFormatConfig(tableConfig);
  }

  @Override
  public void close(final String storeName, final Options options) {
  }
}