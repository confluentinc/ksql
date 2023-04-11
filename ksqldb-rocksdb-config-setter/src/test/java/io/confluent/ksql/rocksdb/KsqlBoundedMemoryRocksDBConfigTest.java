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

import static com.google.common.collect.ImmutableMap.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

public class KsqlBoundedMemoryRocksDBConfigTest {

  private static final long CACHE_SIZE = 16 * 1024 * 1024 * 1024L;
  private static final int NUM_BACKGROUND_THREADS = 4;
  private static final double INDEX_FILTER_BLOCK_RATIO = 0.1;
  private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.LEVEL;
  private static final CompressionType COMPRESSION_TYPE = CompressionType.LZ4_COMPRESSION;
  private static final int MAX_BACKGROUND_JOBS = 10;
  private static final boolean ALLOW_TRIVIAL_MOVE = true;

  @Test
  public void shouldCreateConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.cache.size", CACHE_SIZE,
        "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS,
        "ksql.plugins.rocksdb.index.filter.block.ratio", INDEX_FILTER_BLOCK_RATIO,
        "ksql.plugins.rocksdb.compaction.style", COMPACTION_STYLE.name(),
        "ksql.plugins.rocksdb.compression.type", COMPRESSION_TYPE.name()
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getLong(KsqlBoundedMemoryRocksDBConfig.BLOCK_CACHE_SIZE),
        is(CACHE_SIZE));
    assertThat(
        pluginConfig.getInt(KsqlBoundedMemoryRocksDBConfig.N_BACKGROUND_THREADS_CONFIG),
        is(NUM_BACKGROUND_THREADS));
    assertThat(
        pluginConfig.getDouble(KsqlBoundedMemoryRocksDBConfig.INDEX_FILTER_BLOCK_RATIO_CONFIG),
        is(INDEX_FILTER_BLOCK_RATIO));
    assertThat(
        pluginConfig.getString(KsqlBoundedMemoryRocksDBConfig.COMPACTION_STYLE),
        is(COMPACTION_STYLE.name()));
    assertThat(
        pluginConfig.getString(KsqlBoundedMemoryRocksDBConfig.COMPRESSION_TYPE),
        is(COMPRESSION_TYPE.name()));
  }

  @Test
  public void shouldFailWithoutTotalMemoryConfig() {
    // Given:
    final Map<String, Object> configs = of(
        "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS
    );

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> new KsqlBoundedMemoryRocksDBConfig(configs)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Missing required configuration \"ksql.plugins.rocksdb.cache.size\" "
            + "which has no default value."));
  }

  @Test
  public void shouldDefaultNumThreadsConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.cache.size", CACHE_SIZE
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getInt(KsqlBoundedMemoryRocksDBConfig.N_BACKGROUND_THREADS_CONFIG),
        is(1));
  }

  @Test
  public void shouldDefaultIndexFilterBlockRatioConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.cache.size", CACHE_SIZE
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getDouble(KsqlBoundedMemoryRocksDBConfig.INDEX_FILTER_BLOCK_RATIO_CONFIG),
        is(0.0));
  }

  @Test
  public void shouldDefaultCompactionStyleConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.cache.size", CACHE_SIZE
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getString(KsqlBoundedMemoryRocksDBConfig.COMPACTION_STYLE),
        is(CompactionStyle.UNIVERSAL.name()));
  }

  @Test
  public void shouldDefaultCompressionTypeConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.cache.size", CACHE_SIZE
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getString(KsqlBoundedMemoryRocksDBConfig.COMPRESSION_TYPE),
        is(CompressionType.NO_COMPRESSION.name()));
  }
}
