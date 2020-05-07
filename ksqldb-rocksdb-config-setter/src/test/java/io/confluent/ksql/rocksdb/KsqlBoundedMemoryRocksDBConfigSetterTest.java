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

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rocksdb.KsqlBoundedMemoryRocksDBConfigSetter.LruCacheFactory;
import io.confluent.ksql.rocksdb.KsqlBoundedMemoryRocksDBConfigSetter.WriteBufferManagerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.WriteBufferManager;

@RunWith(MockitoJUnitRunner.class)
public class KsqlBoundedMemoryRocksDBConfigSetterTest {

  private static final long CACHE_SIZE = 16 * 1024 * 1024 * 1024L;
  private static final long WRITE_BUFFER_SIZE = 8 * 1024 * 1024 * 1024L;
  private static final int NUM_BACKGROUND_THREADS = 4;

  private static final Map<String, Object> CONFIG_PROPS = new HashMap<>(ImmutableMap.of(
      "ksql.plugins.rocksdb.cache.size", CACHE_SIZE,
      "ksql.plugins.rocksdb.write.buffer.size", WRITE_BUFFER_SIZE,
      "ksql.plugins.rocksdb.write.buffer.cache.use", true,
      "ksql.plugns.rocksdb.cache.limit.strict", false,
      "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS)
  );

  @Mock
  private Options rocksOptions;
  @Mock
  private Options secondRocksOptions;
  @Mock
  private BlockBasedTableConfig tableConfig;
  @Mock
  private BlockBasedTableConfig secondTableConfig;
  @Mock
  private Env env;
  @Mock
  private LruCacheFactory cacheFactory;
  @Mock
  private WriteBufferManagerFactory bufferManagerFactory;
  @Mock
  private LRUCache blockCache;
  @Mock
  private LRUCache writeCache;
  @Mock
  private WriteBufferManager bufferManager;
  @Captor
  private ArgumentCaptor<WriteBufferManager> writeBufferManagerCaptor;
  @Captor
  private ArgumentCaptor<WriteBufferManager> secondWriteBufferManagerCaptor;
  @Captor
  private ArgumentCaptor<Cache> cacheCaptor;
  @Captor
  private ArgumentCaptor<Cache> secondCacheCaptor;

  private KsqlBoundedMemoryRocksDBConfigSetter rocksDBConfig;
  private KsqlBoundedMemoryRocksDBConfigSetter secondRocksDBConfig;

  @Before
  public void setUp() {
    KsqlBoundedMemoryRocksDBConfigSetter.reset();

    rocksDBConfig = new KsqlBoundedMemoryRocksDBConfigSetter();
    secondRocksDBConfig = new KsqlBoundedMemoryRocksDBConfigSetter();

    when(rocksOptions.tableFormatConfig()).thenReturn(tableConfig);
    when(secondRocksOptions.tableFormatConfig()).thenReturn(secondTableConfig);
    when(rocksOptions.getEnv()).thenReturn(env);
    when(bufferManagerFactory.create(anyLong(), any())).thenReturn(bufferManager);
    when(cacheFactory.create(anyLong(), anyInt(), anyBoolean(), anyDouble()))
        .thenReturn(blockCache)
        .thenReturn(writeCache)
        .thenThrow(new IllegalStateException());
  }

  @Test
  public void shouldFailWithoutConfigure() {
    // Expect:
    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> rocksDBConfig.setConfig("store_name", rocksOptions, emptyMap())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Cannot use KsqlBoundedMemoryRocksDBConfigSetter before it's been configured."));
  }

  @Test
  public void shouldFailIfConfiguredTwiceFromSameInstance() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);

    // Expect:
    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> rocksDBConfig.configure(CONFIG_PROPS)
    );

    // Then:
    assertThat(e.getMessage(), containsString("KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure."));
  }

  @Test
  public void shouldFailIfConfiguredTwiceFromDifferentInstances() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);

    // Expect:
    // When:
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> secondRocksDBConfig.configure(CONFIG_PROPS)
    );

    // Then:
    assertThat(e.getMessage(), containsString("KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure."));
  }

  @Test
  public void shouldSetConfig() {
    // Given:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS, rocksOptions, cacheFactory, bufferManagerFactory);

    // When:
    rocksDBConfig.setConfig("store_name", rocksOptions, Collections.emptyMap());

    // Then:
    verify(rocksOptions).setWriteBufferManager(bufferManager);
    verify(rocksOptions).setStatsDumpPeriodSec(0);
    verify(rocksOptions).setTableFormatConfig(tableConfig);

    verify(tableConfig).setBlockCache(blockCache);
    verify(tableConfig).setCacheIndexAndFilterBlocks(true);
    verify(tableConfig).setCacheIndexAndFilterBlocksWithHighPriority(true);
    verify(tableConfig).setPinTopLevelIndexAndFilter(true);
  }

  @Test
  public void shouldShareCacheAcrossInstances() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);
    rocksDBConfig.setConfig("store_name", rocksOptions, Collections.emptyMap());

    // When:
    secondRocksDBConfig.setConfig("store_name", secondRocksOptions, Collections.emptyMap());

    // Then:
    verify(tableConfig).setBlockCache(cacheCaptor.capture());
    verify(secondTableConfig).setBlockCache(secondCacheCaptor.capture());
    assertThat(cacheCaptor.getValue(), sameInstance(secondCacheCaptor.getValue()));
  }

  @Test
  public void shouldShareWriteBufferManagerAcrossInstances() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);
    rocksDBConfig.setConfig("store_name", rocksOptions, Collections.emptyMap());

    // When:
    secondRocksDBConfig.setConfig("store_name", secondRocksOptions, Collections.emptyMap());

    // Then:
    verify(rocksOptions).setWriteBufferManager(writeBufferManagerCaptor.capture());
    verify(secondRocksOptions).setWriteBufferManager(secondWriteBufferManagerCaptor.capture());
    assertThat(
        writeBufferManagerCaptor.getValue(),
        sameInstance(secondWriteBufferManagerCaptor.getValue()));
  }

  @Test
  public void shouldSetNumThreads() {
    // When:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(env).setBackgroundThreads(NUM_BACKGROUND_THREADS);
  }

  @Test
  public void shouldUseCacheForWriteBufferIfConfigured() {
    // When:
    CONFIG_PROPS.put(KsqlBoundedMemoryRocksDBConfig.ACCOUNT_WRITE_BUFFER_AGAINST_CACHE, true);
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(bufferManagerFactory).create(anyLong(), same(blockCache));
  }

  @Test
  public void shouldNotUseCacheForWriteBufferIfNotConfigured() {
    // When:
    CONFIG_PROPS.put(KsqlBoundedMemoryRocksDBConfig.ACCOUNT_WRITE_BUFFER_AGAINST_CACHE, false);
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(bufferManagerFactory).create(anyLong(), same(writeCache));
  }

  @Test
  public void shouldUseStrictCacheIfConfigured() {
    // When:
    CONFIG_PROPS.put(KsqlBoundedMemoryRocksDBConfig.STRICT_CACHE_LIMIT, true);
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(cacheFactory).create(anyLong(), anyInt(), eq(true), anyDouble());
  }

  @Test
  public void shouldNotUseStrictCacheIfNotConfigured() {
    // When:
    CONFIG_PROPS.put(KsqlBoundedMemoryRocksDBConfig.STRICT_CACHE_LIMIT, false);
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(cacheFactory).create(anyLong(), anyInt(), eq(false), anyDouble());
  }

  @Test
  public void shouldUseConfiguredBlockCacheSize() {
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(cacheFactory).create(eq(16 * 1024 * 1024 * 1024L), anyInt(), anyBoolean(), anyDouble());
  }

  @Test
  public void shouldUseConfiguredWriteBufferSize() {
    KsqlBoundedMemoryRocksDBConfigSetter.configure(
        CONFIG_PROPS,
        rocksOptions,
        cacheFactory,
        bufferManagerFactory
    );

    // Then:
    verify(bufferManagerFactory).create(eq(8 * 1024 * 1024 * 1024L), any());
  }
}