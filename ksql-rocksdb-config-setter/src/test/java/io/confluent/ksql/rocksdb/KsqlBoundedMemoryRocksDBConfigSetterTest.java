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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.WriteBufferManager;

@RunWith(MockitoJUnitRunner.class)
public class KsqlBoundedMemoryRocksDBConfigSetterTest {

  private static final long TOTAL_OFF_HEAP_MEMORY = 16 * 1024 * 1024 * 1024L;
  private static final int NUM_BACKGROUND_THREADS = 4;

  private static final Map<String, Object> CONFIG_PROPS = ImmutableMap.of(
      "ksql.plugins.rocksdb.total.memory", TOTAL_OFF_HEAP_MEMORY,
      "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS);

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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    KsqlBoundedMemoryRocksDBConfigSetter.reset();

    rocksDBConfig = new KsqlBoundedMemoryRocksDBConfigSetter();
    secondRocksDBConfig = new KsqlBoundedMemoryRocksDBConfigSetter();

    when(rocksOptions.tableFormatConfig()).thenReturn(tableConfig);
    when(secondRocksOptions.tableFormatConfig()).thenReturn(secondTableConfig);
    when(rocksOptions.getEnv()).thenReturn(env);
  }

  @Test
  public void shouldFailWithoutConfigure() {
    // Expect:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "Cannot use KsqlBoundedMemoryRocksDBConfigSetter before it's been configured.");

    // When:
    rocksDBConfig.setConfig("store_name", rocksOptions, Collections.emptyMap());
  }

  @Test
  public void shouldFailIfConfiguredTwiceFromSameInstance() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);

    // Expect:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure.");

    // When:
    rocksDBConfig.configure(CONFIG_PROPS);
  }

  @Test
  public void shouldFailIfConfiguredTwiceFromDifferentInstances() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);

    // Expect:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure.");

    // When:
    secondRocksDBConfig.configure(CONFIG_PROPS);
  }

  @Test
  public void shouldSetConfig() {
    // Given:
    rocksDBConfig.configure(CONFIG_PROPS);

    // When:
    rocksDBConfig.setConfig("store_name", rocksOptions, Collections.emptyMap());

    // Then:
    verify(rocksOptions).setWriteBufferManager(any());
    verify(rocksOptions).setMaxWriteBufferNumber(6);
    verify(rocksOptions).setWriteBufferSize(TOTAL_OFF_HEAP_MEMORY / 2 / 6);
    verify(rocksOptions).setStatsDumpPeriodSec(0);
    verify(rocksOptions).setTableFormatConfig(tableConfig);

    verify(tableConfig).setBlockCache(any());
    verify(tableConfig).setCacheIndexAndFilterBlocks(true);
    verify(tableConfig).setCacheIndexAndFilterBlocksWithHighPriority(true);
    verify(tableConfig).setPinTopLevelIndexAndFilter(true);
    verify(tableConfig).setBlockSize(4096L);
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
    KsqlBoundedMemoryRocksDBConfigSetter.configure(CONFIG_PROPS, rocksOptions);

    // Then:
    verify(env).setBackgroundThreads(NUM_BACKGROUND_THREADS);
  }
}