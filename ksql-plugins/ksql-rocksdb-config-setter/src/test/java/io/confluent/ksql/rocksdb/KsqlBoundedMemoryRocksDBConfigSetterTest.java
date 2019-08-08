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

  @Mock
  private Map<String, Object> ksqlConfigs;
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

    when(ksqlConfigs.containsKey("ksql.plugins.rocksdb.total.memory")).thenReturn(true);
    when(ksqlConfigs.containsKey("ksql.plugins.rocksdb.num.background.threads")).thenReturn(true);
    when(ksqlConfigs.get("ksql.plugins.rocksdb.total.memory")).thenReturn(Long.toString(TOTAL_OFF_HEAP_MEMORY));
    when(ksqlConfigs.get("ksql.plugins.rocksdb.num.background.threads")).thenReturn(Integer.toString(
        NUM_BACKGROUND_THREADS));
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
  public void shouldFailIfConfiguredTwice() {
    // Given:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);

    // Expect:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "KsqlBoundedMemoryRocksDBConfigSetter has already been configured. Cannot re-configure.");

    // When:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);
  }

  @Test
  public void shouldSetConfig() {
    // Given:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);

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
  public void shouldFailWithoutTotalMemoryConfig() {
    // Given:
    when(ksqlConfigs.containsKey("ksql.plugins.rocksdb.total.memory")).thenReturn(false);

    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing configuration: ksql.plugins.rocksdb.total.memory");

    // When:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);
  }

  @Test
  public void shouldFailWithoutNumThreadsConfig() {
    // Given:
    when(ksqlConfigs.containsKey("ksql.plugins.rocksdb.num.background.threads")).thenReturn(false);

    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing configuration: ksql.plugins.rocksdb.num.background.threads");

    // When:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);
  }

  @Test
  public void shouldShareCacheAcrossInstances() {
    // Given:
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);
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
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs);
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
    KsqlBoundedMemoryRocksDBConfigSetter.configure(ksqlConfigs, rocksOptions);

    // Then:
    verify(env).setBackgroundThreads(NUM_BACKGROUND_THREADS);
  }
}