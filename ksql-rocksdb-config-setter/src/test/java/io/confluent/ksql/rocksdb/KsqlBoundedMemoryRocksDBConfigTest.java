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
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlBoundedMemoryRocksDBConfigTest {

  private static final long TOTAL_OFF_HEAP_MEMORY = 16 * 1024 * 1024 * 1024L;
  private static final int NUM_BACKGROUND_THREADS = 4;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldCreateConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.total.memory", TOTAL_OFF_HEAP_MEMORY,
        "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getLong(KsqlBoundedMemoryRocksDBConfig.TOTAL_OFF_HEAP_MEMORY_CONFIG),
        is(TOTAL_OFF_HEAP_MEMORY));
    assertThat(
        pluginConfig.getInt(KsqlBoundedMemoryRocksDBConfig.N_BACKGROUND_THREADS_CONFIG),
        is(NUM_BACKGROUND_THREADS));
  }

  @Test
  public void shouldFailWithoutTotalMemoryConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.num.background.threads", NUM_BACKGROUND_THREADS
    );

    // Expect:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage(
        "Missing required configuration \"ksql.plugins.rocksdb.total.memory\" which has no default value.");

    // When:
    new KsqlBoundedMemoryRocksDBConfig(configs);
  }

  @Test
  public void shouldDefaultNumThreadsConfig() {
    // Given:
    final Map<String, Object> configs = ImmutableMap.of(
        "ksql.plugins.rocksdb.total.memory", TOTAL_OFF_HEAP_MEMORY
    );

    // When:
    final KsqlBoundedMemoryRocksDBConfig pluginConfig = new KsqlBoundedMemoryRocksDBConfig(configs);

    // Then:
    assertThat(
        pluginConfig.getInt(KsqlBoundedMemoryRocksDBConfig.N_BACKGROUND_THREADS_CONFIG),
        is(1));
  }
}
