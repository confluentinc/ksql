/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.streams.metrics.RocksDBMetricsCollector.Interval;
import io.confluent.ksql.util.KsqlConfig;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class RocksDBMetricsCollectorTest {
  private static final int UPDATE_INTERVAL = 123;

  @Mock
  private Metrics metrics;
  @Mock
  private Supplier<Instant> clock;
  @Captor
  private ArgumentCaptor<MetricValueProvider<?>> metricValueProvider;

  private RocksDBMetricsCollector collector;

  @Rule
  public final MockitoRule rule = MockitoJUnit.rule();

  @Before
  public void setup() {
    collector = new RocksDBMetricsCollector();
    resetMetrics();
    collector.configure(
        ImmutableMap.of(
            RocksDBMetricsCollector.UPDATE_INTERVAL_CONFIG, UPDATE_INTERVAL,
            KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG, metrics
        )
    );
  }

  private void resetMetrics() {
    reset(metrics);
    when(metrics.metricName(any(), any())).thenAnswer(
        a -> new MetricName(a.getArgument(0), a.getArgument(1), "", Collections.emptyMap()));
  }

  @After
  public void cleanup() {
    RocksDBMetricsCollector.reset();
  }

  private void shouldComputeMaxOfAllStoreMetrics(final String name, final String otherName) {
    // Given:
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, name, "a", BigInteger.valueOf(2)));
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, otherName, "a", BigInteger.valueOf(123)));
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, name, "b", BigInteger.valueOf(3)));

    // When:
    final Gauge<?> gauge = verifyAndGetRegisteredMetric(name + "-max");
    final Object value = gauge.value(null, 0);

    // Then:
    assertThat(value, equalTo(BigInteger.valueOf(3)));
  }

  @Test
  public void shouldComputeMaxOfBlockCacheUsage() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE,
        RocksDBMetricsCollector.BLOCK_CACHE_PINNED_USAGE
    );
  }

  @Test
  public void shouldComputeMaxOfBlockCachePinnedUsage() {
    shouldComputeMaxOfAllStoreMetrics(
        RocksDBMetricsCollector.BLOCK_CACHE_PINNED_USAGE,
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE
    );
  }

  private void shouldComputeSumOfAllStoreMetrics(final String name, final String otherName) {
    // Given:
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, name, "a", BigInteger.valueOf(2)));
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, otherName, "a", BigInteger.valueOf(123)));
    collector.metricChange(mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP, name, "b", BigInteger.valueOf(3)));

    // When:
    final Gauge<?> gauge = verifyAndGetRegisteredMetric(name + "-total");
    final Object value = gauge.value(null, 0);

    // Then:
    assertThat(value, equalTo(BigInteger.valueOf(5)));
  }

  @Test
  public void shouldComputeSumOfRunningCompactions() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.NUMBER_OF_RUNNING_COMPACTIONS,
        RocksDBMetricsCollector.BLOCK_CACHE_PINNED_USAGE
    );
  }

  @Test
  public void shouldComputeSumOfBlockCachePinnedUsage() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.BLOCK_CACHE_PINNED_USAGE,
        RocksDBMetricsCollector.NUMBER_OF_RUNNING_COMPACTIONS
    );
  }

  @Test
  public void shouldComputeSumOfEstimateNumKeys() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.ESTIMATE_NUM_KEYS,
        RocksDBMetricsCollector.NUMBER_OF_RUNNING_COMPACTIONS
    );
  }

  @Test
  public void shouldComputeSumOfEstimateTableReadersMem() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.ESTIMATE_TABLE_READERS_MEM,
        RocksDBMetricsCollector.NUMBER_OF_RUNNING_COMPACTIONS
    );
  }

  @Test
  public void shouldComputeSumOfBlockCacheUsage() {
    shouldComputeSumOfAllStoreMetrics(
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE,
        RocksDBMetricsCollector.NUMBER_OF_RUNNING_COMPACTIONS
    );
  }

  @Test
  public void shouldIgnoreMetricsFromWrongGroup() {
    // When:
    collector.metricChange(mockMetric(
        "some-group",
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE,
        "a",
        BigInteger.valueOf(123)
    ));

    // Then:
    final Gauge<?> value =
        verifyAndGetRegisteredMetric(RocksDBMetricsCollector.BLOCK_CACHE_USAGE + "-total");
    assertThat(value.value(null, 0), equalTo(BigInteger.valueOf(0)));
  }

  @Test
  public void shouldRemoveMetric() {
    // Given:
    final KafkaMetric metric = mockMetric(
            StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
            RocksDBMetricsCollector.BLOCK_CACHE_USAGE,
            "a",
            BigInteger.valueOf(2)
    );
    collector.metricChange(metric);

    // When:
    collector.metricRemoval(metric);

    // Then:
    final Gauge<?> gauge
        = verifyAndGetRegisteredMetric(RocksDBMetricsCollector.BLOCK_CACHE_USAGE + "-total");
    final Object value = gauge.value(null, 0);
    assertThat(value, equalTo(BigInteger.valueOf(0)));
  }

  @Test
  public void shouldComputeIntervalChangeCorrectly() {
    // Given:
    final Instant now = Instant.now();
    when(clock.get()).thenReturn(
        now,
        now.plusSeconds(UPDATE_INTERVAL - 10),
        now.plusSeconds(UPDATE_INTERVAL + 1),
        now.plusSeconds(UPDATE_INTERVAL + 10)
    );
    final Interval interval = new Interval(UPDATE_INTERVAL, clock);

    // When/Then:
    assertThat(interval.check(), is(true));
    assertThat(interval.check(), is(false));
    assertThat(interval.check(), is(true));
    assertThat(interval.check(), is(false));
  }

  @Test
  public void shouldComputeIntervalChangeCorrectlyInSameInstant() {
    // Given:
    final Instant now = Instant.now();
    when(clock.get()).thenReturn(now, now);
    final Interval interval = new Interval(UPDATE_INTERVAL, clock);

    // When/Then:
    assertThat(interval.check(), is(true));
    assertThat(interval.check(), is(false));
  }

  @Test
  public void shouldNotUpdateIfWithinInterval() {
    // Given:
    final RocksDBMetricsCollector collector = new RocksDBMetricsCollector();
    resetMetrics();
    RocksDBMetricsCollector.reset();
    collector.configure(
        ImmutableMap.of(
            RocksDBMetricsCollector.UPDATE_INTERVAL_CONFIG, 7200,
            KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG, metrics
        )
    );
    final KafkaMetric metric = mockMetric(
        StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE,
        "a",
        BigInteger.valueOf(2)
    );
    collector.metricChange(metric);
    final Gauge<?> gauge = verifyAndGetRegisteredMetric(
        RocksDBMetricsCollector.BLOCK_CACHE_USAGE + "-total");

    // When:
    gauge.value(null, 0);
    gauge.value(null, 0);
    gauge.value(null, 0);

    // Then:
    verify(metric, times(1)).metricValue();
  }

  private KafkaMetric mockMetric(
      final String group, final String name, final String store, Object value) {
    final KafkaMetric metric = mock(KafkaMetric.class);
    when(metric.metricName()).thenReturn(
        new MetricName(name, group, "", ImmutableMap.of("store", store)));
    when(metric.metricValue()).thenReturn(value);
    return metric;
  }

  private Gauge<?> verifyAndGetRegisteredMetric(final String name) {
    verify(metrics, atLeastOnce()).addMetric(
        argThat(
            n -> n.group().equals(RocksDBMetricsCollector.KSQL_ROCKSDB_METRICS_GROUP)
                && n.name().equals(name)
        ),
        metricValueProvider.capture()
    );
    return (Gauge<?>) metricValueProvider.getValue();
  }
}