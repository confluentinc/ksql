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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBMetricsCollector implements MetricsReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMetricsCollector.class);

  static final String KSQL_ROCKSDB_METRICS_GROUP = "ksql-rocksdb-aggregates";
  static final String NUMBER_OF_RUNNING_COMPACTIONS = "num-running-compactions";
  static final String BLOCK_CACHE_USAGE = "block-cache-usage";
  static final String BLOCK_CACHE_PINNED_USAGE = "block-cache-pinned-usage";
  static final String ESTIMATE_NUM_KEYS = "estimate-num-keys";
  static final String ESTIMATE_TABLE_READERS_MEM = "estimate-table-readers-mem";
  static final String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE = "num-entries-active-mem-table";
  static final String NUMBER_OF_DELETES_ACTIVE_MEMTABLE = "num-deletes-active-mem-table";
  static final String NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES = "num-entries-imm-mem-tables";
  static final String NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES = "num-deletes-imm-mem-tables";
  static final String NUMBER_OF_IMMUTABLE_MEMTABLES = "num-immutable-mem-table";
  static final String CURRENT_SIZE_OF_ACTIVE_MEMTABLE = "cur-size-active-mem-table";
  static final String CURRENT_SIZE_OF_ALL_MEMTABLES = "cur-size-all-mem-tables";
  static final String SIZE_OF_ALL_MEMTABLES = "size-all-mem-tables";
  static final String MEMTABLE_FLUSH_PENDING = "mem-table-flush-pending";
  static final String NUMBER_OF_RUNNING_FLUSHES = "num-running-flushes";
  static final String COMPACTION_PENDING = "compaction-pending";
  static final String ESTIMATED_BYTES_OF_PENDING_COMPACTION = "estimate-pending-compaction-bytes";
  static final String TOTAL_SST_FILES_SIZE = "total-sst-files-size";
  static final String LIVE_SST_FILES_SIZE = "live-sst-files-size";

  static final String UPDATE_INTERVAL_CONFIG = "ksql.rocksdb.metrics.update.interval.seconds";

  private static final int UPDATE_INTERVAL_DEFAULT = 15;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          UPDATE_INTERVAL_CONFIG,
          Type.INT,
          UPDATE_INTERVAL_DEFAULT,
          Importance.LOW,
          "minimum interval between computations of a metric value"
      );

  private static final Object lock = new Object();

  private static Map<String, Collection<AggregatedMetric<?>>> registeredMetrics = null;
  private Metrics metrics;

  @Override
  public void configure(final Map<String, ?> map) {
    final AbstractConfig config = new AbstractConfig(CONFIG_DEF, map);
    this.metrics = Objects.requireNonNull(
        (Metrics) map.get(KsqlConfig.KSQL_INTERNAL_METRICS_CONFIG)
    );
    configureShared(config, metrics);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Collections.emptySet();
  }

  @Override
  public void init(final List<KafkaMetric> initial) {
    initial.forEach(this::metricChange);
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    if (!metric.metricName().group().equals(StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP)) {
      return;
    }
    metricRemoval(metric);
    final Collection<AggregatedMetric<?>> registered
        = registeredMetrics.get(metric.metricName().name());
    if (registered == null) {
      return;
    }
    registered.forEach(r -> r.add(metric));
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    final MetricName metricName = metric.metricName();
    if (!metricName.group().equals(StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP)) {
      return;
    }
    final Collection<AggregatedMetric<?>> registered
        = registeredMetrics.get(metricName.name());
    if (registered == null) {
      return;
    }
    registered.forEach(r -> r.remove(metricName));
  }

  @VisibleForTesting
  static void reset() {
    registeredMetrics = null;
  }

  @Override
  public void close() {
  }

  public static void update() {
    registeredMetrics.values().stream()
        .flatMap(Collection::stream)
        .forEach(AggregatedMetric::update);
  }

  private static void registerBigIntTotal(
      final int interval,
      final Map<String, Collection<AggregatedMetric<?>>> registeredMetrics,
      final String name,
      final Metrics metrics
  ) {
    registeredMetrics.putIfAbsent(name, new LinkedList<>());
    final AggregatedMetric<BigInteger> registered = new AggregatedMetric<>(
        BigInteger.class,
        BigInteger::add,
        BigInteger.ZERO,
        new Interval(interval)
    );
    registeredMetrics.get(name).add(registered);
    final MetricName metricName = metrics.metricName(name + "-total", KSQL_ROCKSDB_METRICS_GROUP);
    metrics.addMetric(metricName, (Gauge<BigInteger>) (c, t) -> registered.getValue());
  }

  private static void registerBigIntMax(
      final int interval,
      final Map<String, Collection<AggregatedMetric<?>>> registeredMetrics,
      final String name,
      final Metrics metrics
  ) {
    registeredMetrics.putIfAbsent(name, new LinkedList<>());
    final AggregatedMetric<BigInteger> registered = new AggregatedMetric<>(
        BigInteger.class,
        BigInteger::max,
        BigInteger.ZERO,
        new Interval(interval)
    );
    registeredMetrics.get(name).add(registered);
    final MetricName metricName = metrics.metricName(name + "-max", KSQL_ROCKSDB_METRICS_GROUP);
    metrics.addMetric(metricName, (Gauge<BigInteger>) (c, t) -> registered.getValue());
  }

  static final class AggregatedMetric<T> {
    private final Class<T> clazz;
    private final BinaryOperator<T> aggregator;
    private final T identity;
    private final Interval interval;
    private final Map<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();
    private volatile T value;

    private AggregatedMetric(
        final Class<T> clazz,
        final BinaryOperator<T> aggregator,
        final T identity,
        final Interval interval
    ) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
      this.identity = Objects.requireNonNull(identity, "identity");
      this.value = identity;
      this.interval = interval;
    }

    private void add(final KafkaMetric metric) {
      metrics.put(metric.metricName(), metric);
    }

    private void remove(final MetricName name) {
      metrics.remove(name);
    }

    private T getValue() {
      if (interval.check()) {
        value = update();
      }
      return value;
    }

    private T update() {
      T current = identity;
      for (final KafkaMetric metric : metrics.values()) {
        final Object value = metric.metricValue();
        if (!clazz.isInstance(value)) {
          LOGGER.debug(
              "Skipping metric update due to unexpected value type returned by {}",
              metric.metricName().toString()
          );
          return identity;
        }
        current = aggregator.apply(current, clazz.cast(value));
      }
      return current;
    }
  }

  static final class Interval {
    private final int intervalSeconds;
    private final AtomicReference<Instant> last;
    private final Supplier<Instant> clock;

    private Interval(final int intervalSeconds) {
      this(intervalSeconds, Instant::now);
    }

    Interval(final int intervalSeconds, final Supplier<Instant> clock) {
      this.intervalSeconds = intervalSeconds;
      this.clock = Objects.requireNonNull(clock, "clock");
      this.last = new AtomicReference<>(Instant.EPOCH);
    }

    boolean check() {
      final Instant now = clock.get();
      final Instant previous = last.getAndAccumulate(
          now,
          (l, n) -> n.isAfter(l.plusSeconds(intervalSeconds)) ? n : l
      );
      // Return true if the call to getAndAccumulate changed the value
      return last.get().isAfter(previous);
    }
  }

  private static void configureShared(final AbstractConfig config, final Metrics metrics) {
    synchronized (lock) {
      if (registeredMetrics != null) {
        return;
      }
      final int interval = config.getInt(UPDATE_INTERVAL_CONFIG);
      final Map<String, Collection<AggregatedMetric<?>>> builder = new HashMap<>();
      registerAll(interval, builder, metrics);
      registeredMetrics = ImmutableMap.copyOf(
          builder.entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> ImmutableList.copyOf(e.getValue())))
      );
    }
  }

  private static void registerAll(
      final int interval,
      final Map<String, Collection<AggregatedMetric<?>>> builder,
      final Metrics metrics
  ) {
    registerBigIntTotal(interval, builder, NUMBER_OF_RUNNING_COMPACTIONS, metrics);
    registerBigIntTotal(interval, builder, BLOCK_CACHE_USAGE, metrics);
    registerBigIntMax(interval, builder, BLOCK_CACHE_USAGE, metrics);
    registerBigIntTotal(interval, builder, BLOCK_CACHE_PINNED_USAGE, metrics);
    registerBigIntMax(interval, builder, BLOCK_CACHE_PINNED_USAGE, metrics);
    registerBigIntTotal(interval, builder, ESTIMATE_NUM_KEYS, metrics);
    registerBigIntTotal(interval, builder, ESTIMATE_TABLE_READERS_MEM, metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE, metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES, metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_DELETES_ACTIVE_MEMTABLE ,metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES, metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_IMMUTABLE_MEMTABLES, metrics);
    registerBigIntTotal(interval, builder, CURRENT_SIZE_OF_ACTIVE_MEMTABLE, metrics);
    registerBigIntTotal(interval, builder, CURRENT_SIZE_OF_ALL_MEMTABLES, metrics);
    registerBigIntTotal(interval, builder, SIZE_OF_ALL_MEMTABLES, metrics);
    registerBigIntTotal(interval, builder, MEMTABLE_FLUSH_PENDING, metrics);
    registerBigIntTotal(interval, builder, NUMBER_OF_RUNNING_FLUSHES, metrics);
    registerBigIntTotal(interval, builder, COMPACTION_PENDING, metrics);
    registerBigIntTotal(interval, builder, ESTIMATED_BYTES_OF_PENDING_COMPACTION, metrics);
    registerBigIntTotal(interval, builder, TOTAL_SST_FILES_SIZE, metrics);
    registerBigIntTotal(interval, builder, LIVE_SST_FILES_SIZE, metrics);
  }
}
