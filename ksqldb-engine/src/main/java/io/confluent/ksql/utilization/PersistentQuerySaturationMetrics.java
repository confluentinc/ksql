/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.utilization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.MetricsReporter;
import io.confluent.ksql.internal.MetricsReporter.DataPoint;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.Metric;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentQuerySaturationMetrics implements Runnable {
  private static final Logger LOGGER
      = LoggerFactory.getLogger(PersistentQuerySaturationMetrics.class);

  private static final String QUERY_SATURATION = "node-query-saturation";
  private static final String NODE_QUERY_SATURATION = "max-node-query-saturation";
  private static final String QUERY_THREAD_SATURATION = "query-thread-saturation";
  private static final String STREAMS_TOTAL_BLOCKED_TIME = "blocked-time-ns-total";
  private static final String STREAMS_THREAD_START_TIME = "thread-start-time";
  private static final String STREAMS_THREAD_METRICS_GROUP = "stream-thread-metrics";
  private static final String THREAD_ID = "thread-id";
  private static final String QUERY_ID = "query-id";

  private Map<String, String> customTags;
  private final Map<String, KafkaStreamsSaturation> perKafkaStreamsStats = new HashMap<>();
  private final KsqlExecutionContext engine;
  private final Supplier<Instant> time;
  private final MetricsReporter reporter;
  private final Duration window;
  private final Duration sampleMargin;

  public PersistentQuerySaturationMetrics(
      final KsqlEngine engine,
      final MetricsReporter reporter,
      final Duration window,
      final Duration sampleMargin, 
      final Map<String, String> customTags
  ) {
    this(Instant::now, engine, reporter, window, sampleMargin, customTags);
  }

  @VisibleForTesting
  PersistentQuerySaturationMetrics(
      final Supplier<Instant> time,
      final KsqlExecutionContext engine,
      final MetricsReporter reporter,
      final Duration window,
      final Duration sampleMargin,
      final Map<String, String> customTags
  ) {
    this.time =  Objects.requireNonNull(time, "time");
    this.engine = Objects.requireNonNull(engine, "engine");
    this.reporter = Objects.requireNonNull(reporter, "reporter");
    this.window = Objects.requireNonNull(window, "window");
    this.sampleMargin = Objects.requireNonNull(sampleMargin, "sampleMargin");
    this.customTags = Objects.requireNonNull(customTags, "customTags");
  }

  @Override
  public void run() {
    final Instant now = time.get();

    try {
      final Collection<PersistentQueryMetadata> queries = engine.getPersistentQueries();
      final Optional<Double> saturation = queries.stream()
          .collect(Collectors.groupingBy(PersistentQueryMetadata::getQueryApplicationId))
          .entrySet()
          .stream()
          .map(e -> measure(now, e.getKey(), e.getValue()))
          .max(PersistentQuerySaturationMetrics::compareSaturation)
          .orElse(Optional.of(0.0));
      saturation.ifPresent(s -> report(now, s));

      final Set<String> appIds = queries.stream()
          .map(PersistentQueryMetadata::getQueryApplicationId)
          .collect(Collectors.toSet());
      for (final String appId
          : Sets.difference(new HashSet<>(perKafkaStreamsStats.keySet()), appIds)) {
        perKafkaStreamsStats.get(appId).cleanup(reporter);
        perKafkaStreamsStats.remove(appId);
      }
    } catch (final RuntimeException e) {
      LOGGER.error("Error collecting saturation", e);
      throw e;
    }
  }

  private static int compareSaturation(final Optional<Double> a, final Optional<Double> b) {
    // A missing value means we could not compute saturation for some unit (thread, runtime, etc)
    // therefore, the result of the comparison is unknown.
    if (!a.isPresent()) {
      return 1;
    } else if (!b.isPresent()) {
      return -1;
    } else {
      return Double.compare(a.get(), b.get());
    }
  }

  private Optional<Double> measure(
      final Instant now,
      final String appId,
      final List<PersistentQueryMetadata> queryMetadata
  ) {
    final KafkaStreamsSaturation ksSaturation = perKafkaStreamsStats.computeIfAbsent(
        appId,
        k -> new KafkaStreamsSaturation(window, sampleMargin)
    );
    final Optional<KafkaStreams> kafkaStreams = queryMetadata.stream()
        .filter(q -> q.getKafkaStreams() != null)
        .map(PersistentQueryMetadata::getKafkaStreams)
        .findFirst();
    if (!kafkaStreams.isPresent()) {
      return Optional.of(0.0);
    }
    final List<QueryId> queryIds = queryMetadata.stream()
        .map(PersistentQueryMetadata::getQueryId)
        .collect(Collectors.toList());
    return ksSaturation.measure(now, queryIds, kafkaStreams.get(), reporter);
  }

  private void report(final Instant now, final double saturation) {
    LOGGER.info("reporting node-level saturation {}", saturation);
    reporter.report(
        ImmutableList.of(
            new DataPoint(
                now,
                NODE_QUERY_SATURATION,
                saturation,
                customTags
            )
        )
    );
  }
  
  private Map<String, String> getTags(final String key, final String value) {
    final Map<String, String> newTags = new HashMap<>(customTags);
    newTags.put(key, value);
    return newTags;
  }

  private final class KafkaStreamsSaturation {
    private final Set<QueryId> queryIds = new HashSet<>();
    private final Map<String, ThreadSaturation> perThreadSaturation = new HashMap<>();
    private final Duration window;
    private final Duration sampleMargin;

    private KafkaStreamsSaturation(
        final Duration window,
        final Duration sampleMargin
    ) {
      this.window = Objects.requireNonNull(window, "window");
      this.sampleMargin = Objects.requireNonNull(sampleMargin, "sampleMargin");
    }

    private void reportThreadSaturation(
        final Instant now,
        final double saturation,
        final String name,
        final MetricsReporter reporter
    ) {
      LOGGER.info("Reporting thread saturation {} for {}", saturation, name);
      reporter.report(ImmutableList.of(
          new DataPoint(
              now,
              QUERY_THREAD_SATURATION,
              saturation,
              getTags(THREAD_ID, name)
          )
      ));
    }

    private void reportQuerySaturation(
        final Instant now,
        final double saturation,
        final MetricsReporter reporter
    ) {
      for (final QueryId queryId : queryIds) {
        LOGGER.info("Reporting query saturation {} for {}", saturation, queryId);
        reporter.report(ImmutableList.of(
            new DataPoint(
                now,
                QUERY_SATURATION,
                saturation,
               getTags(QUERY_ID, queryId.toString())
            )
        ));
      }
    }

    private void updateQueryIds(final List<QueryId> newQueryIds, final MetricsReporter reporter) {
      for (final QueryId queryId : Sets.difference(queryIds, new HashSet<>(newQueryIds))) {
        reporter.cleanup(QUERY_SATURATION, ImmutableMap.of(QUERY_ID, queryId.toString()));
      }
      queryIds.clear();
      queryIds.addAll(newQueryIds);
    }

    private Map<String, Map<String, Metric>> metricsByThread(final KafkaStreams kafkaStreams) {
      return kafkaStreams.metrics().values().stream()
          .filter(m -> m.metricName().group().equals(STREAMS_THREAD_METRICS_GROUP))
          .collect(Collectors.groupingBy(
              m -> m.metricName().tags().get(THREAD_ID),
              Collectors.toMap(m -> m.metricName().name(), m -> m, (a, b) -> a)
          ));
    }

    private Optional<Double> measure(
        final Instant now,
        final List<QueryId> queryIds,
        final KafkaStreams kafkaStreams,
        final MetricsReporter reporter
    ) {
      updateQueryIds(queryIds, reporter);
      final Map<String, Map<String, Metric>> byThread = metricsByThread(kafkaStreams);
      Optional<Double> saturation = Optional.of(0.0);
      for (final Map.Entry<String, Map<String, Metric>> entry : byThread.entrySet()) {
        final String threadName = entry.getKey();
        final Map<String, Metric> metricsForThread = entry.getValue();
        if (!metricsForThread.containsKey(STREAMS_TOTAL_BLOCKED_TIME)
            || !metricsForThread.containsKey(STREAMS_THREAD_START_TIME)) {
          LOGGER.info("Missing required metrics for thread: {}", threadName);
          continue;
        }
        final double totalBlocked =
            (double) metricsForThread.get(STREAMS_TOTAL_BLOCKED_TIME).metricValue();
        final long startTime =
            (long) metricsForThread.get(STREAMS_THREAD_START_TIME).metricValue();
        final ThreadSaturation threadSaturation = perThreadSaturation.computeIfAbsent(
            threadName,
            k -> {
              LOGGER.debug("Adding saturation for new thread: {}", k);
              return new ThreadSaturation(threadName, startTime, window, sampleMargin);
            }
        );
        LOGGER.debug("Record thread {} sample {} {}", threadName, totalBlocked, startTime);
        final BlockedTimeSample blockedTimeSample = new BlockedTimeSample(now, totalBlocked);
        final Optional<Double> measured = threadSaturation.measure(now, blockedTimeSample);
        LOGGER.debug(
            "Measured value for thread {}: {}",
            threadName,
            (measured.map(Object::toString).orElse(""))
        );
        measured.ifPresent(m -> reportThreadSaturation(now, m, threadName, reporter));
        saturation = compareSaturation(saturation, measured) > 0 ? saturation : measured;
      }
      saturation.ifPresent(s -> reportQuerySaturation(now, s, reporter));
      for (final String threadName
          : Sets.difference(new HashSet<>(perThreadSaturation.keySet()), byThread.keySet())) {
        perThreadSaturation.remove(threadName);
        reporter.cleanup(QUERY_THREAD_SATURATION, ImmutableMap.of(THREAD_ID, threadName));
      }
      return saturation;
    }

    private void cleanup(final MetricsReporter reporter) {
      for (final String threadName : perThreadSaturation.keySet()) {
        reporter.cleanup(QUERY_THREAD_SATURATION, ImmutableMap.of(THREAD_ID, threadName));
      }
      for (final QueryId queryId : queryIds) {
        reporter.cleanup(QUERY_SATURATION, ImmutableMap.of(QUERY_ID, queryId.toString()));
      }
    }
  }

  private static final class ThreadSaturation {
    private final String threadName;
    private final List<BlockedTimeSample> samples = new LinkedList<>();
    private final Instant startTime;
    private final Duration window;
    private final Duration sampleMargin;

    private ThreadSaturation(
        final String threadName,
        final long startTime,
        final Duration window,
        final Duration sampleMargin
    ) {
      this.threadName = Objects.requireNonNull(threadName, "threadName");
      this.startTime = Instant.ofEpochMilli(startTime);
      this.window = Objects.requireNonNull(window, "window");
      this.sampleMargin = Objects.requireNonNull(sampleMargin, "sampleMargin");
    }

    private boolean inRange(final Instant time, final Instant start, final Instant end) {
      return time.isAfter(start) && time.isBefore(end);
    }

    private Optional<Double> measure(final Instant now, final BlockedTimeSample current) {
      final Instant windowStart = now.minus(window);
      final Instant earliest = now.minus(window.plus(sampleMargin));
      final Instant latest = now.minus(window.minus(sampleMargin));
      LOGGER.debug(
          "{}: record and measure with now {},  window {} ({} : {})",
          threadName,
          now,
          windowStart,
          earliest,
          latest
      );
      samples.add(current);
      samples.removeIf(s -> s.timestamp.isBefore(earliest));
      if (!inRange(samples.get(0).timestamp, earliest, latest) && !startTime.isAfter(windowStart)) {
        return Optional.empty();
      }
      final BlockedTimeSample startSample = samples.get(0);
      LOGGER.debug("{}: start sample {}", threadName, startSample);
      double blocked =
          Math.max(current.totalBlockedTime - startSample.totalBlockedTime, 0);
      Instant observedStart = samples.get(0).timestamp;
      if (startTime.isAfter(windowStart)) {
        LOGGER.debug("{}: start time {} is after window start", threadName, startTime);
        blocked += Duration.between(windowStart, startTime).toNanos();
        observedStart = windowStart;
      }

      final Duration duration = Duration.between(observedStart, current.timestamp);
      final double durationNs = duration.toNanos();
      return Optional.of((durationNs - Math.min(blocked, durationNs)) / durationNs);
    }
  }

  private static final class BlockedTimeSample {
    private final Instant timestamp;
    private final double totalBlockedTime;

    private BlockedTimeSample(final Instant timestamp, final double totalBlockedTime) {
      this.timestamp = timestamp;
      this.totalBlockedTime = totalBlockedTime;
    }

    @Override
    public String toString() {
      return "BlockedTimeSample{"
          + "timestamp=" + timestamp
          + ", totalBlockedTime=" + totalBlockedTime
          + '}';
    }
  }
}
