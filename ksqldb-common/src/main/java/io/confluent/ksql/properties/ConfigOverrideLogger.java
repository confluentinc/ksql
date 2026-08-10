/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Logs each property override at a REST endpoint. Gated on
 * {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_LOG} (default off).
 *
 * <p>The message ({@code "Config overrides found"} / {@code "No Config overrides"}) identifies
 * the event; variable fields ({@code endpoint}, {@code property}, {@code inAllowlist}) attach
 * via log4j2 ThreadContext, so JSON layouts surface them as discrete indexable fields.
 * {@link CloseableThreadContext} clears the keys after each call so they don't leak across
 * requests on shared worker threads.
 *
 * <p>Property values are never logged — some keys (e.g. {@code sasl.jaas.config}) carry
 * credentials.
 */
public final class ConfigOverrideLogger {

  private static final Logger LOG = LogManager.getLogger(ConfigOverrideLogger.class);

  private static final String ENDPOINT = "endpoint";
  private static final String PROPERTY = "property";
  private static final String IN_ALLOWLIST = "inAllowlist";
  private static final String QUERY = "query";
  private static final String VALUE = "value";

  private static final String STREAMS = KsqlConfig.KSQL_STREAMS_PREFIX;
  private static final String STREAMS_CONSUMER = STREAMS + StreamsConfig.CONSUMER_PREFIX;
  private static final String STREAMS_PRODUCER = STREAMS + StreamsConfig.PRODUCER_PREFIX;

  /**
   * Range checks, keyed by property name. Each returns a description of how the value is out of
   * range, or {@link Optional#empty()} if it is fine. One check per property that needs one.
   */
  @SuppressWarnings("deprecation")
  private static final Map<String, Function<Object, Optional<String>>> RANGE_CHECKS =
      ImmutableMap.<String, Function<Object, Optional<String>>>builder()
      .put(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
          value -> between(value, 100, 60_000))
      .put(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
          value -> between(value, 1_000, 3_600_000))
      .put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, value -> between(value, -1, 300_000))
      .put(STREAMS + StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, value -> between(value, -1, 300_000))
      .put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, value -> between(value, 1_000, 600_000))
      .put(STREAMS + StreamsConfig.TASK_TIMEOUT_MS_CONFIG,
          value -> between(value, 1_000, 600_000))
      .put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, value -> between(value, 30_000, 900_000))
      .put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
          value -> between(value, 30_000, 900_000))
      .put(STREAMS + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
          value -> between(value, 30_000, 900_000))
      .put(STREAMS_CONSUMER + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
          value -> between(value, 30_000, 900_000))
      .put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value -> between(value, 1, 10_000))
      .put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
          value -> between(value, 1, 10_000))
      .put(STREAMS + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value -> between(value, 1, 10_000))
      .put(STREAMS_CONSUMER + ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
          value -> between(value, 1, 10_000))
      .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
          value -> between(value, 0, 2_097_152))
      .put(STREAMS + StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
          value -> between(value, 0, 2_097_152))
      .put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, value -> between(value, 1_024, 8_388_608))
      .put(STREAMS + ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
          value -> between(value, 1_024, 8_388_608))
      .put(STREAMS_PRODUCER + ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
          value -> between(value, 1_024, 8_388_608))
      .put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
          value -> between(value, 1_024, 8_388_608))
      .put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, value -> exactly(value, 1))
      .put(STREAMS + StreamsConfig.NUM_STREAM_THREADS_CONFIG, value -> exactly(value, 1))
      // The collect_set/collect_list limits are declared in ksqldb-engine, which this module
      // sits below, so their names cannot be referenced as constants from here.
      .put("ksql.functions.collect_set.limit", value -> between(value, 1, 1_000))
      .put("ksql.functions.collect_list.limit", value -> between(value, 1, 1_000))
      .put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, value -> between(value, 100, 30_000))
      .put(STREAMS + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
          value -> between(value, 100, 30_000))
      .put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
          value -> between(value, 1_048_576, 104_857_600))
      .put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
          value -> between(value, 1_048_576, 104_857_600))
      .put(STREAMS + ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
          value -> between(value, 1_048_576, 104_857_600))
      .put(STREAMS_CONSUMER + ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
          value -> between(value, 1_048_576, 104_857_600))
      .put(ProducerConfig.BATCH_SIZE_CONFIG, value -> between(value, 1_024, 1_048_576))
      .put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG,
          value -> between(value, 1_024, 1_048_576))
      .put(STREAMS + ProducerConfig.BATCH_SIZE_CONFIG, value -> between(value, 1_024, 1_048_576))
      .put(STREAMS_PRODUCER + ProducerConfig.BATCH_SIZE_CONFIG,
          value -> between(value, 1_024, 1_048_576))
      .put(StreamsConfig.REPLICATION_FACTOR_CONFIG, value -> exactly(value, 1))
      .put(STREAMS + StreamsConfig.REPLICATION_FACTOR_CONFIG, value -> exactly(value, 1))
      .put(KsqlConfig.KSQL_ASSERT_TOPIC_DEFAULT_TIMEOUT_MS,
          value -> between(value, 1_000, 60_000))
      .build();

  private static volatile boolean overridesLogEnabled = false;
  private static volatile boolean rangeValidationLogEnabled = false;
  private static volatile Set<String> allowlist = ImmutableSet.of();

  private ConfigOverrideLogger() {
  }

  public static void configure(final KsqlConfig config) {
    overridesLogEnabled = config.getBoolean(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG);
    rangeValidationLogEnabled = config.getBoolean(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_RANGE_VALIDATION_LOG_ENABLED);
    allowlist = ImmutableSet.copyOf(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
  }

  @VisibleForTesting
  public static void reset() {
    overridesLogEnabled = false;
    rangeValidationLogEnabled = false;
    allowlist = ImmutableSet.of();
  }

  public static void logOverrides(final String endpoint, final Map<String, Object> properties) {
    logOverrides(endpoint, Optional.empty(), properties);
  }

  public static void logOverrides(
          final String endpoint,
          final Optional<String> query,
          final Map<String, Object> properties) {
    if (!overridesLogEnabled) {
      return;
    }
    if (properties == null || properties.isEmpty()) {
      try (CloseableThreadContext.Instance ignored = CloseableThreadContext
          .put(ENDPOINT, endpoint)) {
        LOG.debug("No Config overrides");
      }
      return;
    }
    for (final String key : properties.keySet()) {
      final CloseableThreadContext.Instance context = CloseableThreadContext
          .put(ENDPOINT, endpoint)
          .put(PROPERTY, key)
          .put(IN_ALLOWLIST, String.valueOf(allowlist.contains(key)));
      query.ifPresent(id -> context.put(QUERY, id));

      try (CloseableThreadContext.Instance ignored = context) {
        LOG.info("Config overrides found");
      }
    }
  }

  /**
   * Logs a WARN for each override whose value is out of range.
   * @param properties Property overrides.
   */
  public static void logRangeViolations(
      final String endpoint,
      final Map<String, Object> properties
  ) {
    logRangeViolations(endpoint, Optional.empty(), properties);
  }

  /**
   * As {@link #logRangeViolations(String, Map)}, additionally naming the query the overrides
   * belong to. Only the command topic restore path knows a query id, since it is assigned when
   * the command is first written.
   */
  public static void logRangeViolations(
      final String endpoint,
      final Optional<String> query,
      final Map<String, Object> properties
  ) {
    if (!rangeValidationLogEnabled || properties == null) {
      return;
    }

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Function<Object, Optional<String>> check = RANGE_CHECKS.get(entry.getKey());
      if (check == null) {
        continue;
      }

      final Optional<String> violation = check.apply(entry.getValue());
      if (!violation.isPresent()) {
        continue;
      }

      final CloseableThreadContext.Instance context = CloseableThreadContext
          .put(ENDPOINT, endpoint)
          .put(PROPERTY, entry.getKey())
          .put(VALUE, String.valueOf(entry.getValue()));
      query.ifPresent(id -> context.put(QUERY, id));

      try (CloseableThreadContext.Instance ignored = context) {
        LOG.warn("Config override outside intended range: {}", violation.get());
      }
    }
  }

  /**
   * Checks an inclusive numeric range.
   */
  private static Optional<String> between(final Object value, final long min, final long max) {
    if (value == null) {
      return Optional.empty();
    }

    final long parsed = Long.parseLong(String.valueOf(value).trim());

    if (parsed < min || parsed > max) {
      return Optional.of("must be between " + min + " and " + max);
    }
    return Optional.empty();
  }

  /**
   * Reports anything other than {@code expected}.
   */
  private static Optional<String> exactly(final Object value, final long expected) {
    if (value == null) {
      return Optional.empty();
    }

    if (Long.parseLong(String.valueOf(value).trim()) != expected) {
      return Optional.of("must be " + expected);
    }
    return Optional.empty();
  }
}
