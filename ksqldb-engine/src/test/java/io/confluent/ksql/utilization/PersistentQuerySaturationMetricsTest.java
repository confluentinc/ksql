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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.MetricsReporter;
import io.confluent.ksql.internal.MetricsReporter.DataPoint;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class PersistentQuerySaturationMetricsTest {
  private static final Duration WINDOW = Duration.ofMinutes(10);
  private static final Duration SAMPLE_MARGIN = Duration.ofSeconds(15);
  private static final String APP_ID1 = "ping";
  private static final String APP_ID2 = "pong";
  private static final QueryId QUERY_ID1 = new QueryId("hootie");
  private static final QueryId QUERY_ID2 = new QueryId("hoo");
  private static final QueryId QUERY_ID3 = new QueryId("boom");
  private static final Map<String, String> CUSTOM_TAGS = ImmutableMap.of("logical_cluster_id", "logical-id");

  @Mock
  private MetricsReporter reporter;
  @Mock
  private Supplier<Instant> clock;
  @Mock
  private KafkaStreams kafkaStreams1;
  @Mock
  private KafkaStreams kafkaStreams2;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private PersistentQueryMetadata query2;
  @Mock
  private PersistentQueryMetadata query3;
  @Mock
  private KsqlEngine engine;
  @Captor
  private ArgumentCaptor<List<DataPoint>> reportedPoints;

  private PersistentQuerySaturationMetrics collector;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() {
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(query1, query2, query3));
    when(query1.getQueryId()).thenReturn(QUERY_ID1);
    when(query1.getKafkaStreams()).thenReturn(kafkaStreams1);
    when(query1.getQueryApplicationId()).thenReturn(APP_ID1);
    when(query2.getQueryId()).thenReturn(QUERY_ID2);
    when(query2.getKafkaStreams()).thenReturn(kafkaStreams2);
    when(query2.getQueryApplicationId()).thenReturn(APP_ID2);
    when(query3.getQueryId()).thenReturn(QUERY_ID3);
    when(query3.getKafkaStreams()).thenReturn(kafkaStreams1);
    when(query3.getQueryApplicationId()).thenReturn(APP_ID1);
    collector = new PersistentQuerySaturationMetrics(
        clock,
        engine,
        reporter,
        WINDOW,
        SAMPLE_MARGIN,
        CUSTOM_TAGS
    );
  }

  @Test
  public void shouldComputeSaturationForThread() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(1));
    collector.run();
    when(clock.get()).thenReturn(start.plus(WINDOW));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));

    // When:
    collector.run();

    // Then:
    final DataPoint point = verifyAndGetLatestDataPoint(
        "query-thread-saturation",
        ImmutableMap.of("thread-id", "t1")
    );
    assertThat((Double) point.getValue(), closeTo(.9, .01));
  }

  @Test
  public void shouldComputeSaturationForQuery() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2))
        .withThreadStartTime("t2", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t2", Duration.ofMinutes(2));
    collector.run();
    when(clock.get()).thenReturn(start.plus(WINDOW));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(3))
        .withThreadStartTime("t2", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t2", Duration.ofMinutes(7));

    // When:
    collector.run();

    // Then:
    final DataPoint point = verifyAndGetLatestDataPoint(
        "node-query-saturation",
        ImmutableMap.of("query-id", "hootie")
    );
    assertThat((Double) point.getValue(), closeTo(.9, .01));
  }

  @Test
  public void shouldComputeSaturationForNode() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));
    givenMetrics(kafkaStreams2)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));
    collector.run();
    when(clock.get()).thenReturn(start.plus(WINDOW));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(3));
    givenMetrics(kafkaStreams2)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(7));

    // When:
    collector.run();

    // Then:
    final DataPoint point = verifyAndGetLatestDataPoint(
        "max-node-query-saturation",
        Collections.emptyMap()
    );
    assertThat((Double) point.getValue(), closeTo(.9, .01));
  }

  @Test
  public void shouldIgnoreSamplesOutsideMargin() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));
    collector.run();
    when(clock.get()).thenReturn(start.plus(WINDOW.plus(SAMPLE_MARGIN.multipliedBy(2))));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(3));

    // When:
    collector.run();

    // Then:
    verifyNoDataPoints("max-node-query-saturation", Collections.emptyMap());
  }

  @Test
  public void shouldCountThreadBlockedToStart() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start)
        .withBlockedTime("t1", Duration.ofMinutes(0));
    collector.run();
    when(clock.get()).thenReturn(start.plus(Duration.ofMinutes(2)));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start)
        .withBlockedTime("t1", Duration.ofMinutes(0));

    // When:
    collector.run();

    // Then:
    final DataPoint point = verifyAndGetLatestDataPoint(
        "query-thread-saturation",
        ImmutableMap.of("thread-id", "t1")
    );
    assertThat((Double) point.getValue(), closeTo(.2, .01));
  }

  @Test
  public void shouldCleanupThreadMetric() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start)
        .withBlockedTime("t1", Duration.ofMinutes(0));
    collector.run();
    when(clock.get()).thenReturn(start.plus(Duration.ofMinutes(2)));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t2", start)
        .withBlockedTime("t2", Duration.ofMinutes(0));

    // When:
    collector.run();

    // Then:
    verify(reporter).cleanup("query-thread-saturation", ImmutableMap.of("thread-id", "t1"));
  }

  @Test
  public void shouldCleanupQueryMetricWhenRuntimeRemoved() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start)
        .withBlockedTime("t1", Duration.ofMinutes(0));
    collector.run();
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(query2));

    // When:
    collector.run();

    // Then:
    verify(reporter).cleanup("node-query-saturation", ImmutableMap.of("query-id", "hootie"));
  }

  @Test
  public void shouldAddPointsForQueriesSharingRuntimes() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));
    collector.run();
    when(clock.get()).thenReturn(start.plus(WINDOW));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(3));

    // When:
    collector.run();

    // Then:
    final DataPoint point = verifyAndGetLatestDataPoint(
        "node-query-saturation",
        ImmutableMap.of("query-id", "boom")
    );
    assertThat((Double) point.getValue(), closeTo(.9, .01));
  }

  @Test
  public void shouldCleanupPointsForQueriesFromSharedRuntimes() {
    // Given:
    final Instant start = Instant.now();
    when(clock.get()).thenReturn(start);
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(2));
    collector.run();
    when(engine.getPersistentQueries()).thenReturn(ImmutableList.of(query1, query2));
    when(clock.get()).thenReturn(start.plus(WINDOW));
    givenMetrics(kafkaStreams1)
        .withThreadStartTime("t1", start.minus(WINDOW.multipliedBy(2)))
        .withBlockedTime("t1", Duration.ofMinutes(3));

    // When:
    collector.run();

    // Then:
    verify(reporter).cleanup("node-query-saturation", ImmutableMap.of("query-id", "boom"));
    verify(reporter, times(0)).cleanup("node-query-saturation", ImmutableMap.of("query-id", "hoo"));
  }

  private List<DataPoint> verifyAndGetDataPoints(final String name, final Map<String, String> tag) {
    verify(reporter, atLeastOnce()).report(reportedPoints.capture());
    return reportedPoints.getAllValues().stream()
        .flatMap(List::stream)
        .filter(p -> p.getName().equals(name))
        .filter(p -> p.getTags().entrySet().containsAll(tag.entrySet()))
        .collect(Collectors.toList());
  }

  private DataPoint verifyAndGetLatestDataPoint(final String name, final Map<String, String> tag) {
    final List<DataPoint> found = verifyAndGetDataPoints(name, tag);
    assertThat(found, hasSize(greaterThan(0)));
    return found.get(found.size() - 1);
  }

  private void verifyNoDataPoints(final String name, final Map<String, String> tag) {
    verify(reporter, atLeastOnce()).report(reportedPoints.capture());
    final List<DataPoint> found = verifyAndGetDataPoints(name, tag);
    assertThat(found, hasSize(equalTo(0)));
  }

  private GivenMetrics givenMetrics(final KafkaStreams kafkaStreams) {
    return new GivenMetrics(kafkaStreams);
  }

  private static final class GivenMetrics {
    final Map<MetricName, Metric> metrics = new HashMap<>();

    private GivenMetrics(final KafkaStreams kafkaStreams) {
      when(kafkaStreams.metrics()).thenReturn((Map) metrics);
    }

    private GivenMetrics withBlockedTime(final String threadName, final Duration blockedTime) {
      final MetricName metricName = new MetricName(
          "blocked-time-ns-total",
          "stream-thread-metrics",
          "",
          ImmutableMap.of("thread-id", threadName)
      );
      metrics.put(
          metricName,
          new Metric() {
            @Override
            public MetricName metricName() {
              return metricName;
            }

            @Override
            public Object metricValue() {
              return (double) blockedTime.toNanos();
            }
          }
      );
      return this;
    }

    private GivenMetrics withThreadStartTime(final String threadName, final Instant startTime) {
      final MetricName metricName = new MetricName(
          "thread-start-time",
          "stream-thread-metrics",
          "",
          ImmutableMap.of("thread-id", threadName)
      );
      metrics.put(
          metricName,
          new Metric() {
            @Override
            public MetricName metricName() {
              return metricName;
            }

            @Override
            public Object metricValue() {
              return startTime.toEpochMilli();
            }
          }
      );
      return this;
    }
  }
}