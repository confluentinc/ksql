/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metrics;

import static io.confluent.ksql.metrics.StreamsErrorCollector.CONSUMER_FAILED_MESSAGES;
import static io.confluent.ksql.metrics.StreamsErrorCollector.CONSUMER_FAILED_MESSAGES_PER_SEC;
import static io.confluent.ksql.test.util.OptionalMatchers.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

import io.confluent.ksql.metrics.TopicSensors.Stat;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class StreamsErrorCollectorTest {
  private final static String TOPIC_NAME = "test-topic";
  private final static String APPLICATION_ID_PREFIX = "test-app-id-";

  private static int appCounter;

  private String applicationId;

  private static String buildApplicationId(final int index) {
    return APPLICATION_ID_PREFIX + index;
  }

  private static String buildApplicationId() {
    appCounter++;
    return buildApplicationId(appCounter);
  }

  @Before
  public void setUp() {
    applicationId = buildApplicationId();
  }

  @Test
  public void shouldCountStreamsErrors() {
    final MetricCollectors metricCollectors = new MetricCollectors();
    final StreamsErrorCollector collector = StreamsErrorCollector.create(
        applicationId,
        metricCollectors
    );

    // When:
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> collector.recordError(TOPIC_NAME));

    // Then:
    assertThat(
        metricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES, true),
        equalTo(1.0 * nmsgs));
  }

  @Test
  public void shouldComputeErrorRate() {
    final MetricCollectors metricCollectors = new MetricCollectors();
    final StreamsErrorCollector collector = StreamsErrorCollector.create(
        applicationId,
        metricCollectors
    );

    // When:
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> collector.recordError(TOPIC_NAME));

    // Then:
    assertThat(
        metricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES_PER_SEC, true),
        greaterThan(0.0));
  }

  @Test
  public void shouldComputeTopicLevelErrorStats() {
    final MetricCollectors metricCollectors = new MetricCollectors();
    final StreamsErrorCollector collector = StreamsErrorCollector.create(
        applicationId,
        metricCollectors
    );

    // Given:
    final String otherTopic = "other-topic";
    final int nmsgs = 3;

    // When:
    IntStream.range(0, nmsgs).forEach(i -> collector.recordError(TOPIC_NAME));
    IntStream.range(0, nmsgs + 1).forEach(i -> collector.recordError(otherTopic));

    // Then:
    final Collection<Stat> stats = metricCollectors.getStatsFor(TOPIC_NAME, true);
    final Optional<Stat> stat = stats.stream().filter((c) -> c.name().equals(CONSUMER_FAILED_MESSAGES)).findFirst();
    assertThat(stat.get().getValue(), equalTo(nmsgs * 1.0));
    final Collection<Stat> otherStats = metricCollectors.getStatsFor(otherTopic, true);
    final Optional<Stat> otherStat = otherStats.stream().filter((c) -> c.name().equals(CONSUMER_FAILED_MESSAGES)).findFirst();
    assertThat(otherStat.get().getValue(), equalTo((nmsgs + 1) * 1.0));
  }

  @Test
  public void shouldComputeIndependentErrorStatsForQuery() {
    final MetricCollectors metricCollectors = new MetricCollectors();
    final StreamsErrorCollector collector = StreamsErrorCollector.create(
        applicationId,
        metricCollectors
    );

    final String otherAppId = buildApplicationId();
    final StreamsErrorCollector otherCollector = StreamsErrorCollector.create(
        otherAppId,
        metricCollectors
    );

    final String otherTopicId = "other-topic-id";
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> collector.recordError(TOPIC_NAME));
    IntStream.range(0, nmsgs + 1).forEach(i -> otherCollector.recordError(otherTopicId));

    // When:
    otherCollector.cleanup();

    // Then:
    final Collection<Stat> statsFor = metricCollectors.getStatsFor(TOPIC_NAME, true);
    assertThat(statsFor, hasSize(2));
    assertThat(getStat(statsFor, "consumer-failed-messages"), of(equalTo(nmsgs * 1.0)));
    assertThat(getStat(statsFor, "consumer-failed-messages-per-sec"), of(greaterThan(0.0)));

    final Collection<Stat> otherStatsFor = metricCollectors.getStatsFor(otherTopicId, true);
    assertThat(otherStatsFor, hasSize(0));

    assertThat(
        metricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES, true),
        equalTo(nmsgs * 1.0));
  }

  private Optional<Double> getStat(final Collection<Stat> statsFor, final String s) {
    return statsFor
        .stream()
        .filter(stat -> stat.name().equals(s))
        .map(Stat::getValue)
        .findFirst();
  }
}
