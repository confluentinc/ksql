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
import static io.confluent.ksql.metrics.StreamsErrorCollector.notifyApplicationClose;
import static io.confluent.ksql.metrics.StreamsErrorCollector.recordError;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.metrics.TopicSensors.Stat;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.After;
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

  @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  @After
  public void tearDown() {
    while (appCounter > 0) {
      StreamsErrorCollector.notifyApplicationClose(buildApplicationId(appCounter));
      appCounter--;
    }
  }

  @Test
  public void shouldCountStreamsErrors() {
    // When:
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> recordError(applicationId, TOPIC_NAME));

    // Then:
    assertThat(
        MetricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES, true),
        equalTo(1.0 * nmsgs));
  }

  @Test
  public void shouldComputeErrorRate() {
    // When:
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> recordError(applicationId, TOPIC_NAME));

    // Then:
    assertThat(
        MetricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES_PER_SEC, true),
        greaterThan(0.0));
  }

  @Test
  public void shouldComputeTopicLevelErrorStats() {
    // Given:
    final String otherTopic = "other-topic";
    final int nmsgs = 3;

    // When:
    IntStream.range(0, nmsgs).forEach(i -> recordError(applicationId, TOPIC_NAME));
    IntStream.range(0, nmsgs + 1).forEach(i -> recordError(applicationId, otherTopic));

    // Then:
    final Collection<Stat> stats = MetricCollectors.getStatsFor(TOPIC_NAME, true);
    final Optional<Stat> stat = stats.stream().filter((c) -> c.name().equals(CONSUMER_FAILED_MESSAGES)).findFirst();
    assertThat(stat.get().getValue(), equalTo(nmsgs * 1.0));
    final Collection<Stat> otherStats = MetricCollectors.getStatsFor(otherTopic, true);
    final Optional<Stat> otherStat = otherStats.stream().filter((c) -> c.name().equals(CONSUMER_FAILED_MESSAGES)).findFirst();
    assertThat(otherStat.get().getValue(), equalTo((nmsgs + 1) * 1.0));
  }

  @Test
  public void shouldComputeIndependentErrorStatsForQuery() {
    // Given:
    final String otherAppId = buildApplicationId();
    final String otherTopicId = "other-topic-id";
    final int nmsgs = 3;
    IntStream.range(0, nmsgs).forEach(i -> recordError(applicationId, TOPIC_NAME));
    IntStream.range(0, nmsgs + 1).forEach(i -> recordError(otherAppId, otherTopicId));

    // When:
    notifyApplicationClose(otherAppId);

    // Then:
    assertThat(
        MetricCollectors.aggregateStat(CONSUMER_FAILED_MESSAGES, true),
        equalTo(nmsgs * 1.0));
  }
}
