/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metrics;

import io.confluent.ksql.metrics.TopicSensors.Stat;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.confluent.ksql.metrics.StreamsErrorCollector.CONSUMER_FAILED_MESSAGES;
import static io.confluent.ksql.metrics.StreamsErrorCollector.CONSUMER_FAILED_MESSAGES_PER_SEC;
import static io.confluent.ksql.metrics.StreamsErrorCollector.notifyApplicationClose;
import static io.confluent.ksql.metrics.StreamsErrorCollector.recordError;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

public class StreamsErrorCollectorTest {
  private final static String TOPIC_NAME = "test-topic";
  private final static String APPLICATION_ID_PREFIX = "test-app-id-";

  private static int nApps;

  private String applicationId;

  private String buildApplicationId(final int index) {
    return APPLICATION_ID_PREFIX + index;
  }

  private String buildApplicationId() {
    nApps++;
    return buildApplicationId(nApps);
  }

  @Before
  public void setUp() {
    applicationId = buildApplicationId();
  }

  @After
  public void tearDown() {
    while (nApps > 0) {
      StreamsErrorCollector.notifyApplicationClose(buildApplicationId(nApps));
      nApps--;
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
    final Map<String, Stat> stats = MetricCollectors.getStatsFor(TOPIC_NAME, true);
    assertThat(stats.keySet(), hasItem(CONSUMER_FAILED_MESSAGES));
    assertThat(stats.get(CONSUMER_FAILED_MESSAGES).getValue(), equalTo(nmsgs * 1.0));
    final Map<String, Stat> otherStats = MetricCollectors.getStatsFor(otherTopic, true);
    assertThat(otherStats.keySet(), hasItem(CONSUMER_FAILED_MESSAGES));
    assertThat(otherStats.get(CONSUMER_FAILED_MESSAGES).getValue(), equalTo((nmsgs + 1) * 1.0));
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
