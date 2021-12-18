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

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConcurrencyLimiterTest {

  @Test
  public void shouldSucceedUnderLimit() {
    final Metrics metrics = new Metrics();
    final Map<String, String> tags = Collections.emptyMap();
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(
        1,
        "pull",
        metrics,
        tags
    );

    assertThat(getReject(metrics, tags), is(0.0));
    assertThat(getRemaining(metrics, tags), is(1.0));

    Decrementer decrementer = limiter.increment();

    assertThat(limiter.getCount(), is(1));
    assertThat(getReject(metrics, tags), is(0.0));
    assertThat(getRemaining(metrics, tags), is(0.0));

    decrementer.decrementAtMostOnce();
    decrementer.decrementAtMostOnce();

    assertThat(limiter.getCount(), is(0));
    assertThat(getReject(metrics, tags), is(0.0));
    assertThat(getRemaining(metrics, tags), is(1.0));

  }

  @Test
  public void shouldError_atLimit() {
    final Metrics metrics = new Metrics();
    final Map<String, String> tags = Collections.emptyMap();
    ConcurrencyLimiter limiter = new ConcurrencyLimiter(
        1,
        "pull",
        metrics,
        tags
    );

    assertThat(getReject(metrics, tags), is(0.0));
    assertThat(getRemaining(metrics, tags), is(1.0));

    Decrementer decrementer = limiter.increment();

    assertThat(getReject(metrics, tags), is(0.0));
    assertThat(getRemaining(metrics, tags), is(0.0));

    final Exception e = assertThrows(
        KsqlException.class,
        limiter::increment
    );

    assertThat(e.getMessage(), containsString("Host is at concurrency limit for pull queries."));
    assertThat(limiter.getCount(), is(1));
    assertThat(getReject(metrics, tags), is(1.0));
    assertThat(getRemaining(metrics, tags), is(0.0));

    decrementer.decrementAtMostOnce();

    assertThat(limiter.getCount(), is(0));
    assertThat(getReject(metrics, tags), is(1.0));
    assertThat(getRemaining(metrics, tags), is(1.0));
  }

  private double getReject(final Metrics metrics, final Map<String, String> tags) {
    final MetricName rejectMetricName = new MetricName(
        "pull-concurrency-limit-reject-count",
        "_confluent-ksql-limits",
        "The number of requests rejected by this limiter",
        tags
    );
    final KafkaMetric rejectMetric = metrics.metrics().get(rejectMetricName);
    return (double) rejectMetric.metricValue();
  }

  private double getRemaining(final Metrics metrics, final Map<String, String> tags) {
    final MetricName remainingMetricName = new MetricName(
        "pull-concurrency-limit-remaining",
        "_confluent-ksql-limits",
        "The current value of the concurrency limiter",
        tags
    );
    final KafkaMetric remainingMetric = metrics.metrics().get(remainingMetricName);
    return (double) remainingMetric.metricValue();
  }
}
