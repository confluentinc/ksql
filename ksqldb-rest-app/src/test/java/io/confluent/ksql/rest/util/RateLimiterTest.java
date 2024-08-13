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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

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
public class RateLimiterTest {

  public static final String METRIC_NAMESPACE = "test";

  @Test
  public void shouldSucceedUnderLimit() {
    final Metrics metrics = new Metrics();
    final Map<String, String> tags = Collections.emptyMap();
    // It doesn't look like the underlying guava rate limiter has a way to control time, so we're
    // just going to have to hope that these tests reliably run in under a second.
    final RateLimiter limiter = new RateLimiter(1, METRIC_NAMESPACE, metrics, tags);

    assertThat(getReject(metrics, tags), is(0.0));

    limiter.checkLimit();

    assertThat(getReject(metrics, tags), is(0.0));
  }

  @Test
  public void shouldError_atLimit() {
    final Metrics metrics = new Metrics();
    final Map<String, String> tags = Collections.emptyMap();
    // It doesn't look like the underlying guava rate limiter has a way to control time, so we're
    // just going to have to hope that these tests reliably run in under a second.
    final RateLimiter limiter = new RateLimiter(1, METRIC_NAMESPACE, metrics, tags);

    assertThat(getReject(metrics, tags), is(0.0));

    limiter.checkLimit();
    final KsqlException ksqlException =
        assertThrows(KsqlException.class, limiter::checkLimit);

    assertThat(
        ksqlException.getMessage(),
        is("Host is at rate limit for pull queries. Currently set to 1.0 qps.")
    );
    assertThat(getReject(metrics, tags), is(1.0));
  }

  private double getReject(final Metrics metrics, final Map<String, String> tags) {
    final MetricName rejectMetricName = new MetricName(
        METRIC_NAMESPACE + "-rate-limit-reject-count",
        "_confluent-ksql-limits",
        "The number of requests rejected by this limiter",
        tags
    );
    final KafkaMetric rejectMetric = metrics.metrics().get(rejectMetricName);
    return (double) rejectMetric.metricValue();
  }
}
