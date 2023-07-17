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

import io.confluent.ksql.util.KsqlRateLimitException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;

@SuppressWarnings("UnstableApiUsage")
public class RateLimiter {

  private final com.google.common.util.concurrent.RateLimiter rateLimiter;
  private final Sensor rejectSensor;

  public RateLimiter(
      final double permitsPerSecond,
      final String metricNamespace,
      final Metrics metrics,
      final Map<String, String> metricsTags) {
    this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(permitsPerSecond);

    this.rejectSensor = metrics.sensor("pull-rate-limit-rejects");
    rejectSensor.add(
        new MetricName(
            metricNamespace + "-rate-limit-reject-count",
            ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "limits",
            "The number of requests rejected by this limiter",
            metricsTags
        ),
        new CumulativeCount()
    );
  }

  public void checkLimit() {
    if (!rateLimiter.tryAcquire()) {
      rejectSensor.record();
      throw new KsqlRateLimitException("Host is at rate limit for pull queries. Currently set to "
          + rateLimiter.getRate() + " qps.");
    }
  }
}
