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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;

/**
 * Limits the concurrency of the caller by checking against a limit and if it's exceeded, throwing
 * an exception. This wraps a semaphore, but provides some additional utilities such as a
 * decrementer class that ensures it's decremented just once since there are often multiple code
 * paths that can trigger it, such as an exception plus completion (if dealing with multiple
 * threads).
 */
public class ConcurrencyLimiter {

  private final Semaphore semaphore;
  private final int limit;
  private final String operationType;
  private final Sensor rejectSensor;

  public ConcurrencyLimiter(
      final int limit,
      final String operationType,
      final Metrics metrics,
      final Map<String, String> metricsTags) {

    this.semaphore = new Semaphore(limit);
    this.limit = limit;
    this.operationType = operationType;

    metrics.addMetric(
        new MetricName(
            operationType + "-concurrency-limit-remaining",
            ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "limits",
            "The current value of the concurrency limiter",
            metricsTags
        ),
        (metricConfig, l) -> semaphore.availablePermits()
    );

    this.rejectSensor = metrics.sensor("concurrency-limit-rejects");
    rejectSensor.add(
        new MetricName(
            operationType + "-concurrency-limit-reject-count",
            ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "limits",
            "The number of requests rejected by this limiter",
            metricsTags
        ),
        new CumulativeCount()
    );
  }

  public Decrementer increment() {
    if (!semaphore.tryAcquire()) {
      rejectSensor.record();
      throw new KsqlException(
          String.format("Host is at concurrency limit for %s queries. Currently set to %d maximum "
              + "concurrent operations.", operationType, limit));
    }
    return new Decrementer();
  }

  private void decrement() {
    semaphore.release();
  }

  @VisibleForTesting
  int getCount() {
    return limit - semaphore.availablePermits();
  }

  public class Decrementer {

    private final AtomicBoolean called = new AtomicBoolean(false);

    public void decrementAtMostOnce() {
      if (!called.getAndSet(true)) {
        ConcurrencyLimiter.this.decrement();
      }
    }
  }
}
