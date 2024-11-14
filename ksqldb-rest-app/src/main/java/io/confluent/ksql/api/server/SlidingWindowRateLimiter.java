/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.server;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.util.KsqlPreconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SlidingWindowRateLimiter keeps a log of timestamps and the size for each response returned by
 * queries. When a response comes, we first pop all outdated timestamps outside of past hour
 * before appending the new response time and size to the log. Then we decide whether this response
 * should be processed depending on whether the log size has exceeded the throttleLimit.
 * Many rate limiters require you to ask for access before it's granted whereas this method always
 * records access (post-facto) but asks that you check first via allow if previous calls put you in
 * debt. This is due to not knowing the size of the response upfront.
 */

public class SlidingWindowRateLimiter {

  private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowRateLimiter.class);
  private static long NUM_BYTES_IN_ONE_MEGABYTE = 1 * 1024 * 1024;

  /**
   * The log of all the responses returned in the past hour.
   * It is a Queue ofPairs of (timestamp in milliseconds, response size in Bytes).
   */
  private final Queue<Pair<Long, Long>> responseSizesLog;

  /**
   * Throttle limit measured in Bytes.
   */
  private final long throttleLimit;

  /**
   * Window size over which the throttle is supposed to be enforced measured in milliseconds.
   */
  private final long slidingWindowSizeMs;
  private final Sensor rejectSensor;

  /**
   * Aggregate of query response sizes in the past hour
   */
  private volatile long numBytesInWindow;

  public SlidingWindowRateLimiter(
      final int requestLimitInMB,
      final long slidingWindowSizeMs,
      final String metricNamespace,
      final Metrics metrics,
      final Map<String, String> metricsTags) {

    checkArgument(requestLimitInMB >= 0,
            "Query bandwidth limit can't be negative.");
    checkArgument(slidingWindowSizeMs >= 0,
            "Query throttle window size can't be negative");

    this.throttleLimit = (long) requestLimitInMB * NUM_BYTES_IN_ONE_MEGABYTE;
    this.slidingWindowSizeMs = slidingWindowSizeMs;
    this.responseSizesLog = new LinkedList<>();
    this.numBytesInWindow = 0;

    metrics.addMetric(
        new MetricName(
            metricNamespace + "-bandwidth-limit-remaining",
            ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "limits",
            "The current value of the bandwidth limiter",
            metricsTags
        ),
        (metricConfig, l) -> throttleLimit - numBytesInWindow
    );

    this.rejectSensor = metrics.sensor("bandwidth-limit-rejects");
    rejectSensor.add(
        new MetricName(
            metricNamespace + "-bandwidth-limit-reject-count",
            ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "limits",
            "The number of requests rejected by this limiter",
            metricsTags
        ),
        new CumulativeCount()
    );
  }

  /**
   * Checks if queries have returned more than the throttleLimit in the past hour.
   * Throws a KsqlException is the limit has been breached
   * @throws KsqlException Exception that the throttle limit has been reached for queries
   */
  public synchronized void allow(final KsqlQueryType ksqlQueryType) throws KsqlException {
    this.allow(ksqlQueryType, Time.SYSTEM.milliseconds());
  }

  @VisibleForTesting
  protected synchronized void allow(final KsqlQueryType ksqlQueryType, final long timestamp)
      throws KsqlException {
    checkArgument(timestamp >= 0,
            "Timestamp can't be negative.");

    while (!responseSizesLog.isEmpty()
            && timestamp - responseSizesLog.peek().left >= slidingWindowSizeMs) {
      this.numBytesInWindow -= responseSizesLog.poll().right;
    }
    if (this.numBytesInWindow > throttleLimit) {
      LOG.warn("Hit bandwidth rate limit of " + throttleLimit + "B with use of "
          + numBytesInWindow + "B");
      rejectSensor.record();
      throw new KsqlApiException("Host is at bandwidth rate limit for "
          + ksqlQueryType.toString().toLowerCase() + " queries.",
          ERROR_CODE_BAD_REQUEST);
    }
  }

  /**
   * Adds the responseSizeInBytes and its timestamp to the queue of all response sizes
   * in the past hour.
   * @param responseSizeInBytes query response size measured in Bytes
   */
  public synchronized void add(final long responseSizeInBytes) {
    add(Time.SYSTEM.milliseconds(), responseSizeInBytes);
  }

  @VisibleForTesting
  protected synchronized void add(final long timestamp, final long responseSizeInBytes) {
    checkArgument(timestamp >= 0,
            "Timestamp can't be negative.");
    checkArgument(responseSizeInBytes >= 0,
            "Response size can't be negative.");

    responseSizesLog.add(new Pair<>(timestamp, responseSizeInBytes));
    this.numBytesInWindow += responseSizeInBytes;
  }
}