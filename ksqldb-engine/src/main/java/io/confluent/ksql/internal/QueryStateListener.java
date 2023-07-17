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

package io.confluent.ksql.internal;

import com.google.common.base.Ticker;
import io.confluent.ksql.query.ErrorStateListener;
import io.confluent.ksql.query.QueryError;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;

public class QueryStateListener implements ErrorStateListener {
  public static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };

  private static final String NO_ERROR = "NO_ERROR";

  private final Metrics metrics;
  private final MetricName stateMetricName;
  private final MetricName errorMetricName;
  private final Ticker ticker;

  private volatile State currentState = State.CREATED;
  private volatile String state = "-";
  private volatile String error = NO_ERROR;
  private volatile long runningSince;

  QueryStateListener(
      final Metrics metrics,
      final String groupPrefix,
      final String queryApplicationId
  ) {
    this(metrics, groupPrefix, queryApplicationId, CURRENT_TIME_MILLIS_TICKER);
  }

  QueryStateListener(
      final Metrics metrics,
      final String groupPrefix,
      final String queryApplicationId,
      final Ticker ticker
  ) {
    Objects.requireNonNull(groupPrefix, "groupPrefix");
    Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null.");
    this.ticker = Objects.requireNonNull(ticker, "ticker");

    this.stateMetricName = metrics.metricName(
        "query-status",
        groupPrefix + "ksql-queries",
        "The current status of the given query.",
        Collections.singletonMap("status", queryApplicationId));

    errorMetricName = metrics.metricName(
        "error-status",
        groupPrefix + "ksql-queries",
        "The current error status of the given query, if the state is in ERROR state",
        Collections.singletonMap("status", queryApplicationId)
    );

    this.metrics.addMetric(stateMetricName, (Gauge<String>)(config, now) -> state);
    this.metrics.addMetric(errorMetricName, (Gauge<String>)(config, now) -> error);

  }

  @Override
  public void onChange(final State newState, final State oldState) {
    state = newState.toString();
    currentState = newState;

    if (newState != State.ERROR) {
      this.error = NO_ERROR;
    }

    if (newState.isRunningOrRebalancing() && !oldState.isRunningOrRebalancing()) {
      runningSince = ticker.read();
    }
  }

  @Override
  public void onError(final QueryError error) {
    this.error = error.getType().name();
  }

  public long uptime() {
    return (currentState.isRunningOrRebalancing() ? ticker.read() - runningSince : 0);
  }

  public void close() {
    metrics.removeMetric(stateMetricName);
    metrics.removeMetric(errorMetricName);
  }
}
