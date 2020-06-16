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

import io.confluent.ksql.query.ErrorStateListener;
import io.confluent.ksql.query.QueryError;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;

public class QueryStateListener implements ErrorStateListener {
  private static final String NO_ERROR = "NO_ERROR";

  private final Metrics metrics;
  private final MetricName stateMetricName;
  private final MetricName errorMetricName;

  private volatile String state = "-";
  private volatile String error = NO_ERROR;

  QueryStateListener(
      final Metrics metrics,
      final String groupPrefix,
      final String queryApplicationId
  ) {
    Objects.requireNonNull(groupPrefix, "groupPrefix");
    Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null.");

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
    if (newState != State.ERROR) {
      this.error = NO_ERROR;
    }
  }

  @Override
  public void onError(final QueryError error) {
    this.error = error.getType().name();
  }

  public void close() {
    metrics.removeMetric(stateMetricName);
    metrics.removeMetric(errorMetricName);
  }
}
