/*
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

package io.confluent.ksql.internal;

import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class QueryStateListener implements StateListener {
  private final Metrics metrics;
  private final MetricName metricName;
  private volatile String state = "-";

  QueryStateListener(
      final Metrics metrics,
      final String queryApplicationId
  ) {
    Objects.requireNonNull(queryApplicationId, "queryApplicationId");
    this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null."); ;
    this.metricName = metrics.metricName(
        "query-status",
        "ksql-queries",
        "The current status of the given query.",
        Collections.singletonMap("status", queryApplicationId));

    this.metrics.addMetric(metricName, (Gauge<String>)(config, now) -> state);
  }

  @Override
  public void onChange(final State newState, final State oldState) {
    state = newState.toString();
  }

  public void close() {
    metrics.removeMetric(metricName);
  }
}
