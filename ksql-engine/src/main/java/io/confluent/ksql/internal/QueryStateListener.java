/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.internal;

import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class QueryStateListener implements StateListener {
  private final Metrics metrics;
  private final MetricName metricName;
  private final QueryStateGauge queryStateGauge;

  public QueryStateListener(
      final Metrics metrics,
      final KafkaStreams kafkaStreams,
      final String queryApplicationId) {
    this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null."); ;
    final String metricGroupName = "ksql-queries";
    this.queryStateGauge = new QueryStateGauge(kafkaStreams);
    this.metricName = metrics.metricName(
        "query-status",
        metricGroupName,
        "The current status of the given query.",
        Collections.singletonMap("status", queryApplicationId));

    this.metrics.addMetric(metricName, queryStateGauge);
  }

  @Override
  public void onChange(final State newState, final State oldState) {
    queryStateGauge.setQueryState(newState);
  }

  public void close() {
    metrics.removeMetric(metricName);
  }
}
