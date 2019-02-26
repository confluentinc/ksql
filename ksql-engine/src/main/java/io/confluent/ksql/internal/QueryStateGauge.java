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

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.streams.KafkaStreams;

public class QueryStateGauge implements Gauge<String> {

  private String queryState;

  public QueryStateGauge(final KafkaStreams kafkaStreams) {
    queryState = kafkaStreams.state().toString();
  }

  @Override
  public String value(final MetricConfig metricConfig, final long l) {
    return queryState;
  }

  public void setQueryState(final KafkaStreams.State state) {
    this.queryState = state.toString();
  }
}
