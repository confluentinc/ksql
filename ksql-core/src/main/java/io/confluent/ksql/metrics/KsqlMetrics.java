/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.metrics;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.ArrayList;
import java.util.List;

public class KsqlMetrics {
  private static final String JMX_PREFIX = "ksql";
  private final long serverId;
  private final List<MetricsReporter> reporters;

  public KsqlMetrics(long serverId) {
    // TODO: make the reporter class configurable, e.g.
    // config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));

    this.serverId = serverId;
    this.reporters = reporters;
  }

  public QueryMetric addQueryMetric(String queryDescription) {
    return new QueryMetric(queryDescription, serverId, reporters);
  }

}
