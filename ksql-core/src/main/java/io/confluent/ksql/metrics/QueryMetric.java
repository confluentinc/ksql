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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryMetric {
  private final Metrics metrics;
  private static final Time time = Time.SYSTEM;

  public QueryMetric(String queryDescription, long serverId, List<MetricsReporter> reporters) {
    Map<String, String> metricTags = new LinkedHashMap<>();
    metricTags.put("query-description", queryDescription);
    metricTags.put("server-id", "Server-" + serverId);

    MetricConfig metricConfig = new MetricConfig().tags(metricTags);
    this.metrics = new Metrics(metricConfig, reporters, time);
  }

  public MetricName metricName(String name, String group, String description) {
    return metrics.metricName(name, group, description);
  }

  public void addMeasurableMetric(MetricName metricName, Measurable measurable) {
    metrics.addMetric(metricName, measurable);
  }

  public void removeMetric(MetricName metricName) {
    metrics.removeMetric(metricName);
  }

  public void removeSensor(String sensorName) {
    metrics.removeSensor(sensorName);
  }

  public Sensor getOrCreateSensor(String sensorName) {
    return metrics.sensor(sensorName);
  }

}
