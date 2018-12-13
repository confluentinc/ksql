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

package io.confluent.ksql.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;

public class MetricsTestUtil {

  public static Metrics getMetrics() {
    final MetricConfig metricConfig = new MetricConfig()
        .samples(100)
        .timeWindow(
            1000,
            TimeUnit.MILLISECONDS
        );
    final List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter("io.confluent.ksql.metrics"));
    return new Metrics(metricConfig, reporters, new SystemTime());
  }

}
