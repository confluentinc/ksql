/*
 * Copyright 2019 Confluent Inc.
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

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;

/**
 * This interface provides a way for users to provide custom metrics that will be emitted alongside
 * the default KSQL engine JMX metrics.
 */
public interface KsqlMetricsExtension extends Configurable {

  @Override
  void configure(Map<String, ?> config);

  /**
   * Returns custom metrics to be emitted alongside the default KSQL engine JMX metrics.
   *
   * @return list of metrics
   */
  List<KsqlMetric> getCustomMetrics();
}
