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

package io.confluent.ksql;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.internal.KsqlMetricsExtension;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ServiceInfo {

  private final String serviceId;
  private final String metricsPrefix;
  private final ImmutableMap<String, String> customMetricsTags;
  private final Optional<KsqlMetricsExtension> metricsExtension;

  /**
   * Create ServiceInfo required by KSQL engine.
   *
   * @param ksqlConfig the server config.
   * @return new instance.
   */
  public static ServiceInfo create(final KsqlConfig ksqlConfig) {
    return create(ksqlConfig, "");
  }

  /**
   * Create ServiceInfo required by KSQL engine.
   *
   * @param ksqlConfig the server config.
   * @param metricsPrefix optional prefix for metrics group names. Default is empty string.
   * @return new instance.
   */
  public static ServiceInfo create(
      final KsqlConfig ksqlConfig,
      final String metricsPrefix
  ) {
    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final Map<String, String> customMetricsTags =
        ksqlConfig.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS);
    final Optional<KsqlMetricsExtension> metricsExtension = Optional.ofNullable(
        ksqlConfig.getConfiguredInstance(
            KsqlConfig.KSQL_CUSTOM_METRICS_EXTENSION,
            KsqlMetricsExtension.class
        ));

    return new ServiceInfo(serviceId, customMetricsTags, metricsExtension, metricsPrefix);
  }

  private ServiceInfo(
      final String serviceId,
      final Map<String, String> customMetricsTags,
      final Optional<KsqlMetricsExtension> metricsExtension,
      final String metricsPrefix
  ) {
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.customMetricsTags = ImmutableMap.copyOf(
        Objects.requireNonNull(customMetricsTags, "customMetricsTags")
    );
    this.metricsExtension = Objects.requireNonNull(metricsExtension, "metricsExtension");
    this.metricsPrefix = Objects.requireNonNull(metricsPrefix, "metricsPrefix");
  }

  public String serviceId() {
    return serviceId;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "customMetricsTags is ImmutableMap")
  public Map<String, String> customMetricsTags() {
    return customMetricsTags;
  }

  public Optional<KsqlMetricsExtension> metricsExtension() {
    return metricsExtension;
  }

  public String metricsPrefix() {
    return metricsPrefix;
  }
}
