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

import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

public final class ServiceInfo {

  private final String serviceId;
  private final Map<String, String> customMetricsTags;

  /**
   * Create an object to be passed from the KSQL context down to the KSQL engine.
   */
  public static ServiceInfo create(final KsqlConfig ksqlConfig) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig cannot be null.");

    final String serviceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final Map<String, String> customMetricsTags =
        ksqlConfig.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS);

    return new ServiceInfo(serviceId, customMetricsTags);
  }

  private ServiceInfo(
      final String serviceId,
      final Map<String, String> customMetricsTags
  ) {
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.customMetricsTags = Objects.requireNonNull(customMetricsTags, "customMetricsTags");
  }

  public String serviceId() {
    return serviceId;
  }

  public Map<String, String> customMetricsTags() {
    return customMetricsTags;
  }
}
