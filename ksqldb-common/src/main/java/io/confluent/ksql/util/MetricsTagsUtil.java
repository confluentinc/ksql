/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.util;

import java.util.HashMap;
import java.util.Map;

public final class MetricsTagsUtil {

  private MetricsTagsUtil() {}

  public static Map<String, String> getCustomMetricsTagsForQuery(
      final String id,
      final KsqlConfig config
  ) {
    return getCustomMetricsTagsForQuery(
        id,
        config.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS));
  }

  public static Map<String, String> getCustomMetricsTagsForQuery(
      final String id,
      final Map<String, String> metricsTags
  ) {
    final Map<String, String> customMetricsTags =
        new HashMap<>(metricsTags);
    customMetricsTags.put("query_id", id);
    return customMetricsTags;
  }
}
