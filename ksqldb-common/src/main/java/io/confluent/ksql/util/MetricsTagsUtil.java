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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;

public final class MetricsTagsUtil {

  private MetricsTagsUtil() {}

  public static Map<String, String> getMetricsTagsWithQueryId(
      final String queryId,
      final Map<String, String> tags
  ) {
    if (queryId.isEmpty()) {
      return tags;
    }
    return addMetricTagToMap("query-id", queryId, tags);
  }

  public static Map<String, String> getMetricsTagsWithLoggerId(
      final String loggerId,
      final Map<String, String> tags
  ) {
    return addMetricTagToMap("logger-id", loggerId, tags);
  }

  private static Map<String, String> addMetricTagToMap(
      final String tagName,
      final String tagValue,
      final Map<String, String> tags
  ) {
    final Map<String, String> newMetricsTags =
        new HashMap<>(tags);
    newMetricsTags.put(tagName, tagValue);
    return ImmutableMap.copyOf(newMetricsTags);
  }
}
