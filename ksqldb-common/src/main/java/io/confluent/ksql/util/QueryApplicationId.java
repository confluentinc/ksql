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

package io.confluent.ksql.util;

import io.confluent.ksql.query.QueryId;

/**
 * Util to build query application ids.
 */
public final class QueryApplicationId {

  public static String PERSISTENT_QUERY_INDICATOR = "query";

  private QueryApplicationId() {
  }

  public static String buildSharedRuntimeId(
      final KsqlConfig config,
      final boolean persistent,
      final int sharedRuntimeIndex
  ) {
    final String queryAppId = buildInternalTopicPrefix(config, persistent) + sharedRuntimeIndex;
    if (persistent) {
      return queryAppId;
    } else {
      return addTimeSuffix(queryAppId);
    }
  }

  public static String build(
      final KsqlConfig config,
      final boolean persistent,
      final QueryId queryId
  ) {
    final String queryAppId = buildInternalTopicPrefix(config, persistent) + queryId;
    if (persistent) {
      return queryAppId;
    } else {
      return addTimeSuffix(queryAppId);
    }
  }

  public static String buildInternalTopicPrefix(
      final KsqlConfig config,
      final boolean persistent
  ) {
    final String serviceId = config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);

    final String configName = persistent
        ? KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG
        : KsqlConfig.KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG;

    final String queryPrefix = config.getString(configName);

    return buildInternalTopicPrefix(serviceId, queryPrefix);
  }

  public static String buildInternalTopicPrefix(
      final String serviceId,
      final String queryPrefix
  ) {
    return ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX
        + serviceId
        + queryPrefix;
  }

  private static String addTimeSuffix(final String original) {
    return String.format("%s_%d", original, System.currentTimeMillis());
  }
}
