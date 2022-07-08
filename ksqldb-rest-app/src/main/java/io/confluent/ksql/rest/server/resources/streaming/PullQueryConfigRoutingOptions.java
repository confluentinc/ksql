/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PullQueryConfigRoutingOptions implements RoutingOptions {

  private final KsqlConfig ksqlConfig;
  private final ImmutableMap<String, ?> configOverrides;
  private final ImmutableMap<String, ?> requestProperties;

  public PullQueryConfigRoutingOptions(
      final KsqlConfig ksqlConfig,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.configOverrides = ImmutableMap.copyOf(configOverrides);
    this.requestProperties = ImmutableMap.copyOf(
        Objects.requireNonNull(requestProperties, "requestProperties")
    );
  }

  private long getLong() {
    if (configOverrides.containsKey(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG)) {
      return (Long) configOverrides.get(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG);
    }
    return ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG);
  }

  private boolean getForwardedFlag() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING)) {
      return (Boolean) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING);
    }
    return KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING_DEFAULT;
  }

  public boolean getIsDebugRequest() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_DEBUG_REQUEST)) {
      return (Boolean) requestProperties.get(KsqlRequestConfig.KSQL_DEBUG_REQUEST);
    }
    return KsqlRequestConfig.KSQL_DEBUG_REQUEST_DEFAULT;
  }

  @Override
  public Set<Integer> getPartitions() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS)) {
      @SuppressWarnings("unchecked")
      final List<String> partitions = (List<String>) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_PARTITIONS);
      return partitions.stream()
          .map(partition -> {
            try {
              return Integer.parseInt(partition);
            } catch (NumberFormatException e) {
              throw new IllegalStateException("Internal request got a bad partition "
                                                  + partition);
            }
          }).collect(Collectors.toSet());
    }
    return Collections.emptySet();
  }

  @Override
  public long getMaxOffsetLagAllowed() {
    return getLong();
  }

  @Override
  public boolean getIsSkipForwardRequest() {
    return getForwardedFlag();
  }
}


