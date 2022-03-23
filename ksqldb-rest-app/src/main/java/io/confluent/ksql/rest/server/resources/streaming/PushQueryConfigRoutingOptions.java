/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.execution.scalablepush.PushRoutingOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class PushQueryConfigRoutingOptions implements PushRoutingOptions {

  private final KsqlConfig ksqlConfig;
  private final Map<String, ?> configOverrides;
  private final Map<String, ?> requestProperties;

  public PushQueryConfigRoutingOptions(
      final KsqlConfig ksqlConfig,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.configOverrides = Objects.requireNonNull(configOverrides, "configOverrides");
    this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties");
  }

  @Override
  public boolean getHasBeenForwarded() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING)) {
      return (Boolean) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING);
    }
    return KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DEFAULT;
  }

  @Override
  public boolean getIsDebugRequest() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_DEBUG_REQUEST)) {
      return (Boolean) requestProperties.get(KsqlRequestConfig.KSQL_DEBUG_REQUEST);
    }
    return KsqlRequestConfig.KSQL_DEBUG_REQUEST_DEFAULT;
  }

  @Override
  public Optional<String> getContinuationToken() {
    if (requestProperties.containsKey(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN)) {
      return Optional.of((String) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN));
    }
    return Optional.empty();
  }

  @Override
  public Optional<String> getCatchupConsumerGroup() {
    if (requestProperties.containsKey(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP)) {
      return Optional.of((String) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CATCHUP_CONSUMER_GROUP));
    }
    return Optional.empty();
  }

  @Override
  public boolean shouldOutputContinuationToken() {
    if (getHasBeenForwarded()) {
      return true;
    } else if (configOverrides.containsKey(
        KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED)) {
      return (Boolean) configOverrides.get(
          KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED);
    }
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED);
  }

  @Override
  public boolean alosEnabled() {
    if (configOverrides.containsKey(KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED)) {
      return (Boolean) configOverrides.get(KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED);
    }
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ALOS_ENABLED);
  }
}
