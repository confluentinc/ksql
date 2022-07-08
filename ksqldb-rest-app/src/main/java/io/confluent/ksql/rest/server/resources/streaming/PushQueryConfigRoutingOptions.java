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

import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.util.Map;
import java.util.Objects;

public class PushQueryConfigRoutingOptions implements PushRoutingOptions {

  private final Map<String, ?> requestProperties;

  public PushQueryConfigRoutingOptions(
      final Map<String, ?> requestProperties
  ) {
    this.requestProperties = Objects.requireNonNull(requestProperties, "requestProperties");
  }

  @Override
  public boolean getIsSkipForwardRequest() {
    if (requestProperties.containsKey(KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING)) {
      return (Boolean) requestProperties.get(
          KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING);
    }
    return KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_SKIP_FORWARDING_DEFAULT;
  }
}
