/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ConfigResponse;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigResource {
  private final Map<String, Object> visibleConfigs = new HashMap<>();

  public ConfigResource(final KsqlConfig ksqlConfig) {
    setVisibleConfigs(ksqlConfig);
  }

  private void setVisibleConfigs(final KsqlConfig ksqlConfig) {
    visibleConfigs.put(
        KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
        ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
    );
  }

  public EndpointResponse getConfigs(final List<String> requestedConfigs) {
    final Map<String, Object> configs = new HashMap<>();
    for (String config : requestedConfigs) {
      if (visibleConfigs.containsKey(config)) {
        configs.put(config, visibleConfigs.get(config));
      }
    }
    return EndpointResponse.ok(new ConfigResponse(configs));
  }

  public EndpointResponse getAllConfigs() {
    return EndpointResponse.ok(new ConfigResponse(visibleConfigs));
  }
}
