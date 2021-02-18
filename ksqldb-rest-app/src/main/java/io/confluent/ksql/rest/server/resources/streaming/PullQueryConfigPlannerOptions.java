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

package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.planner.PullPlannerOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;

public class PullQueryConfigPlannerOptions implements PullPlannerOptions {

  private final KsqlConfig ksqlConfig;
  private final Map<String, ?> configOverrides;

  public PullQueryConfigPlannerOptions(final KsqlConfig ksqlConfig,
      final Map<String, ?> configOverrides) {
    this.ksqlConfig = ksqlConfig;
    this.configOverrides = configOverrides;
  }

  @Override
  public boolean getTableScansEnabled() {
    final boolean configured = ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED);
    if (configOverrides.containsKey(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED)) {
      final boolean override
          = (Boolean) configOverrides.get(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED);
      if (override && !configured) {
        throw new KsqlException("You can only disable table scans with an override, "
            + "not enable them.");
      }
      return override;
    }
    return configured;
  }
}
