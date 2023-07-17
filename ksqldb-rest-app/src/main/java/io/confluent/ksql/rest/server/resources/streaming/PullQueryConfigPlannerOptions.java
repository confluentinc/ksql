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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;

public class PullQueryConfigPlannerOptions implements QueryPlannerOptions {

  private final KsqlConfig ksqlConfig;
  private final ImmutableMap<String, ?> configOverrides;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PullQueryConfigPlannerOptions(
      final KsqlConfig ksqlConfig,
      final Map<String, ?> configOverrides
  ) {
    this.ksqlConfig = ksqlConfig;
    this.configOverrides = ImmutableMap.copyOf(configOverrides);
  }

  @Override
  public boolean getTableScansEnabled() {
    if (configOverrides.containsKey(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED)) {
      return (Boolean) configOverrides.get(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED);
    }
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED);
  }

  @Override
  public boolean getInterpreterEnabled() {
    if (configOverrides.containsKey(KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED)) {
      return (Boolean) configOverrides.get(KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED);
    }
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED);
  }
}
