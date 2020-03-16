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

package io.confluent.ksql.planner.plan;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

public final class ConfiguredKsqlPlan {
  private final KsqlPlan plan;
  private final Map<String, Object> overrides;
  private final KsqlConfig config;

  public static ConfiguredKsqlPlan of(
      final KsqlPlan plan,
      final Map<String, Object> overrides,
      final KsqlConfig config
  ) {
    return new ConfiguredKsqlPlan(plan, overrides, config);
  }

  private ConfiguredKsqlPlan(
      final KsqlPlan plan,
      final Map<String, Object> overrides,
      final KsqlConfig config
  ) {
    this.plan = Objects.requireNonNull(plan, "plan");
    this.overrides = ImmutableMap.copyOf(Objects.requireNonNull(overrides, "overrides"));
    this.config = Objects.requireNonNull(config, "config");
  }

  public KsqlPlan getPlan() {
    return plan;
  }

  public Map<String, Object> getOverrides() {
    return overrides;
  }

  public KsqlConfig getConfig() {
    return config;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConfiguredKsqlPlan that = (ConfiguredKsqlPlan) o;
    return Objects.equals(plan, that.plan)
        && Objects.equals(overrides, that.overrides)
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(plan, overrides, config);
  }
}
