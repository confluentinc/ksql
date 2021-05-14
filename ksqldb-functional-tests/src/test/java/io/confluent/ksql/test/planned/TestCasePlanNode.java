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

package io.confluent.ksql.test.planned;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.StreamsConfig;

class TestCasePlanNode {
  private final List<KsqlPlan> plan;
  private final Map<String, String> configs;

  public TestCasePlanNode(
      @JsonProperty("plan") final List<KsqlPlan> plan,
      @JsonProperty("configs") final Map<String, String> configs
  ) {
    this.plan = ImmutableList.copyOf(Objects.requireNonNull(plan, "plan"));
    this.configs = Collections
        .unmodifiableMap(filterConfigs(Objects.requireNonNull(configs, "configs")));
  }

  public List<KsqlPlan> getPlan() {
    return plan;
  }

  @SuppressWarnings("DefaultAnnotationParam") // ALWAYS overrides the default set on the mapper.
  @JsonInclude(content = Include.ALWAYS)
  public Map<String, String> getConfigs() {
    return configs;
  }

  private static Map<String, String> filterConfigs(final Map<String, String> configs) {
    final HashMap<String, String> copy = new HashMap<>(configs);
    // Exclude state dir as it differs on every run and should NOT use previous value:
    copy.keySet().remove(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG);
    return copy;
  }
}
