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

package io.confluent.ksql.test.tools;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.test.model.SchemaNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TopologyAndConfigs {

  private final Optional<List<KsqlPlan>> plan;
  private final String topology;
  private final Map<String, SchemaNode> schemas;
  private final Map<String, String> configs;

  public TopologyAndConfigs(
      final Optional<List<KsqlPlan>> plan,
      final String topology,
      final Map<String, SchemaNode> schemas,
      final Map<String, String> configs
  ) {
    this.plan = Objects.requireNonNull(plan, "plan");
    this.topology = Objects.requireNonNull(topology, "topology");
    this.schemas = ImmutableMap.copyOf(Objects.requireNonNull(schemas, "schemas"));
    // cannot use ImmutableMap, because we need to handle `null`
    this.configs = new HashMap<>(Objects.requireNonNull(configs, "configs"));
  }

  public String getTopology() {
    return topology;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "schemas is ImmutableMap")
  public Map<String, SchemaNode> getSchemas() {
    return schemas;
  }

  public Map<String, String> getConfigs() {
    return Collections.unmodifiableMap(configs);
  }

  public Optional<List<KsqlPlan>> getPlan() {
    return plan;
  }
}
