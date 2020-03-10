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

package io.confluent.ksql.test.planned;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.engine.KsqlPlan;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class TestCasePlanNode {
  private final String version;
  private final long timestamp;
  private final List<KsqlPlan> plan;
  private final Map<String, String> schemas;
  private final Map<String, String> configs;

  public TestCasePlanNode(
      @JsonProperty("version") final String version,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("plan") final List<KsqlPlan> plan,
      @JsonProperty("schemas") final Map<String, String> schemas,
      @JsonProperty("configs") final Map<String, String> configs
  ) {
    this.version = Objects.requireNonNull(version, "version");
    this.timestamp = timestamp;
    this.plan = Objects.requireNonNull(plan, "plan");
    this.schemas = Objects.requireNonNull(schemas, "schemas");
    this.configs = Objects.requireNonNull(configs, "configs");
  }

  public List<KsqlPlan> getPlan() {
    return plan;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  public Map<String, String> getSchemas() {
    return schemas;
  }

  public String getVersion() {
    return version;
  }
}
