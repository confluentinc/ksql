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

import io.confluent.ksql.engine.KsqlPlan;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TestCasePlan {
  private final TestCasePlanNode node;
  private final String topology;

  TestCasePlan(
      final String version,
      final long timestamp,
      final List<KsqlPlan> plan,
      final String topology,
      final Map<String, String> schemas,
      final Map<String, String> configs
  ) {
    this(new TestCasePlanNode(version, timestamp, plan, schemas, configs), topology);
  }

  TestCasePlan(final TestCasePlanNode node, final String topology) {
    this.node = Objects.requireNonNull(node, "node");
    this.topology = Objects.requireNonNull(topology, "topology");
  }

  public List<KsqlPlan> getPlan() {
    return node.getPlan();
  }

  public long getTimestamp() {
    return node.getTimestamp();
  }

  public Map<String, String> getConfigs() {
    return node.getConfigs();
  }

  public Map<String, String> getSchemas() {
    return node.getSchemas();
  }

  public String getTopology() {
    return topology;
  }

  public String getVersion() {
    return node.getVersion();
  }

  TestCasePlanNode getNode() {
    return node;
  }
}
