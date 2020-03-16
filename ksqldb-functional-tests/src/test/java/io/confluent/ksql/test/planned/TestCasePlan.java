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
import io.confluent.ksql.test.model.RecordNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TestCasePlan {
  private final TestCaseSpecNode specNode;
  private final TestCasePlanNode planNode;
  private final String topology;

  TestCasePlan(
      final String version,
      final long timestamp,
      final List<KsqlPlan> planNode,
      final String topology,
      final Map<String, String> schemas,
      final Map<String, String> configs,
      final List<RecordNode> inputs,
      final List<RecordNode> outputs
  ) {
    this(
        new TestCaseSpecNode(version, timestamp, schemas, inputs, outputs),
        new TestCasePlanNode(planNode, configs),
        topology
    );
  }

  TestCasePlan(
      final TestCaseSpecNode specNode,
      final TestCasePlanNode planNode,
      final String topology
  ) {
    this.specNode = Objects.requireNonNull(specNode, "spec");
    this.planNode = Objects.requireNonNull(planNode, "plan");
    this.topology = Objects.requireNonNull(topology, "topology");
  }

  public List<KsqlPlan> getPlan() {
    return planNode.getPlan();
  }

  public long getTimestamp() {
    return specNode.getTimestamp();
  }

  public Map<String, String> getConfigs() {
    return planNode.getConfigs();
  }

  public Map<String, String> getSchemas() {
    return specNode.getSchemas();
  }

  public String getTopology() {
    return topology;
  }

  public String getVersion() {
    return specNode.getVersion();
  }

  TestCaseSpecNode getSpecNode() {
    return specNode;
  }

  TestCasePlanNode getPlanNode() {
    return planNode;
  }

  public List<RecordNode> getInputs() {
    return specNode.getInputs();
  }

  public List<RecordNode> getOutputs() {
    return specNode.getOutputs();
  }
}
