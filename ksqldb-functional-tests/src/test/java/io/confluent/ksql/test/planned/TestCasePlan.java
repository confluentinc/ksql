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

import io.confluent.ksql.tools.test.model.TestLocation;
import java.util.Objects;

public final class TestCasePlan {

  private final TestLocation location;
  private final TestCaseSpecNode specNode;
  private final TestCasePlanNode planNode;
  private final String topology;

  TestCasePlan(
      final TestLocation location,
      final TestCaseSpecNode specNode,
      final TestCasePlanNode planNode,
      final String topology
  ) {
    this.location = Objects.requireNonNull(location, "location");
    this.specNode = Objects.requireNonNull(specNode, "spec");
    this.planNode = Objects.requireNonNull(planNode, "plan");
    this.topology = Objects.requireNonNull(topology, "topology");
  }

  public TestLocation getLocation() {
    return location;
  }

  public String getTopology() {
    return topology;
  }

  public TestCaseSpecNode getSpecNode() {
    return specNode;
  }

  public TestCasePlanNode getPlanNode() {
    return planNode;
  }
}
