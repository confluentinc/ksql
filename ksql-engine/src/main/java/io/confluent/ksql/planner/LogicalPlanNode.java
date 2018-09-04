/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.planner;

import io.confluent.ksql.planner.plan.PlanNode;
import java.util.Objects;

public final class LogicalPlanNode {

  private final String statementText;
  private final PlanNode node;

  public LogicalPlanNode(final String statementText, final PlanNode node) {
    this.statementText = Objects.requireNonNull(statementText, "statementText");
    this.node = node;
  }

  public String getStatementText() {
    return statementText;
  }

  public PlanNode getNode() {
    return node;
  }
}
