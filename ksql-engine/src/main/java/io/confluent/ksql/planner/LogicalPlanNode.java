/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.planner;

import io.confluent.ksql.planner.plan.OutputNode;
import java.util.Objects;
import java.util.Optional;

public final class LogicalPlanNode {

  private final String statementText;
  private final Optional<OutputNode> node;
  private long offset;

  public LogicalPlanNode(final String statementText, final Optional<OutputNode> node) {
    this.statementText = Objects.requireNonNull(statementText, "statementText");
    this.node = Objects.requireNonNull(node, "node");
    this.offset = -1L;
  }

  public String getStatementText() {
    return statementText;
  }

  public Optional<OutputNode> getNode() {
    return node;
  }

  public void setOffset(final long offset) {
    this.offset = offset;
  }

  public long getOffset() {
    return offset;
  }
}
