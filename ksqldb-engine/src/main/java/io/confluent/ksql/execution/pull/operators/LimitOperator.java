/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.pull.operators;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.common.operators.UnaryPhysicalOperator;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryLimitNode;
import java.util.List;
import java.util.Objects;

public class LimitOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {
  private AbstractPhysicalOperator child;
  private final QueryLimitNode logicalNode;
  private final int limit;
  private int rowsReturned;

  public LimitOperator(final QueryLimitNode logicalNode) {
    this(logicalNode, logicalNode.getLimit());
  }

  @VisibleForTesting
  LimitOperator(final QueryLimitNode logicalNode,
                final int limit
  ) {
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.limit = limit;
    this.rowsReturned = 0;
  }

  @Override
  public void open() {
    child.open();
  }

  @Override
  public Object next() {
    if (rowsReturned >= limit) {
      return null;
    }
    final Object row = child.next();
    if (row == null) {
      return null;
    }
    rowsReturned += 1;
    return row;
  }

  @Override
  public void close() {
    child.close();
  }

  @Override
  public PlanNode getLogicalNode() {
    return logicalNode;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public void addChild(final AbstractPhysicalOperator child) {
    if (this.child != null) {
      throw new UnsupportedOperationException("The limit operator already has a child.");
    }
    Objects.requireNonNull(child, "child");
    this.child = child;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public AbstractPhysicalOperator getChild() {
    return child;
  }

  @Override
  public AbstractPhysicalOperator getChild(final int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    throw new UnsupportedOperationException();
  }
}
