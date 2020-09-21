/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.physical.pull.operators;

import io.confluent.ksql.planner.plan.FilterNode;
import java.util.List;
import java.util.Objects;

public class SelectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final FilterNode logicalNode;
  private AbstractPhysicalOperator child;

  public SelectOperator(final FilterNode logicalNode) {
    this.logicalNode = Objects.requireNonNull(logicalNode);
  }

  @Override
  public void open() {
    child.open();
  }

  @Override
  public Object next() {
    return child.next();
  }

  @Override
  public void close() {

  }

  @Override
  public void addChild(final AbstractPhysicalOperator child) {
    if (this.child != null) {
      throw new UnsupportedOperationException("The select operator already has a child.");
    }
    Objects.requireNonNull(child, "child");
    this.child = child;
  }

  @Override
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
