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

import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.plan.PullQueryFilterNode;
import java.util.List;
import java.util.Objects;

public class SelectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final PullQueryFilterNode logicalNode;
  private final ProcessingLogger logger;
  private final SqlPredicate predicate;

  private AbstractPhysicalOperator child;
  private KsqlTransformer<Object, GenericRow> transformer;
  private TableRow row;

  public SelectOperator(final PullQueryFilterNode logicalNode, final ProcessingLogger logger) {
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.logger = Objects.requireNonNull(logger, "logger");
    this.predicate = new SqlPredicate(
        logicalNode.getPredicate(),
        logicalNode.getCompiledWhereClause()
    );

  }

  @Override
  public void open() {
    transformer = predicate.getTransformer(logger);
    child.open();
  }

  @Override
  public Object next() {
    row = (TableRow)child.next();

    return transformer.transform(
        row.key(),
        row.value(),
        new PullProcessingContext(row.rowTime())
    );
  }

  @Override
  public void close() {

  }

  @Override
  public PlanNode getLogicalNode() {
    return logicalNode;
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
