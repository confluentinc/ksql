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

package io.confluent.ksql.execution.common.operators;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.streams.SqlPredicateFactory;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryFilterNode;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SelectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final QueryFilterNode logicalNode;
  private final ProcessingLogger logger;
  private final SqlPredicate predicate;

  private AbstractPhysicalOperator child;
  private KsqlTransformer<Object, Optional<GenericRow>> transformer;

  public SelectOperator(final QueryFilterNode logicalNode, final ProcessingLogger logger) {
    this(logicalNode, logger, SqlPredicate::new);
  }

  @VisibleForTesting
  SelectOperator(
      final QueryFilterNode logicalNode,
      final ProcessingLogger logger,
      final SqlPredicateFactory predicateFactory
  ) {
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.logger = Objects.requireNonNull(logger, "logger");
    this.predicate = predicateFactory.create(
        logicalNode.getRewrittenPredicate(),
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
    Optional<QueryRow> result = Optional.empty();
    while (result.equals(Optional.empty())) {
      final QueryRow row = (QueryRow) child.next();
      if (row == null) {
        return null;
      }
      if (row.getOffsetRange().isPresent()) {
        return row;
      }
      result = transformRow(row);
    }
    return result.get();
  }

  private Optional<QueryRow> transformRow(final QueryRow queryRow) {
    final GenericRow intermediate = PhysicalOperatorUtil.getIntermediateRow(
        queryRow, logicalNode.getAddAdditionalColumnsToIntermediateSchema());
    return transformer.transform(queryRow.key(), intermediate)
        .map(r -> QueryRowImpl.of(
            logicalNode.getIntermediateSchema(),
            queryRow.key(),
            queryRow.window(),
            r,
            queryRow.rowTime()
        ));
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
      throw new UnsupportedOperationException("The select operator already has a child.");
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
