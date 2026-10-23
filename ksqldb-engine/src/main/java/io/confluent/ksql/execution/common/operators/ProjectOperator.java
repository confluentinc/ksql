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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory.SelectValueMapperFactorySupplier;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryProjectNode;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ProjectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final ProcessingLogger logger;
  private final SelectValueMapperFactorySupplier selectValueMapperFactorySupplier;
  private final QueryProjectNode logicalNode;

  private AbstractPhysicalOperator child;
  private KsqlTransformer<Object, GenericRow> transformer;

  public ProjectOperator(
      final ProcessingLogger logger,
      final QueryProjectNode logicalNode
  ) {
    this(
        logger,
        logicalNode,
        SelectValueMapperFactory::create
    );
  }

  @VisibleForTesting
  ProjectOperator(
      final ProcessingLogger logger,
      final QueryProjectNode logicalNode,
      final SelectValueMapperFactorySupplier selectValueMapperFactorySupplier
  ) {
    this.logger = Objects.requireNonNull(logger, "logger");
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.selectValueMapperFactorySupplier = selectValueMapperFactorySupplier;
  }

  @Override
  public void open() {
    child.open();

    if (logicalNode.getIsSelectStar()) {
      return;
    }

    final SelectValueMapper<Object> select = selectValueMapperFactorySupplier.create(
        logicalNode.getSelectExpressions(),
        logicalNode.getCompiledSelectExpressions()
    );

    transformer = select.getTransformer(logger);
  }

  @Override
  public Object next() {
    final QueryRow row = (QueryRow) child.next();
    if (row == null) {
      return null;
    }
    if (row.getOffsetRange().isPresent()) {
      return row;
    }

    final GenericRow intermediate = PhysicalOperatorUtil.getIntermediateRow(
        row, logicalNode.getAddAdditionalColumnsToIntermediateSchema());

    if (logicalNode.getIsSelectStar()) {
      return QueryRowImpl.of(logicalNode.getSchema(),
          GenericKey.genericKey(),
          Optional.empty(),
          GenericRow.fromList(createRowForSelectStar(intermediate)),
          row.rowTime());
    }

    final GenericRow mapped = transformer.transform(
        row.key(),
        intermediate);
    validateProjection(mapped, logicalNode.getSchema());

    return QueryRowImpl.of(logicalNode.getSchema(),
        GenericKey.genericKey(),
        Optional.empty(),
        GenericRow.fromList(mapped.values()),
        row.rowTime());
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
      throw new UnsupportedOperationException("The project operator already has a child.");
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

  private static void validateProjection(
      final GenericRow fullRow,
      final LogicalSchema schema
  ) {
    final int actual = fullRow.size();
    final int expected = schema.columns().size();
    if (actual != expected) {
      throw new IllegalStateException("Row column count mismatch."
                                          + " expected:" + expected
                                          + ", got:" + actual
      );
    }
  }

  // Optimization for select star, to avoid having to do code generation
  private List<?> createRowForSelectStar(final GenericRow intermediate) {
    final List<Object> rowList = new ArrayList<>();
    for (SelectExpression selectExpression : logicalNode.getSelectExpressions()) {
      final Optional<Column> column = logicalNode.getIntermediateSchema()
          .findValueColumn(selectExpression.getAlias());
      if (!column.isPresent()) {
        throw new IllegalStateException("Couldn't find alias in intermediate schema "
            + selectExpression.getAlias());
      }
      final int i = column.get().index();
      rowList.add(intermediate.get(i));
    }
    return rowList;
  }
}
