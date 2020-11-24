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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.PullProcessingContext;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.select.SelectValueMapper;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory;
import io.confluent.ksql.execution.transform.select.SelectValueMapperFactory.SelectValueMapperFactorySupplier;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.planner.plan.FinalProjectNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class ProjectOperator extends AbstractPhysicalOperator implements UnaryPhysicalOperator {

  private final ProcessingLogger logger;
  private final SelectValueMapperFactorySupplier selectValueMapperFactorySupplier;
  private final FinalProjectNode logicalNode;

  private AbstractPhysicalOperator child;
  private TableRow row;
  private KsqlTransformer<Object, GenericRow> transformer;

  public ProjectOperator(
      final ProcessingLogger logger,
      final FinalProjectNode logicalNode
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
      final FinalProjectNode logicalNode,
      final SelectValueMapperFactorySupplier selectValueMapperFactorySupplier
  ) {
    this.logger = Objects.requireNonNull(logger, "logger");
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.selectValueMapperFactorySupplier = selectValueMapperFactorySupplier;
  }

  @Override
  public void open() {
    child.open();
    if (logicalNode.getIsPullQuerySelectStar()) {
      return;
    }

    final SelectValueMapper<Object> select = selectValueMapperFactorySupplier.create(
        logicalNode.getSelectExpressions(),
        logicalNode.getCompiledSelectExpressions().get()
    );

    transformer = select.getTransformer(logger);
  }

  @Override
  public Object next() {
    row = (TableRow)child.next();
    if (row == null) {
      return null;
    }
    if (logicalNode.getIsPullQuerySelectStar()) {
      return createRow(row);
    }
    final GenericRow intermediate = getPreSelectTransform().apply(row);

    final GenericRow mapped = transformer.transform(
        row.key(),
        intermediate,
        new PullProcessingContext(row.rowTime())
    );
    validateProjection(mapped, logicalNode.getPullQueryOutputSchema().get());

    return mapped.values();
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
  public void addChild(final AbstractPhysicalOperator child) {
    if (this.child != null) {
      throw new UnsupportedOperationException("The project operator already has a child.");
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

  private Function<TableRow, GenericRow> getPreSelectTransform() {

    if (logicalNode.getNoAdditionalColumnsInSchema()) {
      return TableRow::value;
    }

    return row -> {
      final Struct key = row.key();
      final GenericRow value = row.value();

      final List<Object> keyFields = key.schema().fields().stream()
          .map(key::get)
          .collect(Collectors.toList());

      value.ensureAdditionalCapacity(
          1 // ROWTIME
              + keyFields.size()
              + row.window().map(w -> 2).orElse(0)
      );

      value.append(row.rowTime());
      value.appendAll(keyFields);

      row.window().ifPresent(window -> {
        value.append(window.start().toEpochMilli());
        value.append(window.end().toEpochMilli());
      });

      return value;
    };
  }

  private void validateProjection(
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

  private static List<?> createRow(final TableRow row) {
    final List<Object> rowList = new ArrayList<>(row.key().values());

    row.window().ifPresent(window -> {
      rowList.add(window.start().toEpochMilli());
      rowList.add(window.end().toEpochMilli());
    });

    rowList.addAll(row.value().values());

    return rowList;
  }
}
