/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.logicalplanner.nodes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterators;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.LogicalColumn;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.UnknownColumnException;
import java.util.Objects;

public final class SelectNode extends SingleInputNode<SelectNode> {
  final ImmutableList<LogicalColumn> outputSchema;

  public SelectNode(
      final Node<?> input,
      final Select selectClause
  ) {
    super(input);
    Objects.requireNonNull(selectClause, "selectClause");

    final ImmutableList<LogicalColumn> inputSchema = input.getOutputSchema();
    final Builder<LogicalColumn> outputSchemaBuilder = ImmutableList.builder();

    selectClause.getSelectItems().forEach(
        s -> {
          if (s instanceof AllColumns) {
            outputSchemaBuilder.addAll(inputSchema);
          } else {
            final Expression e = ((SingleColumn) s).getExpression();
            // to-do: maybe extend using
            // `UnqualifiedColumnReferenceExp` and `QualifiedColumnReferenceExp`
            if (e instanceof ColumnReferenceExp) {
              final ColumnName columnName = ((ColumnReferenceExp) e).getColumnName();
              final ImmutableList<LogicalColumn> matchingInputColumns = inputSchema.stream()
                  .filter(logicalColumn -> logicalColumn.name().equals(columnName))
                  .collect(ImmutableList.toImmutableList());
              if (matchingInputColumns.isEmpty()) {
                throw new UnknownColumnException("SELECT", (ColumnReferenceExp) e);

              }
              if (matchingInputColumns.size() > 1) {
                throw new KsqlException("Ambiguous column " + columnName + " in SELECT clause");
              }

              outputSchemaBuilder.add(Iterators.getOnlyElement(matchingInputColumns.iterator()));
            } else {
              throw new UnsupportedOperationException(
                  "New query planner only support column references, no expressions or aliases."
              );
            }
          }
        }
    );

    outputSchema = outputSchemaBuilder.build();
  }

  public ImmutableList<LogicalColumn> getOutputSchema() {
    return outputSchema;
  }

  public Node<?> getInputNode() {
    return input;
  }

  public <ReturnsT> ReturnsT accept(final NodeVisitor<SelectNode, ReturnsT> visitor) {
    return visitor.process(this);
  }
}
