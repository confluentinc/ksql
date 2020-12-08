/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.physical.pull.operators.WhereInfo.KeyAndWindowBounds;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Set;

public class PullQueryFilterNode extends SingleSourcePlanNode {

  private static final Set<Type> VALID_WINDOW_BOUND_COMPARISONS = ImmutableSet.of(
      Type.EQUAL,
      Type.GREATER_THAN,
      Type.GREATER_THAN_OR_EQUAL,
      Type.LESS_THAN,
      Type.LESS_THAN_OR_EQUAL
  );

  private final Expression predicate;
  private final boolean isWindowed;
  private final RewrittenAnalysis analysis;

  private boolean isKeyedQuery;

  public PullQueryFilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate,
      final RewrittenAnalysis analysis
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    this.predicate = Objects.requireNonNull(predicate, "predicate");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();
  }

  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public LogicalSchema getSchema() {
    return getSource().getSchema();
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    throw new UnsupportedOperationException();
  }

  /**
   * Validate the WHERE clause for pull queries.
   * 1. There must be at least one equality condition or IN predicate that involves a key.
   * 2. An IN predicate can refer to a single key.
   * 3. The IN predicate cannot be combined with other conditions.
   */
  private void validateWhereClause() {
    // Visit all comparison expressions in WHERE clause

    if (predicate instanceof ComparisonExpression) {
      final ComparisonExpression comparison = (ComparisonExpression) predicate;
      final UnqualifiedColumnReferenceExp column;
      if (comparison.getRight() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) comparison.getRight();
      } else if (comparison.getLeft() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) comparison.getLeft();
      } else {
        return;
      }
      final ColumnName columnName = column.getColumnName();
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();
      if (columnName.equals(keyColumn)) {
        if (isKeyedQuery) {
          throw invalidWhereClauseException(
              "An equality condition on the key column cannot be combined with other comparisons"
                  + " such as an IN predicate",
              isWindowed);
        }
        isKeyedQuery = true;
        return;
      }
    } else if (predicate instanceof InPredicate) {
      final InPredicate inPredicate = (InPredicate) predicate;
      final UnqualifiedColumnReferenceExp column
          = (UnqualifiedColumnReferenceExp) inPredicate.getValue();
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();
      if (column.getColumnName().equals(keyColumn)) {
        if (isKeyedQuery) {
          throw invalidWhereClauseException(
              "The IN predicate cannot be combined with other comparisons on the key column",
              isWindowed);
        }
        isKeyedQuery = true;
        return;
      }
    }
  }

  private KsqlException invalidWhereClauseException(
    final String msg,
    final boolean windowed
  ) {
      final String additional = !windowed
          ? ""
          : System.lineSeparator()
              + " - (optionally) limits the time bounds of the windowed table."
              + System.lineSeparator()
              + "\t Bounds on " + SystemColumns.windowBoundsColumnNames() + " are supported"
              + System.lineSeparator()
              + "\t Supported operators are " + VALID_WINDOW_BOUND_COMPARISONS;

      return new KsqlException(
          msg
              + ". "
              + PullQueryValidator.PULL_QUERY_SYNTAX_HELP
              + System.lineSeparator()
              + "Pull queries require a WHERE clause that:"
              + System.lineSeparator()
              + " - limits the query to a single key, e.g. `SELECT * FROM X WHERE <key-column>=Y;`."
              + additional
      );
    }


}
