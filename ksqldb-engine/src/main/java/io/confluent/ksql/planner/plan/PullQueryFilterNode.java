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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.PullQueryValidator;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.engine.generic.GenericExpressionResolver;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
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
  private final ExpressionMetadata compiledWhereClause;

  private boolean isKeyedQuery;

  public PullQueryFilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate,
      final RewrittenAnalysis analysis,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    this.predicate = Objects.requireNonNull(predicate, "predicate");
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();
    final Validator validator = new Validator();
    validator.process(predicate, null);
    if (!isKeyedQuery) {
      throw invalidWhereClauseException("WHERE clause missing key column", isWindowed);
    }
    compiledWhereClause = CodeGenRunner.compileExpression(
        predicate,
        "Predicate",
        getSchema(),
        ksqlConfig,
        metaStore
    );
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

  public ExpressionMetadata getCompiledWhereClause() {
    return compiledWhereClause;
  }

  private List<Object> extractKeys() {
    final List<Object> keys;

    if (keyComparison.size() > 0) {
      keys = ImmutableList.of(
          extractKeyWhereClause(
              keyComparison,
              windowed,
              query.getLogicalSchema(),
              metaStore,
              config)
      );
    } else {
      keys = extractKeysFromInPredicate(
          inPredicate,
          windowed,
          query.getLogicalSchema(),
          metaStore,
          config
      );
    }
  }

  /**
   * Validate the WHERE clause for pull queries.
   * 1. There must be exactly one equality condition or one IN predicate that involves a key.
   * 2. An IN predicate can refer to a single key.
   * 3. The IN predicate cannot be combined with other conditions.
   * 4. Only AND is allowed.
   */
  private final class Validator extends TraversalExpressionVisitor<Object> {

    //TODO any other expression should throw exception

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context
    ) {
      if (node.getType() != LogicalBinaryExpression.Type.AND) {
        throw invalidWhereClauseException("Only AND expressions are supported: " + node, false);
      }
      process(node.getLeft(), context);
      process(node.getRight(), context);
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node,
        final Object context
    ) {
      final ComparisonExpression comparison = (ComparisonExpression) predicate;
      final ColumnName keyColumn = Iterables.getOnlyElement(getSource().getSchema().key()).name();
      if (comparison.getType() != Type.EQUAL) {
        throw invalidWhereClauseException(
            "Bound on '" + keyColumn.text() + "' must currently be '='", isWindowed);
      }
      final UnqualifiedColumnReferenceExp column;
      if (comparison.getRight() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) comparison.getRight();
      } else if (comparison.getLeft() instanceof UnqualifiedColumnReferenceExp) {
        column = (UnqualifiedColumnReferenceExp) comparison.getLeft();
      } else {
        return null;
      }
      final ColumnName columnName = column.getColumnName();

      if (columnName.equals(keyColumn)) {
        if (isKeyedQuery) {
          throw invalidWhereClauseException(
              "An equality condition on the key column cannot be combined with other comparisons"
                  + " such as an IN predicate",
              isWindowed);
        }
        // TODO check if columnName is window and throw if table not windowed
        // TODO Check if column is not key and throw
        isKeyedQuery = true;
      }
      return null;
    }

    @Override
    public Void visitInPredicate(
        final InPredicate node,
        final Object context
    ) {
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
        // TODO Check if column is not key and throw
        isKeyedQuery = true;
      }
      return null;
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

  private static Object resolveKey(
      final Expression exp,
      final Column keyColumn,
      final MetaStore metaStore,
      final KsqlConfig config,
      final Expression errorMessageHint
  ) {
    final Object obj;
    if (exp instanceof NullLiteral) {
      obj = null;
    } else if (exp instanceof Literal) {
      // skip the GenericExpressionResolver because this is
      // a critical code path executed once-per-query
      obj = ((Literal) exp).getValue();
    } else {
      obj = new GenericExpressionResolver(
          keyColumn.type(),
          keyColumn.name(),
          metaStore,
          config,
          "pull query"
      ).resolve(exp);
    }

    if (obj == null) {
      throw new KsqlException("Primary key columns can not be NULL: " + errorMessageHint);
    }

    return DefaultSqlValueCoercer.STRICT.coerce(obj, keyColumn.type())
        .orElseThrow(() -> new KsqlException("'" + obj + "' can not be converted "
                                                 + "to the type of the key column: " + keyColumn.toString(
            FormatOptions.noEscape())))
        .orElse(null);
  }


}
