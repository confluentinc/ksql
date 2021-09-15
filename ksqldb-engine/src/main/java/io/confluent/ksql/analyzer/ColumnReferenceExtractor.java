package io.confluent.ksql.analyzer;

import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.name.ColumnName;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The purpose of this class is to extract ColumnNames from an {@link Expression}.
 * This currently successfully traverses the expression tree only if
 * non-leaf nodes are composed exclusively of ComparisonExpressions and LogicalBinaryExpressions,
 * as this was the behavior necessary to disable pull queries and scalable push queries
 * with ROWPARTITION and ROWOFFSET pseudocolumns (as these queries do not work with other clauses).
 */
public class ColumnReferenceExtractor extends
    VisitParentExpressionVisitor<Optional<Expression>, Void> {

  private final Set<ColumnName> columns = new HashSet<>();

  public Set<ColumnName> getColumns() {
    return columns;
  }

  @Override
  public Optional<Expression> visitUnqualifiedColumnReference(
      final UnqualifiedColumnReferenceExp node, final Void context) {
    columns.add(node.getColumnName());
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visitQualifiedColumnReference(
      final QualifiedColumnReferenceExp node, final Void context) {
    columns.add(node.getColumnName());
    return Optional.empty();
  }

  @Override
  public Optional<Expression> visitComparisonExpression(
      final ComparisonExpression node, final Void context) {
    process(node.getLeft(), null);
    process(node.getRight(), null);

    return Optional.empty();
  }

  @Override
  public Optional<Expression> visitLogicalBinaryExpression(
      final LogicalBinaryExpression node, final Void context) {
    process(node.getLeft(), null);
    process(node.getRight(), null);

    return Optional.empty();
  }
}
