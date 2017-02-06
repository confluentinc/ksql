/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.parser.tree;

/**
 * When walking Expressions, don't traverse into SubqueryExpressions
 */
public abstract class DefaultExpressionTraversalVisitor<R, C>
    extends DefaultTraversalVisitor<R, C> {

  @Override
  protected R visitSubqueryExpression(SubqueryExpression node, C context) {
    // Don't traverse into Subqueries within an Expression
    return null;
  }
}
