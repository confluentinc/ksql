/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.expression.tree;

/**
 * TraversalExpressionVisitor provides an abstract base class for implementations of
 * ExpressionVisitor, which by default traverses the expression tree.
 *
 * @param <C> The type of the context object passed through the visitor.
 */
public abstract class TraversalExpressionVisitor<C> implements ExpressionVisitor<Void, C> {

  @Override
  public Void visitCast(final Cast node, final C context) {
    return process(node.getExpression(), context);
  }

  @Override
  public Void visitArithmeticBinary(final ArithmeticBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitBetweenPredicate(final BetweenPredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);
    return null;
  }

  @Override
  public Void visitSubscriptExpression(final SubscriptExpression node, final C context) {
    process(node.getBase(), context);
    process(node.getIndex(), context);
    return null;
  }

  @Override
  public Void visitCreateArrayExpression(final CreateArrayExpression exp, final C context) {
    exp.getValues().forEach(val -> process(val, context));
    return null;
  }

  @Override
  public Void visitCreateMapExpression(final CreateMapExpression exp, final C context) {
    exp.getMap().keySet().forEach(key -> process(key, context));
    exp.getMap().values().forEach(val -> process(val, context));
    return null;
  }

  @Override
  public Void visitStructExpression(final CreateStructExpression node, final C context) {
    node.getFields().forEach(field -> process(field.getValue(), context));
    return null;
  }

  @Override
  public Void visitComparisonExpression(final ComparisonExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitWhenClause(final WhenClause node, final C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);
    return null;
  }

  @Override
  public Void visitInPredicate(final InPredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);
    return null;
  }

  @Override
  public Void visitFunctionCall(final FunctionCall node, final C context) {
    for (final Expression argument : node.getArguments()) {
      process(argument, context);
    }
    return null;
  }

  @Override
  public Void visitLambdaExpression(final LambdaFunctionCall node, final C context) {
    process(node.getBody(), context);
    return null;
  }

  @Override
  public Void visitDereferenceExpression(final DereferenceExpression node, final C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  public Void visitSimpleCaseExpression(final SimpleCaseExpression node, final C context) {
    process(node.getOperand(), context);
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, context));
    return null;
  }

  @Override
  public Void visitInListExpression(final InListExpression node, final C context) {
    for (final Expression value : node.getValues()) {
      process(value, context);
    }
    return null;
  }

  @Override
  public Void visitArithmeticUnary(final ArithmeticUnaryExpression node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitNotExpression(final NotExpression node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitSearchedCaseExpression(final SearchedCaseExpression node, final C context) {
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, context));
    return null;
  }

  @Override
  public Void visitLikePredicate(final LikePredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);
    return null;
  }

  @Override
  public Void visitIsNotNullPredicate(final IsNotNullPredicate node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitIsNullPredicate(final IsNullPredicate node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitLogicalBinaryExpression(final LogicalBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitDoubleLiteral(final DoubleLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitDecimalLiteral(final DecimalLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitTimeLiteral(final TimeLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitTimestampLiteral(final TimestampLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitStringLiteral(final StringLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitBooleanLiteral(final BooleanLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitLambdaVariable(final LambdaVariable node, final C context) {
    return null;
  }

  @Override
  public Void visitIntervalUnit(final IntervalUnit node, final C context) {
    return null;
  }

  @Override
  public Void visitUnqualifiedColumnReference(
      final UnqualifiedColumnReferenceExp node,
      final C context
  ) {
    return null;
  }

  @Override
  public Void visitQualifiedColumnReference(
      final QualifiedColumnReferenceExp node,
      final C context
  ) {
    return null;
  }

  @Override
  public Void visitNullLiteral(final NullLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitLongLiteral(final LongLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitIntegerLiteral(final IntegerLiteral node, final C context) {
    return null;
  }

  @Override
  public Void visitType(final Type node, final C context) {
    return null;
  }
}
