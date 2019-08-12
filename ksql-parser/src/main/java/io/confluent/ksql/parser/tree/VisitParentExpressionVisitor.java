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

package io.confluent.ksql.parser.tree;

/**
 * VisitParentExpressionVisitor is meant to be a base class for an ExpressionVisitor implementation
 * that by default, implements each visit[NodeType] call by calling a delegate visit impl for that
 * type's parent. This is useful for implementing visit behaviour that is common to all nodes of
 * a given type. For example, a visitor may be interested in recording the type of any literal,
 * and so can provide an implementation for visitLiteral rather than implementing
 * visitIntegerLiteral, visitBooleanLiteral, and so on.
 *
 * @param <R> The type of the result returned by the visitor
 * @param <C> The type of the context object passed through calls to visit[NodeType]
 */
public abstract class VisitParentExpressionVisitor<R, C> implements ExpressionVisitor<R, C> {
  private final R defaultValue;

  protected VisitParentExpressionVisitor() {
    this(null);
  }

  protected VisitParentExpressionVisitor(final R defaultValue) {
    this.defaultValue = defaultValue;
  }

  protected R visitExpression(final Expression node, final C context) {
    return defaultValue;
  }

  @Override
  public R visitArithmeticBinary(final ArithmeticBinaryExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitBetweenPredicate(final BetweenPredicate node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitComparisonExpression(final ComparisonExpression node, final C context) {
    return visitExpression(node, context);
  }

  protected R visitLiteral(final Literal node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitDoubleLiteral(final DoubleLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitDecimalLiteral(final DecimalLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitTimeLiteral(final TimeLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitTimestampLiteral(final TimestampLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitWhenClause(final WhenClause node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitInPredicate(final InPredicate node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitFunctionCall(final FunctionCall node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSimpleCaseExpression(final SimpleCaseExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitStringLiteral(final StringLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitBooleanLiteral(final BooleanLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitInListExpression(final InListExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitQualifiedNameReference(final QualifiedNameReference node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitDereferenceExpression(final DereferenceExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitNullLiteral(final NullLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitArithmeticUnary(final ArithmeticUnaryExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitNotExpression(final NotExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSearchedCaseExpression(final SearchedCaseExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitLikePredicate(final LikePredicate node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitIsNotNullPredicate(final IsNotNullPredicate node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitIsNullPredicate(final IsNullPredicate node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSubscriptExpression(final SubscriptExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitLongLiteral(final LongLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitIntegerLiteral(final IntegerLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitLogicalBinaryExpression(final LogicalBinaryExpression node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitType(final Type node, final C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitCast(final Cast node, final C context) {
    return visitExpression(node, context);
  }
}
