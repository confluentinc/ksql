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
 * VisitParentExpressionVisitor is meant to be a base class for an ExpressionVisitor implementation
 * that by default, implements each visit[NodeType] call by calling a delegate visit impl for that
 * type's parent. This is useful for implementing visit behaviour that is common to all nodes of a
 * given type. For example, a visitor may be interested in recording the type of any literal, and so
 * can provide an implementation for visitLiteral rather than implementing visitIntegerLiteral,
 * visitBooleanLiteral, and so on.
 *
 * @param <R> The type of the result returned by the visitor
 * @param <C> The type of the context object passed through calls to visit[NodeType]
 */
public abstract class VisitParentExpressionVisitor<R, C> implements ExpressionVisitor<R, C> {

  private final R defaultValue;

  protected VisitParentExpressionVisitor() {
    this(null);
  }

  protected VisitParentExpressionVisitor(R defaultValue) {
    this.defaultValue = defaultValue;
  }

  protected R visitExpression(Expression node, C context) {
    return defaultValue;
  }

  @Override
  public R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitDoubleLiteral(DoubleLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitDecimalLiteral(DecimalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitTimeLiteral(TimeLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitTimestampLiteral(TimestampLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitColumnReference(ColumnReferenceExp node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitLikePredicate(LikePredicate node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitSubscriptExpression(SubscriptExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitStructExpression(StructExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitIntegerLiteral(IntegerLiteral node, C context) {
    return visitLiteral(node, context);
  }

  @Override
  public R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitType(Type node, C context) {
    return visitExpression(node, context);
  }

  @Override
  public R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }
}
