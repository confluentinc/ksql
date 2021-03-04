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

public interface ExpressionVisitor<R, C> {

  default R process(final Expression node, final C context) {
    return node.accept(this, context);
  }

  R visitArithmeticBinary(ArithmeticBinaryExpression exp, C context);

  R visitArithmeticUnary(ArithmeticUnaryExpression exp, C context);

  R visitBetweenPredicate(BetweenPredicate exp, C context);

  R visitBooleanLiteral(BooleanLiteral exp, C context);

  R visitCast(Cast exp, C context);

  R visitComparisonExpression(ComparisonExpression exp, C context);

  R visitDecimalLiteral(DecimalLiteral exp, C context);

  R visitDereferenceExpression(DereferenceExpression exp, C context);

  R visitDoubleLiteral(DoubleLiteral exp, C context);

  R visitFunctionCall(FunctionCall exp, C context);

  R visitInListExpression(InListExpression exp, C context);

  R visitInPredicate(InPredicate exp, C context);

  R visitIntegerLiteral(IntegerLiteral exp, C context);

  R visitIsNotNullPredicate(IsNotNullPredicate exp, C context);

  R visitIsNullPredicate(IsNullPredicate exp, C context);

  R visitLikePredicate(LikePredicate exp, C context);

  R visitLogicalBinaryExpression(LogicalBinaryExpression exp, C context);

  R visitLongLiteral(LongLiteral exp, C context);

  R visitNotExpression(NotExpression exp, C context);

  R visitNullLiteral(NullLiteral exp, C context);

  R visitUnqualifiedColumnReference(UnqualifiedColumnReferenceExp exp, C context);

  R visitQualifiedColumnReference(QualifiedColumnReferenceExp exp, C context);

  R visitSearchedCaseExpression(SearchedCaseExpression exp, C context);

  R visitSimpleCaseExpression(SimpleCaseExpression exp, C context);

  R visitStringLiteral(StringLiteral exp, C context);

  R visitSubscriptExpression(SubscriptExpression exp, C context);

  R visitCreateArrayExpression(CreateArrayExpression exp, C context);

  R visitCreateMapExpression(CreateMapExpression exp, C context);

  R visitStructExpression(CreateStructExpression exp, C context);

  R visitTimeLiteral(TimeLiteral exp, C context);

  R visitTimestampLiteral(TimestampLiteral exp, C context);

  R visitType(Type exp, C context);

  R visitWhenClause(WhenClause exp, C context);

  R visitLambdaExpression(LambdaFunctionCall exp, C context);

  R visitLambdaVariable(LambdaVariable exp, C context);

  R visitIntervalUnit(IntervalUnit exp, C context);
}
