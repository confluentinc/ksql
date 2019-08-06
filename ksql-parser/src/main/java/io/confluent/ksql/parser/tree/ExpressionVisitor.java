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

import javax.annotation.Nullable;

public interface ExpressionVisitor<R, C> {
  default R process(final Expression node, @Nullable final C context) {
    return node.accept(this, context);
  }

  R visitArithmeticBinary(ArithmeticBinaryExpression exp, @Nullable C context);

  R visitArithmeticUnary(ArithmeticUnaryExpression exp, @Nullable C context);

  R visitBetweenPredicate(BetweenPredicate exp, @Nullable C context);

  R visitBooleanLiteral(BooleanLiteral exp, @Nullable C context);

  R visitCast(Cast exp, @Nullable C context);

  R visitComparisonExpression(ComparisonExpression exp, @Nullable C context);

  R visitDecimalLiteral(DecimalLiteral exp, @Nullable C context);

  R visitDereferenceExpression(DereferenceExpression exp, @Nullable C context);

  R visitDoubleLiteral(DoubleLiteral exp, @Nullable C context);

  R visitFunctionCall(FunctionCall exp, @Nullable C context);

  R visitInListExpression(InListExpression exp, @Nullable C context);

  R visitInPredicate(InPredicate exp, @Nullable C context);

  R visitIntegerLiteral(IntegerLiteral exp, @Nullable C context);

  R visitIsNotNullPredicate(IsNotNullPredicate exp, @Nullable C context);

  R visitIsNullPredicate(IsNullPredicate exp, @Nullable C context);

  R visitLikePredicate(LikePredicate exp, @Nullable C context);

  R visitLogicalBinaryExpression(LogicalBinaryExpression exp, @Nullable C context);

  R visitLongLiteral(LongLiteral exp, @Nullable C context);

  R visitNotExpression(NotExpression exp, @Nullable C context);

  R visitNullLiteral(NullLiteral exp, @Nullable C context);

  R visitQualifiedNameReference(QualifiedNameReference exp, @Nullable C context);

  R visitSearchedCaseExpression(SearchedCaseExpression exp, @Nullable C context);

  R visitSimpleCaseExpression(SimpleCaseExpression exp, @Nullable C context);

  R visitStringLiteral(StringLiteral exp, @Nullable C context);

  R visitSubscriptExpression(SubscriptExpression exp, @Nullable C context);

  R visitTimeLiteral(TimeLiteral exp, @Nullable C context);

  R visitTimestampLiteral(TimestampLiteral exp, @Nullable C context);

  R visitType(Type exp, @Nullable C context);

  R visitWhenClause(WhenClause exp, @Nullable C context);

}
