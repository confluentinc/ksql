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
  public Void visitCast(Cast node, C context) {
    return process(node.getExpression(), context);
  }

  @Override
  public Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitBetweenPredicate(BetweenPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);
    return null;
  }

  @Override
  public Void visitSubscriptExpression(SubscriptExpression node, C context) {
    process(node.getBase(), context);
    process(node.getIndex(), context);
    return null;
  }

  @Override
  public Void visitStructExpression(CreateStructExpression node, C context) {
    node.getStruct().values().forEach(exp -> process(exp, context));
    return null;
  }

  @Override
  public Void visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitWhenClause(WhenClause node, C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);
    return null;
  }

  @Override
  public Void visitInPredicate(InPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);
    return null;
  }

  @Override
  public Void visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      process(argument, context);
    }
    return null;
  }

  @Override
  public Void visitDereferenceExpression(DereferenceExpression node, C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  public Void visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    process(node.getOperand(), context);
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, context));
    return null;
  }

  @Override
  public Void visitInListExpression(InListExpression node, C context) {
    for (Expression value : node.getValues()) {
      process(value, context);
    }
    return null;
  }

  @Override
  public Void visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitNotExpression(NotExpression node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, context));
    return null;
  }

  @Override
  public Void visitLikePredicate(LikePredicate node, C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);
    return null;
  }

  @Override
  public Void visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitIsNullPredicate(IsNullPredicate node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public Void visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  @Override
  public Void visitDoubleLiteral(DoubleLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitDecimalLiteral(DecimalLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitTimeLiteral(TimeLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitTimestampLiteral(TimestampLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitStringLiteral(StringLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitBooleanLiteral(BooleanLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitColumnReference(ColumnReferenceExp node, C context) {
    return null;
  }

  @Override
  public Void visitNullLiteral(NullLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitLongLiteral(LongLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitIntegerLiteral(IntegerLiteral node, C context) {
    return null;
  }

  @Override
  public Void visitType(Type node, C context) {
    return null;
  }
}
