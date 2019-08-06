/*
 * Copyright 2018 Confluent Inc.
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

public abstract class DefaultAstVisitor<R, C>
    extends AstVisitor<R, C> {

  public R process(final AstNode node, @Nullable final C context) {
    return node.accept(this, context);
  }

  protected R visitNode(final AstNode node, final C context) {
    return null;
  }

  protected R visitExpression(final Expression node, final C context) {
    return null;
  }

  public R visitArithmeticBinary(final ArithmeticBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  public R visitBetweenPredicate(final BetweenPredicate node, final C context) {
    return visitExpression(node, context);
  }

  public R visitComparisonExpression(final ComparisonExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  protected R visitLiteral(final Literal node, final C context) {
    return visitExpression(node, context);
  }

  public R visitDoubleLiteral(final DoubleLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitDecimalLiteral(final DecimalLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  protected R visitStatements(final Statements node, final C context) {
    return visitNode(node, context);
  }

  protected R visitStatement(final Statement node, final C context) {
    return visitNode(node, context);
  }

  protected R visitQuery(final Query node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitExplain(final Explain node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitShowColumns(final ShowColumns node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitShowFunctions(final ListFunctions node, final C context) {
    return visitStatement(node, context);
  }

  public R visitTimeLiteral(final TimeLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  protected R visitSelect(final Select node, final C context) {
    return visitNode(node, context);
  }

  protected R visitRelation(final Relation node, final C context) {
    return visitNode(node, context);
  }

  public R visitTimestampLiteral(final TimestampLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitWhenClause(final WhenClause node, final C context) {
    return visitExpression(node, context);
  }

  public R visitInPredicate(final InPredicate node, final C context) {
    return visitExpression(node, context);
  }

  public R visitFunctionCall(final FunctionCall node, final C context) {
    return visitExpression(node, context);
  }

  public R visitSimpleCaseExpression(final SimpleCaseExpression node, final C context) {
    return visitExpression(node, context);
  }

  public R visitStringLiteral(final StringLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitBooleanLiteral(final BooleanLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitInListExpression(final InListExpression node, final C context) {
    return visitExpression(node, context);
  }

  public R visitQualifiedNameReference(final QualifiedNameReference node, final C context) {
    return visitExpression(node, context);
  }

  public R visitDereferenceExpression(final DereferenceExpression node, final C context) {
    return visitExpression(node, context);
  }

  public R visitNullLiteral(final NullLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitArithmeticUnary(final ArithmeticUnaryExpression node, final C context) {
    process(node.getValue(), context);
    return visitExpression(node, context);
  }

  public R visitNotExpression(final NotExpression node, final C context) {
    return visitExpression(node, context);
  }

  protected R visitSelectItem(final SelectItem node, final C context) {
    return visitNode(node, context);
  }

  protected R visitSingleColumn(final SingleColumn node, final C context) {
    process(node.getExpression(), context);
    return visitSelectItem(node, context);
  }

  protected R visitAllColumns(final AllColumns node, final C context) {
    return visitSelectItem(node, context);
  }

  public R visitSearchedCaseExpression(final SearchedCaseExpression node, final C context) {
    return visitExpression(node, context);
  }

  public R visitLikePredicate(final LikePredicate node, final C context) {
    process(node.getValue(), context);
    return visitExpression(node, context);
  }

  public R visitIsNotNullPredicate(final IsNotNullPredicate node, final C context) {
    return visitExpression(node, context);
  }

  public R visitIsNullPredicate(final IsNullPredicate node, final C context) {
    return visitExpression(node, context);
  }

  public R visitSubscriptExpression(final SubscriptExpression node, final C context) {
    return visitExpression(node, context);
  }

  public R visitLongLiteral(final LongLiteral node, final C context) {
    return visitLiteral(node, context);
  }

  public R visitLogicalBinaryExpression(final LogicalBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return visitExpression(node, context);
  }

  protected R visitTable(final Table node, final C context) {
    return visitRelation(node, context);
  }

  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return visitRelation(node, context);
  }

  protected R visitJoin(final Join node, final C context) {
    return visitRelation(node, context);
  }

  public R visitCast(final Cast node, final C context) {
    return visitExpression(node, context);
  }

  protected R visitWindowExpression(final WindowExpression node, final C context) {
    return visitNode(node, context);
  }

  protected R visitKsqlWindowExpression(final KsqlWindowExpression node, final C context) {
    return visitNode(node, context);
  }

  protected R visitTumblingWindowExpression(final TumblingWindowExpression node, final C context) {
    return visitKsqlWindowExpression(node, context);
  }

  protected R visitHoppingWindowExpression(final HoppingWindowExpression node, final C context) {
    return visitKsqlWindowExpression(node, context);
  }

  protected R visitSessionWindowExpression(final SessionWindowExpression node, final C context) {
    return visitKsqlWindowExpression(node, context);
  }

  protected R visitTableElement(final TableElement node, final C context) {
    return visitNode(node, context);
  }

  protected R visitCreateStream(final CreateStream node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateStreamAsSelect(final CreateStreamAsSelect node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTable(final CreateTable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitCreateTableAsSelect(final CreateTableAsSelect node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitInsertInto(final InsertInto node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDropStream(final DropStream node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitDropTable(final DropTable node, final C context) {
    return visitStatement(node, context);
  }

  protected R visitGroupBy(final GroupBy node, final C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingElement(final GroupingElement node, final C context) {
    return visitNode(node, context);
  }

  protected R visitSimpleGroupBy(final SimpleGroupBy node, final C context) {
    return visitGroupingElement(node, context);
  }
}
