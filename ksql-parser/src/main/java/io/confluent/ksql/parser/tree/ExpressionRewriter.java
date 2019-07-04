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

public class ExpressionRewriter<C> {

  public Expression rewriteExpression(final Expression node, final C context,
                                      final ExpressionTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Expression rewriteStruct(
      final Type node,
      final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticUnary(final ArithmeticUnaryExpression node, final C context,
                                           final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticBinary(final ArithmeticBinaryExpression node, final C context,
                                            final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteComparisonExpression(final ComparisonExpression node, final C context,
                                                final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteBetweenPredicate(final BetweenPredicate node, final C context,
                                            final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLogicalBinaryExpression(
      final LogicalBinaryExpression node, final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteNotExpression(final NotExpression node, final C context,
                                         final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNullPredicate(final IsNullPredicate node, final C context,
                                           final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNotNullPredicate(final IsNotNullPredicate node, final C context,
                                              final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSearchedCaseExpression(
      final SearchedCaseExpression node, final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSimpleCaseExpression(final SimpleCaseExpression node, final C context,
                                                final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteWhenClause(final WhenClause node, final C context,
                                      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInListExpression(final InListExpression node, final C context,
                                            final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteFunctionCall(final FunctionCall node, final C context,
                                        final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLikePredicate(final LikePredicate node, final C context,
                                         final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInPredicate(final InPredicate node, final C context,
                                       final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLiteral(final Literal node, final C context,
                                   final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSubscriptExpression(final SubscriptExpression node, final C context,
                                               final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteQualifiedNameReference(
      final QualifiedNameReference node, final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteDereferenceExpression(
      final DereferenceExpression node, final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteCast(
      final Cast node,
      final C context,
      final ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }
}
