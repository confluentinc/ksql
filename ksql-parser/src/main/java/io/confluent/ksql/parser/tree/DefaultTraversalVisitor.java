/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import io.confluent.ksql.util.Pair;
import java.util.Set;

public abstract class DefaultTraversalVisitor<R, C>
    extends AstVisitor<R, C> {

  @Override
  protected R visitExtract(final Extract node, final C context) {
    return process(node.getExpression(), context);
  }

  @Override
  protected R visitCast(final Cast node, final C context) {
    return process(node.getExpression(), context);
  }

  @Override
  protected R visitArithmeticBinary(final ArithmeticBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected R visitBetweenPredicate(final BetweenPredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);

    return null;
  }

  @Override
  protected R visitSubscriptExpression(final SubscriptExpression node, final C context) {
    process(node.getBase(), context);
    process(node.getIndex(), context);

    return null;
  }

  @Override
  protected R visitComparisonExpression(final ComparisonExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected R visitQuery(final Query node, final C context) {
    process(node.getQueryBody(), context);
    return null;
  }


  @Override
  protected R visitWithQuery(final WithQuery node, final C context) {
    return process(node.getQuery(), context);
  }

  @Override
  protected R visitSelect(final Select node, final C context) {
    for (final SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  protected R visitSingleColumn(final SingleColumn node, final C context) {
    process(node.getExpression(), context);

    return null;
  }

  @Override
  protected R visitWhenClause(final WhenClause node, final C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);

    return null;
  }

  @Override
  protected R visitInPredicate(final InPredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);

    return null;
  }

  @Override
  protected R visitFunctionCall(final FunctionCall node, final C context) {
    for (final Expression argument : node.getArguments()) {
      process(argument, context);
    }

    if (node.getWindow().isPresent()) {
      process(node.getWindow().get(), context);
    }

    return null;
  }

  @Override
  protected R visitDereferenceExpression(final DereferenceExpression node, final C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  public R visitWindow(final Window node, final C context) {

    process(node.getWindowExpression(), context);
    return null;
  }

  @Override
  public R visitWindowFrame(final WindowFrame node, final C context) {
    process(node.getStart(), context);
    if (node.getEnd().isPresent()) {
      process(node.getEnd().get(), context);
    }

    return null;
  }

  @Override
  public R visitFrameBound(final FrameBound node, final C context) {
    if (node.getValue().isPresent()) {
      process(node.getValue().get(), context);
    }

    return null;
  }

  @Override
  protected R visitSimpleCaseExpression(final SimpleCaseExpression node, final C context) {
    process(node.getOperand(), context);
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }

    node.getDefaultValue()
        .ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected R visitInListExpression(final InListExpression node, final C context) {
    for (final Expression value : node.getValues()) {
      process(value, context);
    }

    return null;
  }

  @Override
  protected R visitNullIfExpression(final NullIfExpression node, final C context) {
    process(node.getFirst(), context);
    process(node.getSecond(), context);

    return null;
  }

  @Override
  protected R visitArithmeticUnary(final ArithmeticUnaryExpression node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  protected R visitNotExpression(final NotExpression node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  protected R visitSearchedCaseExpression(final SearchedCaseExpression node, final C context) {
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  protected R visitLikePredicate(final LikePredicate node, final C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);
    if (node.getEscape() != null) {
      process(node.getEscape(), context);
    }

    return null;
  }

  @Override
  protected R visitIsNotNullPredicate(final IsNotNullPredicate node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  protected R visitIsNullPredicate(final IsNullPredicate node, final C context) {
    return process(node.getValue(), context);
  }

  @Override
  protected R visitLogicalBinaryExpression(final LogicalBinaryExpression node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected R visitSubqueryExpression(final SubqueryExpression node, final C context) {
    return process(node.getQuery(), context);
  }

  @Override
  protected R visitQuerySpecification(final QuerySpecification node, final C context) {
    process(node.getSelect(), context);

    process(node.getFrom(), context);
    if (node.getWhere().isPresent()) {
      process(node.getWhere().get(), context);
    }
    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), context);
    }
    if (node.getHaving().isPresent()) {
      process(node.getHaving().get(), context);
    }
    return null;
  }

  @Override
  protected R visitSetOperation(final SetOperation node, final C context) {
    for (final Relation relation : node.getRelations()) {
      process(relation, context);
    }
    return null;
  }

  @Override
  protected R visitValues(final Values node, final C context) {
    for (final Expression row : node.getRows()) {
      process(row, context);
    }
    return null;
  }

  @Override
  protected R visitStruct(final Struct node, final C context) {
    for (final Pair<String, Type> structItem : node.getItems()) {
      process(structItem.getRight(), context);
    }
    return null;
  }

  @Override
  protected R visitTableSubquery(final TableSubquery node, final C context) {
    return process(node.getQuery(), context);
  }

  @Override
  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return process(node.getRelation(), context);
  }

  @Override
  protected R visitSampledRelation(final SampledRelation node, final C context) {
    process(node.getRelation(), context);
    process(node.getSamplePercentage(), context);
    if (node.getColumnsToStratifyOn().isPresent()) {
      for (final Expression expression : node.getColumnsToStratifyOn().get()) {
        process(expression, context);
      }
    }
    return null;
  }

  @Override
  protected R visitJoin(final Join node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    node.getCriteria()
        .filter(criteria -> criteria instanceof JoinOn)
        .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

    return null;
  }

  @Override
  protected R visitGroupBy(final GroupBy node, final C context) {
    for (final GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, context);
    }

    return null;
  }

  @Override
  protected R visitGroupingElement(final GroupingElement node, final C context) {
    for (final Set<Expression> expressions : node.enumerateGroupingSets()) {
      for (final Expression expression : expressions) {
        process(expression, context);
      }
    }
    return null;
  }

  @Override
  protected R visitSimpleGroupBy(final SimpleGroupBy node, final C context) {
    visitGroupingElement(node, context);

    for (final Expression expression : node.getColumnExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  protected R visitDelete(final Delete node, final C context) {
    process(node.getTable(), context);
    node.getWhere().ifPresent(where -> process(where, context));
    return null;
  }

  @Override
  protected R visitCreateTableAsSelect(final CreateTableAsSelect node, final C context) {
    process(node.getQuery(), context);
    node.getProperties().values().forEach(expression -> process(expression, context));
    return null;
  }

  @Override
  protected R visitInsertInto(final InsertInto node, final C context) {
    process(node.getQuery(), context);
    return null;
  }

  protected R visitCreateStreamAsSelect(final CreateStreamAsSelect node, final C context) {
    process(node.getQuery(), context);
    node.getProperties().values().forEach(expression -> process(expression, context));

    return null;
  }

}
