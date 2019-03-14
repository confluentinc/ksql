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

package io.confluent.ksql.parser;

import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.WhenClause;
import java.util.Set;

public abstract class DefaultTraversalVisitor<R, C>
    extends AstVisitor<R, C> {

  @Override
  protected R visitStatements(final Statements node, final C context) {
    node.getStatements()
        .forEach(stmt -> process(stmt, context));
    return visitNode(node, context);
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

    return null;
  }

  @Override
  protected R visitDereferenceExpression(final DereferenceExpression node, final C context) {
    process(node.getBase(), context);
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
  protected R visitStruct(final Struct node, final C context) {
    for (final Struct.Field field : node.getFields()) {
      process(field.getType(), context);
    }
    return null;
  }

  @Override
  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return process(node.getRelation(), context);
  }

  @Override
  protected R visitJoin(final Join node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    if (node.getCriteria() instanceof JoinOn) {
      process(((JoinOn) node.getCriteria()).getExpression(), context);
    }

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

    for (final Expression expression : node.getColumns()) {
      process(expression, context);
    }

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
}
