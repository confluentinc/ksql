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

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.Delete;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FrameBound;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetOperation;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowFrame;
import io.confluent.ksql.parser.tree.WithQuery;
import io.confluent.ksql.util.Pair;
import java.util.Set;

public class SetParentVisitor extends DefaultAstVisitor<Node, Node> {

  @Override
  protected Node visitExtract(final Extract node, final Node parent) {
    node.setParent(parent);
    
    return process(node.getExpression(), node);
  }

  @Override
  protected Node visitCast(final Cast node, final Node parent) {
    node.setParent(parent);
    return process(node.getExpression(), node);
  }

  @Override
  protected Node visitArithmeticBinary(final ArithmeticBinaryExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getLeft(), node);
    process(node.getRight(), node);

    return null;
  }

  @Override
  protected Node visitBetweenPredicate(final BetweenPredicate node, final Node parent) {
    node.setParent(parent);
    process(node.getValue(), node);
    process(node.getMin(), node);
    process(node.getMax(), node);

    return null;
  }

  @Override
  protected Node visitSubscriptExpression(final SubscriptExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getBase(), node);
    process(node.getIndex(), node);

    return null;
  }

  @Override
  protected Node visitComparisonExpression(final ComparisonExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getLeft(), node);
    process(node.getRight(), node);

    return null;
  }

  @Override
  protected Node visitQuery(final Query node, final Node parent) {
    node.setParent(parent);
    process(node.getQueryBody(), node);
    return null;
  }


  @Override
  protected Node visitWithQuery(final WithQuery node, final Node parent) {
    node.setParent(parent);
    return process(node.getQuery(), node);
  }

  @Override
  protected Node visitSelect(final Select node, final Node parent) {
    node.setParent(parent);
    for (final SelectItem item : node.getSelectItems()) {
      process(item, node);
    }

    return null;
  }

  @Override
  protected Node visitSingleColumn(final SingleColumn node, final Node parent) {
    node.setParent(parent);
    process(node.getExpression(), node);

    return null;
  }

  @Override
  protected Node visitWhenClause(final WhenClause node, final Node parent) {
    node.setParent(parent);
    process(node.getOperand(), node);
    process(node.getResult(), node);

    return null;
  }

  @Override
  protected Node visitInPredicate(final InPredicate node, final Node parent) {
    node.setParent(parent);
    process(node.getValue(), node);
    process(node.getValueList(), node);

    return null;
  }

  @Override
  protected Node visitFunctionCall(final FunctionCall node, final Node parent) {
    node.setParent(parent);
    for (final Expression argument : node.getArguments()) {
      process(argument, node);
    }

    if (node.getWindow().isPresent()) {
      process(node.getWindow().get(), node);
    }

    return null;
  }

  @Override
  protected Node visitDereferenceExpression(final DereferenceExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getBase(), node);
    return null;
  }

  @Override
  public Node visitWindow(final Window node, final Node parent) {
    node.setParent(parent);

    process(node.getWindowExpression(), node);
    return null;
  }

  @Override
  public Node visitWindowFrame(final WindowFrame node, final Node parent) {
    node.setParent(parent);
    process(node.getStart(), node);
    if (node.getEnd().isPresent()) {
      process(node.getEnd().get(), node);
    }

    return null;
  }

  @Override
  public Node visitFrameBound(final FrameBound node, final Node parent) {
    node.setParent(parent);
    if (node.getValue().isPresent()) {
      process(node.getValue().get(), node);
    }

    return null;
  }

  @Override
  protected Node visitSimpleCaseExpression(final SimpleCaseExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getOperand(), node);
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, node);
    }

    node.getDefaultValue()
        .ifPresent(value -> process(value, node));

    return null;
  }

  @Override
  protected Node visitInListExpression(final InListExpression node, final Node parent) {
    node.setParent(parent);
    for (final Expression value : node.getValues()) {
      process(value, node);
    }

    return null;
  }

  @Override
  protected Node visitNullIfExpression(final NullIfExpression node, final Node parent) {
    node.setParent(parent);
    process(node.getFirst(), node);
    process(node.getSecond(), node);

    return null;
  }

  @Override
  protected Node visitArithmeticUnary(final ArithmeticUnaryExpression node, final Node parent) {
    node.setParent(parent);
    return process(node.getValue(), node);
  }

  @Override
  protected Node visitNotExpression(final NotExpression node, final Node parent) {
    node.setParent(parent);
    return process(node.getValue(), node);
  }

  @Override
  protected Node visitSearchedCaseExpression(final SearchedCaseExpression node, final Node parent) {
    node.setParent(parent);
    for (final WhenClause clause : node.getWhenClauses()) {
      process(clause, node);
    }
    node.getDefaultValue()
        .ifPresent(value -> process(value, node));

    return null;
  }

  @Override
  protected Node visitLikePredicate(final LikePredicate node, final Node parent) {
    node.setParent(parent);
    process(node.getValue(), node);
    process(node.getPattern(), node);
    if (node.getEscape() != null) {
      process(node.getEscape(), node);
    }

    return null;
  }

  @Override
  protected Node visitIsNotNullPredicate(final IsNotNullPredicate node, final Node parent) {
    node.setParent(parent);
    return process(node.getValue(), node);
  }

  @Override
  protected Node visitIsNullPredicate(final IsNullPredicate node, final Node parent) {
    node.setParent(parent);
    return process(node.getValue(), node);
  }

  @Override
  protected Node visitLogicalBinaryExpression(
      final LogicalBinaryExpression node,
      final Node parent) {
    node.setParent(parent);
    process(node.getLeft(), node);
    process(node.getRight(), node);

    return null;
  }

  @Override
  protected Node visitSubqueryExpression(final SubqueryExpression node, final Node parent) {
    node.setParent(parent);
    return process(node.getQuery(), node);
  }

  @Override
  protected Node visitQuerySpecification(final QuerySpecification node, final Node parent) {
    node.setParent(parent);
    process(node.getSelect(), node);

    process(node.getFrom(), node);
    if (node.getWhere().isPresent()) {
      process(node.getWhere().get(), node);
    }
    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), node);
    }
    if (node.getHaving().isPresent()) {
      process(node.getHaving().get(), node);
    }
    return null;
  }

  @Override
  protected Node visitSetOperation(final SetOperation node, final Node parent) {
    node.setParent(parent);
    for (final Relation relation : node.getRelations()) {
      process(relation, node);
    }
    return null;
  }

  @Override
  protected Node visitValues(final Values node, final Node parent) {
    node.setParent(parent);
    for (final Expression row : node.getRows()) {
      process(row, node);
    }
    return null;
  }

  @Override
  protected Node visitStruct(final Struct node, final Node parent) {
    node.setParent(parent);
    for (final Pair<String, Type> structItem : node.getItems()) {
      process(structItem.getRight(), node);
    }
    return null;
  }

  @Override
  protected Node visitTableSubquery(final TableSubquery node, final Node parent) {
    node.setParent(parent);
    return process(node.getQuery(), node);
  }

  @Override
  protected Node visitAliasedRelation(final AliasedRelation node, final Node parent) {
    node.setParent(parent);
    return process(node.getRelation(), node);
  }

  @Override
  protected Node visitJoin(final Join node, final Node parent) {
    node.setParent(parent);
    process(node.getLeft(), node);
    process(node.getRight(), node);

    node.getCriteria()
        .filter(criteria -> criteria instanceof JoinOn)
        .map(criteria -> process(((JoinOn) criteria).getExpression(), node));

    return null;
  }

  @Override
  protected Node visitGroupBy(final GroupBy node, final Node parent) {
    node.setParent(parent);
    for (final GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, node);
    }

    return null;
  }

  @Override
  protected Node visitGroupingElement(final GroupingElement node, final Node parent) {
    node.setParent(parent);
    for (final Set<Expression> expressions : node.enumerateGroupingSets()) {
      for (final Expression expression : expressions) {
        process(expression, node);
      }
    }
    return null;
  }

  @Override
  protected Node visitSimpleGroupBy(final SimpleGroupBy node, final Node parent) {
    node.setParent(parent);
    visitGroupingElement(node, node);

    for (final Expression expression : node.getColumnExpressions()) {
      process(expression, node);
    }

    return null;
  }

  @Override
  protected Node visitDelete(final Delete node, final Node parent) {
    node.setParent(parent);
    process(node.getTable(), node);
    node.getWhere().ifPresent(where -> process(where, node));

    return null;
  }

  @Override
  protected Node visitCreateTableAsSelect(final CreateTableAsSelect node, final Node parent) {
    node.setParent(parent);
    process(node.getQuery(), node);
    node.getProperties().values().forEach(expression -> process(expression, node));

    return null;
  }

  protected Node visitCreateStreamAsSelect(final CreateStreamAsSelect node, final Node parent) {
    node.setParent(parent);
    process(node.getQuery(), node);
    node.getProperties().values().forEach(expression -> process(expression, node));

    return null;
  }

}
