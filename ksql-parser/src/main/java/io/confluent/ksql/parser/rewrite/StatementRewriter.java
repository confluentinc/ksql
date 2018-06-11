/*
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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BinaryLiteral;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateView;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.Delete;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropView;
import io.confluent.ksql.parser.tree.ExistsPredicate;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ExplainOption;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FieldReference;
import io.confluent.ksql.parser.tree.FrameBound;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GenericLiteral;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IntervalLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.LambdaExpression;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryBody;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RenameColumn;
import io.confluent.ksql.parser.tree.SampledRelation;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.SetOperation;
import io.confluent.ksql.parser.tree.SetSession;
import io.confluent.ksql.parser.tree.ShowCatalogs;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.ShowCreate;
import io.confluent.ksql.parser.tree.ShowFunctions;
import io.confluent.ksql.parser.tree.ShowPartitions;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.SymbolReference;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WindowFrame;
import io.confluent.ksql.parser.tree.WithQuery;
import io.confluent.ksql.util.KsqlException;

public class StatementRewriter extends DefaultAstVisitor<Node, Object> {

  protected Node visitNode(Node node, Object context) {
    return null;
  }

  protected Node visitExpression(Expression node, Object context) {
    return node;
  }

  protected Node visitExtract(Extract node, Object context) {

    if (node.getLocation().isPresent()) {
      return new Extract(node.getLocation().get(),
          (Expression) process(node.getExpression(), context),
          node.getField());
    } else {
      return new Extract((Expression) process(node.getExpression(), context),
          node.getField());
    }

  }

  protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new ArithmeticBinaryExpression(node.getLocation().get(),
          node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context));
    } else {
      return new ArithmeticBinaryExpression(node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context));
    }

  }

  protected Node visitBetweenPredicate(BetweenPredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new BetweenPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context),
          (Expression) process(node.getMin(), context),
          (Expression) process(node.getMax(), context));
    } else {
      return new BetweenPredicate((Expression) process(node.getValue(), context),
          (Expression) process(node.getMin(), context),
          (Expression) process(node.getMax(), context));
    }

  }

  protected Node visitComparisonExpression(ComparisonExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new ComparisonExpression(node.getLocation().get(),
          node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context));
    } else {
      return new ComparisonExpression(node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context));
    }

  }

  protected Node visitLiteral(Literal node, Object context) {
    return visitExpression(node, context);
  }

  protected Node visitDoubleLiteral(DoubleLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new DoubleLiteral(node.getLocation().get(), String.valueOf(node.getValue()));
    } else {
      return new DoubleLiteral(String.valueOf(node.getValue()));
    }

  }

  protected Node visitDecimalLiteral(DecimalLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new DecimalLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new DecimalLiteral(node.getValue());
    }
  }

  protected Node visitStatements(Statements node, Object context) {
    return new Statements(
        node.statementList
            .stream()
            .map(s -> (Statement) process(s, context))
            .collect(Collectors.toList())
    );
  }

  protected Node visitStatement(Statement node, Object context) {
    return visitNode(node, context);
  }

  protected Node visitQuery(Query node, Object context) {
    return new Query((QueryBody) process(node.getQueryBody(), context),
        node.getLimit());
  }

  protected Node visitExplain(Explain node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitShowCatalogs(ShowCatalogs node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitShowColumns(ShowColumns node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitShowPartitions(ShowPartitions node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitShowCreate(ShowCreate node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitShowFunctions(ShowFunctions node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitSetSession(SetSession node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitGenericLiteral(GenericLiteral node, Object context) {
    return visitLiteral(node, context);
  }

  protected Node visitTimeLiteral(TimeLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new TimeLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new TimeLiteral(node.getValue());
    }

  }

  protected Node visitExplainOption(ExplainOption node, Object context) {
    return visitNode(node, context);
  }

  protected Node visitWithQuery(WithQuery node, Object context) {
    if (node.getLocation().isPresent()) {
      return new WithQuery(node.getLocation().get(),
          node.getName(),
          (Query) process(node.getQuery(), context),
          node.getColumnNames());
    } else {
      return new WithQuery(node.getName(),
          (Query) process(node.getQuery(), context),
          node.getColumnNames());
    }

  }

  protected Node visitSelect(Select node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Select(node.getLocation().get(),
          node.isDistinct(),
          node.getSelectItems()
              .stream()
              .map(selectItem -> (SelectItem) process(selectItem, context))
              .collect(Collectors.toList())
      );
    } else {
      return new Select(node.isDistinct(),
          node.getSelectItems()
              .stream()
              .map(selectItem -> (SelectItem) process(selectItem, context))
              .collect(Collectors.toList())
      );
    }

  }

  protected Node visitRelation(Relation node, Object context) {
    return visitNode(node, context);
  }

  protected Node visitQueryBody(QueryBody node, Object context) {
    return visitRelation(node, context);
  }

  protected Node visitQuerySpecification(QuerySpecification node, Object context) {
    Optional<WindowExpression> windowExpression = node.getWindowExpression().isPresent()
        ? Optional.ofNullable((WindowExpression) process(node.getWindowExpression().get(), context))
        : Optional.empty();
    Optional<Expression> where = node.getWhere().isPresent()
        ? Optional.ofNullable((Expression) process(node.getWhere().get(),
        context))
        : Optional.empty();

    Optional<GroupBy> groupBy = node.getGroupBy().isPresent()
        ? Optional.ofNullable((GroupBy) process(node.getGroupBy().get(), context))
        : Optional.empty();

    Optional<Expression> having = node.getHaving().isPresent()
        ? Optional.ofNullable((Expression) process(node.getHaving().get(), context))
        : Optional.empty();

    if (node.getLocation().isPresent()) {
      return new QuerySpecification(
          node.getLocation().get(),
          (Select) process(node.getSelect(), context),
          (Relation) process(node.getInto(), context),
          node.isShouldCreateInto(),
          (Relation) process(node.getFrom(), context),
          windowExpression,
          where,
          groupBy,
          having,
          node.getLimit()
      );
    } else {
      return new QuerySpecification(
          (Select) process(node.getSelect(), context),
          (Relation) process(node.getInto(), context),
          node.isShouldCreateInto(),
          (Relation) process(node.getFrom(), context),
          windowExpression,
          where,
          groupBy,
          having,
          node.getLimit()
      );
    }

  }

  protected Node visitSetOperation(SetOperation node, Object context) {
    return visitQueryBody(node, context);
  }

  protected Node visitTimestampLiteral(TimestampLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new TimeLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new TimeLiteral(node.getValue());
    }
  }

  protected Node visitWhenClause(WhenClause node, Object context) {
    if (node.getLocation().isPresent()) {
      return new WhenClause(node.getLocation().get(),
          (Expression) process(node.getOperand(), context),
          (Expression) process(node.getResult(), context));
    } else {
      return new WhenClause((Expression) process(node.getOperand(), context),
          (Expression) process(node.getResult(), context));
    }

  }

  protected Node visitIntervalLiteral(IntervalLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new IntervalLiteral(node.getLocation().get(),
          node.getValue(),
          node.getSign(),
          node.getStartField(),
          node.getEndField()
      );
    } else {
      return new IntervalLiteral(node.getValue(),
          node.getSign(),
          node.getStartField(),
          node.getEndField()
      );
    }

  }

  protected Node visitInPredicate(InPredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new InPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context),
          (Expression) process(node.getValueList(), context));
    } else {
      return new InPredicate((Expression) process(node.getValue(), context),
          (Expression) process(node.getValueList(), context));
    }

  }

  protected Node visitFunctionCall(FunctionCall node, Object context) {
    Optional<Window> window = node.getWindow().isPresent()
        ? Optional.ofNullable((Window) process(node.getWindow().get(), context))
        : Optional.empty();

    if (node.getLocation().isPresent()) {
      return new FunctionCall(node.getLocation().get(),
          node.getName(),
          window,
          node.isDistinct(),
          node.getArguments()
              .stream()
              .map(arg -> (Expression) process(arg, context))
              .collect(Collectors.toList())
      );
    } else {
      return new FunctionCall(node.getName(),
          window,
          node.isDistinct(),
          node.getArguments()
              .stream()
              .map(arg -> (Expression) process(arg, context))
              .collect(Collectors.toList())
      );
    }

  }

  protected Node visitLambdaExpression(LambdaExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new LambdaExpression(node.getLocation().get(),
          node.getArguments(),
          (Expression) process(node.getBody(), context)
      );
    } else {
      return new LambdaExpression(node.getArguments(),
          (Expression) process(node.getBody(), context)
      );
    }
  }

  protected Node visitSimpleCaseExpression(SimpleCaseExpression node, Object context) {
    Optional<Expression> defaultValue = node.getDefaultValue().isPresent()
        ? Optional.ofNullable((Expression) process(node.getDefaultValue().get(), context))
        : Optional.empty();
    if (node.getLocation().isPresent()) {
      return new SimpleCaseExpression(node.getLocation().get(),
          (Expression) process(node.getOperand(), context),
          node.getWhenClauses()
              .stream()
              .map(whenClause ->
                  (WhenClause) process(whenClause, context))
              .collect(Collectors.toList()),
          defaultValue
      );
    } else {
      return new SimpleCaseExpression((Expression) process(node.getOperand(), context),
          node.getWhenClauses()
              .stream()
              .map(whenClause ->
                  (WhenClause) process(whenClause, context))
              .collect(Collectors.toList()),
          defaultValue
      );
    }

  }

  protected Node visitStringLiteral(StringLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new StringLiteral(node.getLocation().get(),
          node.getValue());
    } else {
      return new StringLiteral(node.getValue());
    }

  }

  protected Node visitBinaryLiteral(BinaryLiteral node, Object context) {
    // May need some changes.
    if (node.getLocation().isPresent()) {
      return new BinaryLiteral(node.getLocation().get(), node.getValue().toString());
    } else {
      return new BinaryLiteral(node.getValue().toString());
    }

  }

  protected Node visitBooleanLiteral(BooleanLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new BooleanLiteral(node.getLocation().get(), String.valueOf(node.getValue()));
    } else {
      return new BooleanLiteral(String.valueOf(node.getValue()));
    }


  }

  protected Node visitInListExpression(InListExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new InListExpression(node.getLocation().get(),
          node.getValues().stream()
              .map(value -> (Expression) process(value, context))
              .collect(Collectors.toList())
      );
    } else {
      return new InListExpression(node.getValues().stream()
          .map(value -> (Expression) process(value, context))
          .collect(Collectors.toList())
      );
    }

  }

  protected Node visitQualifiedNameReference(QualifiedNameReference node, Object context) {
    if (node.getLocation().isPresent()) {
      return new QualifiedNameReference(node.getLocation().get(),
          node.getName());
    } else {
      return new QualifiedNameReference(node.getName());
    }

  }

  protected Node visitDereferenceExpression(DereferenceExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new DereferenceExpression(node.getLocation().get(),
          (Expression) process(node.getBase(), context),
          node.getFieldName()
      );
    } else {
      return new DereferenceExpression((Expression) process(node.getBase(), context),
          node.getFieldName()
      );
    }

  }

  protected Node visitNullIfExpression(NullIfExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new NullIfExpression(node.getLocation().get(),
          (Expression) process(node.getFirst(), context),
          (Expression) process(node.getSecond(), context));
    } else {
      return new NullIfExpression((Expression) process(node.getFirst(), context),
          (Expression) process(node.getSecond(), context));
    }

  }

  protected Node visitNullLiteral(NullLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new NullLiteral(node.getLocation().get());
    } else {
      return new NullLiteral();
    }

  }

  protected Node visitArithmeticUnary(ArithmeticUnaryExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new ArithmeticUnaryExpression(
          node.getLocation().get(),
          node.getSign(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new ArithmeticUnaryExpression(
          node.getSign(),
          (Expression) process(node.getValue(), context)
      );
    }

  }

  protected Node visitNotExpression(NotExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new NotExpression(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new NotExpression((Expression) process(node.getValue(), context)
      );
    }

  }

  protected Node visitSelectItem(SelectItem node, Object context) {
    return visitNode(node, context);
  }

  protected Node visitSingleColumn(SingleColumn node, Object context) {

    if (node.getLocation().isPresent()) {
      return new SingleColumn(node.getLocation().get(),
          (Expression) process(node.getExpression(), context),
          node.getAlias()
      );
    } else {
      return new SingleColumn(
          (Expression) process(node.getExpression(), context),
          node.getAlias()
      );
    }

  }

  protected Node visitAllColumns(AllColumns node, Object context) {
    if (node.getLocation().isPresent()) {
      if (node.getPrefix().isPresent()) {
        return new AllColumns(node.getLocation().get(), node.getPrefix().get());
      } else {
        return new AllColumns(node.getLocation().get());
      }

    } else {
      if (node.getPrefix().isPresent()) {
        return new AllColumns(node.getLocation().get());
      } else {
        throw new KsqlException("Cannot have both localtion and prefix null in AllColumns AST "
            + "node.");
      }

    }

  }

  protected Node visitSearchedCaseExpression(SearchedCaseExpression node, Object context) {
    Optional<Expression> defaultValue = node.getDefaultValue().isPresent()
        ? Optional.ofNullable((Expression) process(node.getDefaultValue().get(), context))
        : Optional.empty();
    if (node.getLocation().isPresent()) {
      return new SearchedCaseExpression(
          node.getLocation().get(),
          node.getWhenClauses()
              .stream().map(whenClause -> (WhenClause) process(whenClause, context))
              .collect(Collectors.toList()),
          defaultValue
      );
    } else {
      return new SearchedCaseExpression(
          node.getWhenClauses()
              .stream().map(whenClause -> (WhenClause) process(whenClause, context))
              .collect(Collectors.toList()),
          defaultValue
      );
    }

  }

  protected Node visitLikePredicate(LikePredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new LikePredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context),
          (Expression) process(node.getPattern(), context),
          node.getEscape() != null
              ? (Expression) process(node.getEscape(), context)
              : null
      );
    } else {
      return new LikePredicate((Expression) process(node.getValue(), context),
          (Expression) process(node.getPattern(), context),
          node.getEscape() != null
              ? (Expression) process(node.getEscape(), context)
              : null
      );
    }

  }

  protected Node visitIsNotNullPredicate(IsNotNullPredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new IsNotNullPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new IsNotNullPredicate((Expression) process(node.getValue(), context)
      );
    }

  }

  protected Node visitIsNullPredicate(IsNullPredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new IsNullPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new IsNullPredicate((Expression) process(node.getValue(), context)
      );
    }

  }

  protected Node visitSubscriptExpression(SubscriptExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new SubscriptExpression(node.getLocation().get(),
          (Expression) process(node.getBase(), context),
          (Expression) process(node.getIndex(), context)
      );
    } else {
      return new SubscriptExpression((Expression) process(node.getBase(), context),
          (Expression) process(node.getIndex(), context)
      );
    }

  }

  protected Node visitLongLiteral(LongLiteral node, Object context) {
    if (node.getLocation().isPresent()) {
      return new LongLiteral(node.getLocation().get(), String.valueOf(node.getValue()));
    } else {
      return new LongLiteral(String.valueOf(node.getValue()));
    }

  }

  protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new LogicalBinaryExpression(node.getLocation().get(),
          node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context)
      );
    } else {
      return new LogicalBinaryExpression(node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context)
      );
    }

  }

  protected Node visitSubqueryExpression(SubqueryExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new SubqueryExpression(node.getLocation().get(),
          (Query) process(node.getQuery(), context));
    } else {
      return new SubqueryExpression((Query) process(node.getQuery(), context));
    }

  }

  protected Node visitTable(Table node, Object context) {
    Map<String, Expression> properties = node.getProperties().entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> (Expression) process(e.getValue(), context)
        ));
    if (node.getLocation().isPresent()) {
      return new Table(node.getLocation().get(),
          node.getName(),
          node.isStdOut,
          properties);
    } else {
      return new Table(node.getName(),
          node.isStdOut,
          properties);
    }

  }

  protected Node visitValues(Values node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Values(node.getLocation().get(),
          node.getRows().stream()
              .map(row -> (Expression) process(row, context))
              .collect(Collectors.toList())
      );
    } else {
      return new Values(node.getRows().stream()
          .map(row -> (Expression) process(row, context))
          .collect(Collectors.toList())
      );
    }

  }

  protected Node visitStruct(Struct node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Struct(node.getLocation().get(),
          node.getItems());
    } else {
      return new Struct(node.getItems());
    }

  }

  protected Node visitTableSubquery(TableSubquery node, Object context) {
    if (node.getLocation().isPresent()) {
      return new TableSubquery(node.getLocation().get(),
          (Query) process(node.getQuery(), context)
      );
    } else {
      return new TableSubquery((Query) process(node.getQuery(), context)
      );
    }

  }

  protected Node visitAliasedRelation(AliasedRelation node, Object context) {
    if (node.getLocation().isPresent()) {
      return new AliasedRelation(node.getLocation().get(),
          (Relation) process(node.getRelation(), context),
          node.getAlias(),
          node.getColumnNames());
    } else {
      return new AliasedRelation((Relation) process(node.getRelation(), context),
          node.getAlias(),
          node.getColumnNames());
    }

  }

  protected Node visitSampledRelation(SampledRelation node, Object context) {

    Optional<List<Expression>> columnsToStratifyOn = node.getColumnsToStratifyOn().isPresent()
        ? Optional.ofNullable(
        node.getColumnsToStratifyOn().get()
            .stream()
            .map(expression -> (Expression) process(expression, context))
            .collect(Collectors.toList()))
        : Optional.empty();
    if (node.getLocation().isPresent()) {
      return new SampledRelation(node.getLocation().get(),
          (Relation) process(node.getRelation(), context),
          node.getType(),
          (Expression) process(node.getSamplePercentage(), context),
          node.isRescaled(),
          columnsToStratifyOn
      );
    } else {
      return new SampledRelation((Relation) process(node.getRelation(), context),
          node.getType(),
          (Expression) process(node.getSamplePercentage(), context),
          node.isRescaled(),
          columnsToStratifyOn
      );
    }

  }

  protected Node visitJoin(Join node, Object context) {
    //TODO: Will have to look into Criteria later (includes Expression)
    if (node.getLocation().isPresent()) {
      return new Join(node.getLocation().get(),
          node.getType(),
          (Relation) process(node.getLeft(), context),
          (Relation) process(node.getRight(), context),
          node.getCriteria()
      );
    } else {
      return new Join(node.getType(),
          (Relation) process(node.getLeft(), context),
          (Relation) process(node.getRight(), context),
          node.getCriteria()
      );
    }

  }

  protected Node visitExists(ExistsPredicate node, Object context) {
    if (node.getLocation().isPresent()) {
      return new ExistsPredicate(
          node.getLocation().get(),
          (Query) process(node.getSubquery(), context)
      );
    } else {
      return new ExistsPredicate((Query) process(node.getSubquery(), context)
      );
    }

  }

  protected Node visitCast(Cast node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Cast(
          node.getLocation().get(),
          (Expression) process(node.getExpression(), context),
          node.getType()
      );
    } else {
      return new Cast((Expression) process(node.getExpression(), context),
          node.getType()
      );
    }

  }

  protected Node visitFieldReference(FieldReference node, Object context) {
    return new FieldReference(node.getFieldIndex());
  }

  protected Node visitWindowExpression(WindowExpression node, Object context) {
    if (node.getLocation().isPresent()) {
      return new WindowExpression(
          node.getLocation(),
          node.getWindowName(),
          (KsqlWindowExpression) process(node.getKsqlWindowExpression(), context));
    } else {
      return new WindowExpression(
          node.getWindowName(),
          (KsqlWindowExpression) process(node.getKsqlWindowExpression(), context));
    }

  }

  protected Node visitTumblingWindowExpression(TumblingWindowExpression node, Object context) {
    return new TumblingWindowExpression(node.getSize(), node.getSizeUnit());
  }

  protected Node visitHoppingWindowExpression(HoppingWindowExpression node, Object context) {
    return new HoppingWindowExpression(
        node.getSize(),
        node.getSizeUnit(),
        node.getAdvanceBy(),
        node.getAdvanceByUnit());
  }

  protected Node visitSessionWindowExpression(SessionWindowExpression node, Object context) {
    return new SessionWindowExpression(node.getGap(),
        node.getSizeUnit());
  }

  protected Node visitWindow(Window node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Window(node.getLocation().get(),
          node.getWindowName(),
          (WindowExpression) process(node.getWindowExpression(), context)
      );
    } else {
      return new Window(node.getWindowName(),
          (WindowExpression) process(node.getWindowExpression(), context)
      );
    }

  }

  protected Node visitWindowFrame(WindowFrame node, Object context) {
    if (node.getLocation().isPresent()) {
      return new WindowFrame(
          node.getLocation().get(),
          node.getType(),
          (FrameBound) process(node.getStart(), context),
          node.getEnd().isPresent()
              ? Optional.ofNullable((FrameBound) process(node.getEnd().get(), context))
              : Optional.empty()
      );
    } else {
      return new WindowFrame(
          node.getType(),
          (FrameBound) process(node.getStart(), context),
          node.getEnd().isPresent()
              ? Optional.ofNullable((FrameBound) process(node.getEnd().get(), context))
              : Optional.empty()
      );
    }
  }

  protected Node visitFrameBound(FrameBound node, Object context) {
    if (node.getLocation().isPresent()) {
      return new FrameBound(node.getLocation().get(),
          node.getType(),
          node.getValue().isPresent()
              ? (Expression) process(node.getValue().get(), context)
              : null
      );
    } else {
      return new FrameBound(node.getType(),
          node.getValue().isPresent()
              ? (Expression) process(node.getValue().get(), context)
              : null
      );
    }
  }

  protected Node visitTableElement(TableElement node, Object context) {
    if (node.getLocation().isPresent()) {
      return new TableElement(node.getLocation().get(),
          node.getName(),
          node.getType());
    } else {
      return new TableElement(node.getName(),
          node.getType());
    }
  }

  protected Node visitCreateStream(CreateStream node, Object context) {
    if (node.getLocation().isPresent()) {
      return new CreateStream(
          node.getLocation().get(),
          node.getName(),
          node.getElements().stream()
              .map(tableElement -> (TableElement) process(tableElement, context))
              .collect(Collectors.toList()),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              ))
      );
    } else {
      return new CreateStream(
          node.getName(),
          node.getElements().stream()
              .map(tableElement -> (TableElement) process(tableElement, context))
              .collect(Collectors.toList()),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              ))
      );
    }
  }

  protected Node visitCreateStreamAsSelect(CreateStreamAsSelect node, Object context) {
    if (node.getLocation().isPresent()) {
      return new CreateStreamAsSelect(node.getLocation().get(),
          node.getName(),
          (Query) process(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )),
          node.getPartitionByColumn().isPresent()
              ? Optional.ofNullable(
              (Expression) process(node.getPartitionByColumn().get(),
                  context))
              : Optional.empty()
      );
    } else {
      return new CreateStreamAsSelect(node.getName(),
          (Query) process(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )),
          node.getPartitionByColumn().isPresent()
              ? Optional.ofNullable(
              (Expression) process(node.getPartitionByColumn().get(),
                  context))
              : Optional.empty()
      );
    }
  }

  protected Node visitCreateTable(CreateTable node, Object context) {
    if (node.getLocation().isPresent()) {
      return new CreateTable(node.getLocation().get(),
          node.getName(),
          node.getElements().stream()
              .map(tableElement -> (TableElement) process(tableElement, context))
              .collect(Collectors.toList()),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )));
    } else {
      return new CreateTable(node.getName(),
          node.getElements().stream()
              .map(tableElement -> (TableElement) process(tableElement, context))
              .collect(Collectors.toList()),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )));
    }

  }

  protected Node visitCreateTableAsSelect(CreateTableAsSelect node, Object context) {
    if (node.getLocation().isPresent()) {
      return new CreateTableAsSelect(node.getLocation().get(),
          node.getName(),
          (Query) process(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )));
    } else {
      return new CreateTableAsSelect(node.getName(),
          (Query) process(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties().entrySet().stream()
              .collect(Collectors.toMap(
                  Entry::getKey,
                  e -> (Expression) process(e.getValue(), context)
              )));
    }

  }

  protected Node visitInsertInto(InsertInto node, Object context) {
    if (node.getLocation().isPresent()) {
      return new InsertInto(node.getLocation().get(),
          node.getTarget(),
          (Query) process(node.getQuery(), context),
          node.getPartitionByColumn().isPresent()
              ? Optional.ofNullable(
              (Expression) process(node.getPartitionByColumn().get(),
                  context))
              : Optional.empty());
    } else {
      return new InsertInto(node.getTarget(),
          (Query) process(node.getQuery(), context),
          node.getPartitionByColumn().isPresent()
              ? Optional.ofNullable(
              (Expression) process(node.getPartitionByColumn().get(),
                  context))
              : Optional.empty());
    }
  }

  protected Node visitDropTable(DropTable node, Object context) {
    if (node.getLocation().isPresent()) {
      return new DropTable(node.getLocation().get(),
          node.getTableName(),
          node.getIfExists(),
          node.isDeleteTopic());
    } else {
      return new DropTable(node.getTableName(),
          node.getIfExists(),
          node.isDeleteTopic());
    }
  }

  protected Node visitRenameColumn(RenameColumn node, Object context) {
    if (node.getLocation().isPresent()) {
      return new RenameColumn(node.getLocation().get(),
          node.getTable(),
          node.getSource(),
          node.getTarget()
      );
    } else {
      return new RenameColumn(node.getTable(),
          node.getSource(),
          node.getTarget()
      );
    }

  }

  protected Node visitCreateView(CreateView node, Object context) {
    return visitStatement(node, context);
  }

  protected Node visitDropView(DropView node, Object context) {
    if (node.getLocation().isPresent()) {
      return new DropView(node.getLocation().get(),
          node.getName(),
          node.isExists());
    } else {
      return new DropView(node.getName(),
          node.isExists());
    }

  }

  protected Node visitDelete(Delete node, Object context) {
    if (node.getLocation().isPresent()) {
      return new Delete(node.getLocation().get(),
          (Table) process(node.getTable(), context),
          node.getWhere().isPresent()
              ? Optional.ofNullable((Expression) process(node.getWhere().get(), context))
              : Optional.empty()
      );
    } else {
      return new Delete((Table) process(node.getTable(), context),
          node.getWhere().isPresent()
              ? Optional.ofNullable((Expression) process(node.getWhere().get(), context))
              : Optional.empty()
      );
    }

  }

  protected Node visitGroupBy(GroupBy node, Object context) {
    if (node.getLocation().isPresent()) {
      return new GroupBy(
          node.getLocation().get(),
          node.isDistinct(),
          node.getGroupingElements().stream()
              .map(groupingElement -> (GroupingElement) process(groupingElement, context))
              .collect(Collectors.toList())
      );
    } else {
      return new GroupBy(
          node.isDistinct(),
          node.getGroupingElements().stream()
              .map(groupingElement -> (GroupingElement) process(groupingElement, context))
              .collect(Collectors.toList())
      );
    }

  }

  protected Node visitGroupingElement(GroupingElement node, Object context) {
    return visitNode(node, context);
  }

  protected Node visitSimpleGroupBy(SimpleGroupBy node, Object context) {
    if (node.getLocation().isPresent()) {
      return new SimpleGroupBy(node.getLocation().get(),
          node.getColumnExpressions().stream()
              .map(ce -> (Expression) process(ce, context))
              .collect(Collectors.toList())
      );
    } else {
      return new SimpleGroupBy(node.getColumnExpressions().stream()
          .map(ce -> (Expression) process(ce, context))
          .collect(Collectors.toList())
      );
    }

  }

  protected Node visitSymbolReference(SymbolReference node, Object context) {
    return new SymbolReference(node.getName());
  }
}