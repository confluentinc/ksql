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
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.Delete;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropView;
import io.confluent.ksql.parser.tree.ExistsPredicate;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FieldReference;
import io.confluent.ksql.parser.tree.FrameBound;
import io.confluent.ksql.parser.tree.FunctionCall;
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
import io.confluent.ksql.parser.tree.LikePredicate;
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


/**
 * This class will create a new AST given the input AST. The new AST is exactly the clone of
 * the imput one. If you want to rewrite a query by changing the AST you can inherit from this
 * class and implemet the changes for the nodes you need. The newly generated tree will include
 * your changes and the rest of the tree will remain the same.
 * 
 */
public class StatementRewriter extends DefaultAstVisitor<Node, Object> {

  protected Node visitExpression(final Expression node, final Object context) {
    return node;
  }

  protected Node visitExtract(final Extract node, final Object context) {

    if (node.getLocation().isPresent()) {
      return new Extract(node.getLocation().get(),
          (Expression) process(node.getExpression(), context),
          node.getField());
    } else {
      return new Extract((Expression) process(node.getExpression(), context),
          node.getField());
    }

  }

  protected Node visitArithmeticBinary(
      final ArithmeticBinaryExpression node,
      final Object context
  ) {
    return node.getLocation()
        .map(location ->
            new ArithmeticBinaryExpression(location, node.getType(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context)))
        .orElse(new ArithmeticBinaryExpression(node.getType(),
            (Expression) process(node.getLeft(), context),
            (Expression) process(node.getRight(), context)));

  }

  protected Node visitBetweenPredicate(final BetweenPredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new BetweenPredicate(node.getLocation().get(),
                (Expression) process(node.getValue(), context),
                (Expression) process(node.getMin(), context),
                (Expression) process(node.getMax(), context))
        )
        .orElse(
            new BetweenPredicate((Expression) process(node.getValue(), context),
                (Expression) process(node.getMin(), context),
                (Expression) process(node.getMax(), context))
        );

  }

  protected Node visitComparisonExpression(final ComparisonExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new ComparisonExpression(node.getLocation().get(),
                node.getType(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context))
        )
        .orElse(
            new ComparisonExpression(node.getType(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context))
        );

  }

  protected Node visitDoubleLiteral(final DoubleLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new DoubleLiteral(node.getLocation().get(), String.valueOf(node.getValue()))
        )
        .orElse(
            new DoubleLiteral(String.valueOf(node.getValue()))
        );
  }

  protected Node visitDecimalLiteral(final DecimalLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new DecimalLiteral(node.getLocation().get(), node.getValue())
        )
        .orElse(
            new DecimalLiteral(node.getValue())
        );
  }

  protected Node visitStatements(Statements node, final Object context) {
    return new Statements(
        node.statementList
            .stream()
            .map(s -> (Statement) process(s, context))
            .collect(Collectors.toList())
    );
  }


  protected Node visitQuery(final Query node, final Object context) {
    return new Query((QueryBody) process(node.getQueryBody(), context),
        node.getLimit());
  }

  protected Node visitTimeLiteral(final TimeLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new TimeLiteral(node.getLocation().get(), node.getValue())
        )
        .orElse(
            new TimeLiteral(node.getValue())
        );

  }

  protected Node visitWithQuery(final WithQuery node, final Object context) {
    return node.getLocation()
        .map(location ->
            new WithQuery(node.getLocation().get(),
                node.getName(),
                (Query) process(node.getQuery(), context),
                node.getColumnNames())
        )
        .orElse(
            new WithQuery(node.getName(),
                (Query) process(node.getQuery(), context),
                node.getColumnNames())
        );

  }

  protected Node visitSelect(final Select node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Select(node.getLocation().get(),
                node.isDistinct(),
                node.getSelectItems()
                    .stream()
                    .map(selectItem -> (SelectItem) process(selectItem, context))
                    .collect(Collectors.toList()))
        )
        .orElse(
            new Select(node.isDistinct(),
                node.getSelectItems()
                    .stream()
                    .map(selectItem -> (SelectItem) process(selectItem, context))
                    .collect(Collectors.toList())
            )
        );
  }

  protected Node visitQuerySpecification(final QuerySpecification node, final Object context) {
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

    return node.getLocation()
        .map(location ->
            new QuerySpecification(
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
            )
        )
        .orElse(
            new QuerySpecification(
                (Select) process(node.getSelect(), context),
                (Relation) process(node.getInto(), context),
                node.isShouldCreateInto(),
                (Relation) process(node.getFrom(), context),
                windowExpression,
                where,
                groupBy,
                having,
                node.getLimit()
            )
        );
  }

  protected Node visitTimestampLiteral(TimestampLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new TimeLiteral(node.getLocation().get(), node.getValue())
        )
        .orElse(
            new TimeLiteral(node.getValue())
        );
  }

  protected Node visitWhenClause(final WhenClause node, final Object context) {
    return node.getLocation()
        .map(location ->
            new WhenClause(node.getLocation().get(),
                (Expression) process(node.getOperand(), context),
                (Expression) process(node.getResult(), context))
        )
        .orElse(
            new WhenClause((Expression) process(node.getOperand(), context),
                (Expression) process(node.getResult(), context))
        );
  }

  protected Node visitIntervalLiteral(final IntervalLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new IntervalLiteral(node.getLocation().get(),
                node.getValue(),
                node.getSign(),
                node.getStartField(),
                node.getEndField()
            )
        )
        .orElse(
            new IntervalLiteral(node.getValue(),
                node.getSign(),
                node.getStartField(),
                node.getEndField()
            )
        );
  }

  protected Node visitInPredicate(final InPredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new InPredicate(node.getLocation().get(),
                (Expression) process(node.getValue(), context),
                (Expression) process(node.getValueList(), context))
        )
        .orElse(
            new InPredicate((Expression) process(node.getValue(), context),
                (Expression) process(node.getValueList(), context))
        );
  }

  protected Node visitFunctionCall(final FunctionCall node, final Object context) {
    Optional<Window> window = node.getWindow().isPresent()
        ? Optional.ofNullable((Window) process(node.getWindow().get(), context))
        : Optional.empty();

    return node.getLocation()
        .map(location ->
            new FunctionCall(node.getLocation().get(),
                node.getName(),
                window,
                node.isDistinct(),
                node.getArguments()
                    .stream()
                    .map(arg -> (Expression) process(arg, context))
                    .collect(Collectors.toList())
            )
        )
        .orElse(
            new FunctionCall(node.getName(),
                window,
                node.isDistinct(),
                node.getArguments()
                    .stream()
                    .map(arg -> (Expression) process(arg, context))
                    .collect(Collectors.toList())
            )
        );
  }

  protected Node visitSimpleCaseExpression(final SimpleCaseExpression node, final Object context) {
    Optional<Expression> defaultValue = node.getDefaultValue().isPresent()
        ? Optional.ofNullable((Expression) process(node.getDefaultValue().get(), context))
        : Optional.empty();
    return node.getLocation()
        .map(location ->
            new SimpleCaseExpression(node.getLocation().get(),
                (Expression) process(node.getOperand(), context),
                node.getWhenClauses()
                    .stream()
                    .map(whenClause ->
                        (WhenClause) process(whenClause, context))
                    .collect(Collectors.toList()),
                defaultValue
            )
        )
        .orElse(
            new SimpleCaseExpression((Expression) process(node.getOperand(), context),
                node.getWhenClauses()
                    .stream()
                    .map(whenClause ->
                        (WhenClause) process(whenClause, context))
                    .collect(Collectors.toList()),
                defaultValue
            )
        );
  }

  protected Node visitStringLiteral(final StringLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new StringLiteral(node.getLocation().get(),
                node.getValue())
        )
        .orElse(
            new StringLiteral(node.getValue())
        );
  }

  protected Node visitBinaryLiteral(final BinaryLiteral node, final Object context) {
    // May need some changes.
    return node.getLocation()
        .map(location ->
            new BinaryLiteral(node.getLocation().get(), node.getValue().toString())
        )
        .orElse(
            new BinaryLiteral(node.getValue().toString())
        );
  }

  protected Node visitBooleanLiteral(final BooleanLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new BooleanLiteral(node.getLocation().get(), String.valueOf(node.getValue()))
        )
        .orElse(
            new BooleanLiteral(String.valueOf(node.getValue()))
        );

  }

  protected Node visitInListExpression(final InListExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new InListExpression(node.getLocation().get(),
                node.getValues().stream()
                    .map(value -> (Expression) process(value, context))
                    .collect(Collectors.toList())
            )
        )
        .orElse(
            new InListExpression(node.getValues().stream()
                .map(value -> (Expression) process(value, context))
                .collect(Collectors.toList())
            )
        );
  }

  protected Node visitQualifiedNameReference(
      final QualifiedNameReference node,
      final Object context
  ) {
    return node.getLocation()
        .map(location ->
            new QualifiedNameReference(node.getLocation().get(),
                node.getName())
        )
        .orElse(
            new QualifiedNameReference(node.getName())
        );
  }

  protected Node visitDereferenceExpression(
      final DereferenceExpression node,
      final Object context
  ) {
    return node.getLocation()
        .map(location ->
            new DereferenceExpression(node.getLocation().get(),
                (Expression) process(node.getBase(), context),
                node.getFieldName()
            )
        )
        .orElse(
            new DereferenceExpression((Expression) process(node.getBase(), context),
                node.getFieldName()
            )
        );
  }

  protected Node visitNullIfExpression(final NullIfExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new NullIfExpression(node.getLocation().get(),
                (Expression) process(node.getFirst(), context),
                (Expression) process(node.getSecond(), context))
        )
        .orElse(
            new NullIfExpression((Expression) process(node.getFirst(), context),
                (Expression) process(node.getSecond(), context))
        );
  }

  protected Node visitNullLiteral(final NullLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new NullLiteral(node.getLocation().get())
        )
        .orElse(
            new NullLiteral()
        );
  }

  protected Node visitArithmeticUnary(final ArithmeticUnaryExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new ArithmeticUnaryExpression(
                node.getLocation().get(),
                node.getSign(),
                (Expression) process(node.getValue(), context)
            )
        )
        .orElse(
            new ArithmeticUnaryExpression(
                node.getSign(),
                (Expression) process(node.getValue(), context)
            )
        );
  }

  protected Node visitNotExpression(final NotExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new NotExpression(node.getLocation().get(),
                (Expression) process(node.getValue(), context)
            )
        )
        .orElse(
            new NotExpression((Expression) process(node.getValue(), context))
        );
  }

  protected Node visitSingleColumn(final SingleColumn node, final Object context) {
    return node.getLocation()
        .map(location ->
            new SingleColumn(node.getLocation().get(),
                (Expression) process(node.getExpression(), context),
                node.getAlias()
            )
        )
        .orElse(
            new SingleColumn(
                (Expression) process(node.getExpression(), context),
                node.getAlias()
            )
        );
  }

  protected Node visitAllColumns(final AllColumns node, final Object context) {
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
        throw new KsqlException("Cannot have both location and prefix null in AllColumns AST "
            + "node.");
      }

    }
  }

  protected Node visitSearchedCaseExpression(
      final SearchedCaseExpression node,
      final Object context
  ) {
    final Optional<Expression> defaultValue = node.getDefaultValue().isPresent()
        ? Optional.ofNullable((Expression) process(node.getDefaultValue().get(), context))
        : Optional.empty();
    return node.getLocation()
        .map(location ->
            new SearchedCaseExpression(
                node.getLocation().get(),
                node.getWhenClauses()
                    .stream().map(whenClause -> (WhenClause) process(whenClause, context))
                    .collect(Collectors.toList()),
                defaultValue
            )
        )
        .orElse(
            new SearchedCaseExpression(
                node.getWhenClauses()
                    .stream().map(whenClause -> (WhenClause) process(whenClause, context))
                    .collect(Collectors.toList()),
                defaultValue
            )
        );
  }

  protected Node visitLikePredicate(final LikePredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new LikePredicate(node.getLocation().get(),
                (Expression) process(node.getValue(), context),
                (Expression) process(node.getPattern(), context),
                node.getEscape() != null
                    ? (Expression) process(node.getEscape(), context)
                    : null
            )
        )
        .orElse(
            new LikePredicate((Expression) process(node.getValue(), context),
                (Expression) process(node.getPattern(), context),
                node.getEscape() != null
                    ? (Expression) process(node.getEscape(), context)
                    : null
            )
        );
  }

  protected Node visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new IsNotNullPredicate(node.getLocation().get(),
                (Expression) process(node.getValue(), context)
            )
        )
        .orElse(
            new IsNotNullPredicate((Expression) process(node.getValue(), context))
        );
  }

  protected Node visitIsNullPredicate(final IsNullPredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new IsNullPredicate(node.getLocation().get(),
                (Expression) process(node.getValue(), context)
            )
        )
        .orElse(
            new IsNullPredicate((Expression) process(node.getValue(), context))
        );
  }

  protected Node visitSubscriptExpression(final SubscriptExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new SubscriptExpression(node.getLocation().get(),
                (Expression) process(node.getBase(), context),
                (Expression) process(node.getIndex(), context)
            )
        )
        .orElse(
            new SubscriptExpression((Expression) process(node.getBase(), context),
                (Expression) process(node.getIndex(), context)
            )
        );
  }

  protected Node visitLongLiteral(final LongLiteral node, final Object context) {
    return node.getLocation()
        .map(location ->
            new LongLiteral(node.getLocation().get(), node.getValue())
        )
        .orElse(
            new LongLiteral(node.getValue())
        );
  }

  protected Node visitLogicalBinaryExpression(
      final LogicalBinaryExpression node,
      final Object context
  ) {
    return node.getLocation()
        .map(location ->
            new LogicalBinaryExpression(node.getLocation().get(),
                node.getType(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context)
            )
        )
        .orElse(
            new LogicalBinaryExpression(node.getType(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context)
            )
        );
  }

  protected Node visitSubqueryExpression(final SubqueryExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new SubqueryExpression(node.getLocation().get(),
                (Query) process(node.getQuery(), context))
        )
        .orElse(
            new SubqueryExpression((Query) process(node.getQuery(), context))
        );
  }

  protected Node visitTable(final Table node, final Object context) {
    final Map<String, Expression> properties = node.getProperties().entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> (Expression) process(e.getValue(), context)
        ));
    return node.getLocation()
        .map(location ->
            new Table(node.getLocation().get(),
                node.getName(),
                node.isStdOut,
                properties)
        )
        .orElse(
            new Table(node.getName(),
                node.isStdOut,
                properties)
        );
  }

  protected Node visitValues(final Values node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Values(node.getLocation().get(),
                node.getRows().stream()
                    .map(row -> (Expression) process(row, context))
                    .collect(Collectors.toList())
            )
        )
        .orElse(
            new Values(node.getRows().stream()
                .map(row -> (Expression) process(row, context))
                .collect(Collectors.toList())
            )
        );
  }

  protected Node visitStruct(final Struct node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Struct(node.getLocation().get(),
                node.getItems())
        )
        .orElse(
            new Struct(node.getItems())
        );
  }

  protected Node visitTableSubquery(final TableSubquery node, final Object context) {
    return node.getLocation()
        .map(location ->
            new TableSubquery(node.getLocation().get(),
                (Query) process(node.getQuery(), context)
            )
        )
        .orElse(
            new TableSubquery((Query) process(node.getQuery(), context))
        );
  }

  protected Node visitAliasedRelation(final AliasedRelation node, final Object context) {
    return node.getLocation()
        .map(location ->
            new AliasedRelation(node.getLocation().get(),
                (Relation) process(node.getRelation(), context),
                node.getAlias(),
                node.getColumnNames())
        )
        .orElse(
            new AliasedRelation((Relation) process(node.getRelation(), context),
                node.getAlias(),
                node.getColumnNames())
        );
  }

  protected Node visitSampledRelation(final SampledRelation node, final Object context) {

    Optional<List<Expression>> columnsToStratifyOn = node.getColumnsToStratifyOn().isPresent()
        ? Optional.ofNullable(
        node.getColumnsToStratifyOn().get()
            .stream()
            .map(expression -> (Expression) process(expression, context))
            .collect(Collectors.toList()))
        : Optional.empty();
    return node.getLocation()
        .map(location ->
            new SampledRelation(node.getLocation().get(),
                (Relation) process(node.getRelation(), context),
                node.getType(),
                (Expression) process(node.getSamplePercentage(), context),
                node.isRescaled(),
                columnsToStratifyOn
            )
        )
        .orElse(
            new SampledRelation((Relation) process(node.getRelation(), context),
                node.getType(),
                (Expression) process(node.getSamplePercentage(), context),
                node.isRescaled(),
                columnsToStratifyOn
            )
        );
  }

  protected Node visitJoin(final Join node, final Object context) {
    //TODO: Will have to look into Criteria later (includes Expression)
    return node.getLocation()
        .map(location ->
            new Join(node.getLocation().get(),
                node.getType(),
                (Relation) process(node.getLeft(), context),
                (Relation) process(node.getRight(), context),
                node.getCriteria(), node.getWithinExpression()
            )
        )
        .orElse(
            new Join(node.getType(),
                (Relation) process(node.getLeft(), context),
                (Relation) process(node.getRight(), context),
                node.getCriteria()
            )
        );
  }

  protected Node visitExists(final ExistsPredicate node, final Object context) {
    return node.getLocation()
        .map(location ->
            new ExistsPredicate(
                node.getLocation().get(),
                (Query) process(node.getSubquery(), context)
            )
        )
        .orElse(
            new ExistsPredicate((Query) process(node.getSubquery(), context))
        );
  }

  protected Node visitCast(final Cast node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Cast(
                node.getLocation().get(),
                (Expression) process(node.getExpression(), context),
                node.getType()
            )
        )
        .orElse(
            new Cast((Expression) process(node.getExpression(), context),
                node.getType()
            )
        );
  }

  protected Node visitFieldReference(final FieldReference node, final Object context) {
    return new FieldReference(node.getFieldIndex());
  }

  protected Node visitWindowExpression(final WindowExpression node, final Object context) {
    return node.getLocation()
        .map(location ->
            new WindowExpression(
                node.getLocation(),
                node.getWindowName(),
                (KsqlWindowExpression) process(node.getKsqlWindowExpression(), context))
        )
        .orElse(
            new WindowExpression(
                node.getWindowName(),
                (KsqlWindowExpression) process(node.getKsqlWindowExpression(), context))
        );
  }

  protected Node visitTumblingWindowExpression(
      final TumblingWindowExpression node,
      final Object context
  ) {
    return new TumblingWindowExpression(node.getSize(), node.getSizeUnit());
  }

  protected Node visitHoppingWindowExpression(
      final HoppingWindowExpression node,
      final Object context
  ) {
    return new HoppingWindowExpression(
        node.getSize(),
        node.getSizeUnit(),
        node.getAdvanceBy(),
        node.getAdvanceByUnit());
  }

  protected Node visitSessionWindowExpression(
      final SessionWindowExpression node,
      final Object context
  ) {
    return new SessionWindowExpression(node.getGap(),
        node.getSizeUnit());
  }

  protected Node visitWindow(final Window node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Window(node.getLocation().get(),
                node.getWindowName(),
                (WindowExpression) process(node.getWindowExpression(), context)
            )
        )
        .orElse(
            new Window(node.getWindowName(),
                (WindowExpression) process(node.getWindowExpression(), context)
            )
        );
  }

  protected Node visitWindowFrame(final WindowFrame node, final Object context) {
    return node.getLocation()
        .map(location ->
            new WindowFrame(
                node.getLocation().get(),
                node.getType(),
                (FrameBound) process(node.getStart(), context),
                node.getEnd().isPresent()
                    ? Optional.ofNullable((FrameBound) process(node.getEnd().get(), context))
                    : Optional.empty()
            )
        )
        .orElse(
            new WindowFrame(
                node.getType(),
                (FrameBound) process(node.getStart(), context),
                node.getEnd().isPresent()
                    ? Optional.ofNullable((FrameBound) process(node.getEnd().get(), context))
                    : Optional.empty()
            )
        );
  }

  protected Node visitFrameBound(final FrameBound node, final Object context) {
    return node.getLocation()
        .map(location ->
            new FrameBound(node.getLocation().get(),
                node.getType(),
                node.getValue().isPresent()
                    ? (Expression) process(node.getValue().get(), context)
                    : null
            )
        )
        .orElse(
            new FrameBound(node.getType(),
                node.getValue().isPresent()
                    ? (Expression) process(node.getValue().get(), context)
                    : null
            )
        );
  }

  protected Node visitTableElement(final TableElement node, final Object context) {
    return node.getLocation()
        .map(location ->
            new TableElement(node.getLocation().get(),
                node.getName(),
                node.getType())
        )
        .orElse(
            new TableElement(node.getName(),
                node.getType())
        );
  }

  protected Node visitCreateStream(final CreateStream node, final Object context) {
    return new CreateStream(
        node.getLocation(),
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

  protected Node visitCreateStreamAsSelect(final CreateStreamAsSelect node, final Object context) {
    return new CreateStreamAsSelect(
        node.getLocation(),
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
  }

  protected Node visitCreateTable(final CreateTable node, final Object context) {
    return new CreateTable(node.getLocation(),
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
  }

  protected Node visitCreateTableAsSelect(final CreateTableAsSelect node, final Object context) {
    return new CreateTableAsSelect(node.getLocation(),
        node.getName(),
        (Query) process(node.getQuery(), context),
        node.isNotExists(),
        node.getProperties().entrySet().stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                e -> (Expression) process(e.getValue(), context)
            )));
  }

  protected Node visitInsertInto(final InsertInto node, final Object context) {
    return new InsertInto(node.getLocation(),
        node.getTarget(),
        (Query) process(node.getQuery(), context),
        node.getPartitionByColumn().isPresent()
            ? Optional.ofNullable(
            (Expression) process(node.getPartitionByColumn().get(),
                context))
            : Optional.empty());
  }

  protected Node visitDropTable(final DropTable node, final Object context) {
    return new DropTable(node.getLocation(),
        node.getTableName(),
        node.getIfExists(),
        node.isDeleteTopic());
  }

  protected Node visitRenameColumn(final RenameColumn node, final Object context) {
    return node.getLocation()
        .map(location ->
            new RenameColumn(node.getLocation().get(),
                node.getTable(),
                node.getSource(),
                node.getTarget()
            )
        )
        .orElse(
            new RenameColumn(node.getTable(),
                node.getSource(),
                node.getTarget()
            )
        );
  }

  protected Node visitDropView(final DropView node, final Object context) {
    return node.getLocation()
        .map(location ->
            new DropView(node.getLocation().get(),
                node.getName(),
                node.isExists())
        )
        .orElse(
            new DropView(node.getName(),
                node.isExists())
        );
  }

  protected Node visitDelete(final Delete node, final Object context) {
    return node.getLocation()
        .map(location ->
            new Delete(node.getLocation().get(),
                (Table) process(node.getTable(), context),
                node.getWhere().isPresent()
                    ? Optional.ofNullable((Expression) process(node.getWhere().get(), context))
                    : Optional.empty()
            )
        )
        .orElse(
            new Delete((Table) process(node.getTable(), context),
                node.getWhere().isPresent()
                    ? Optional.ofNullable((Expression) process(node.getWhere().get(), context))
                    : Optional.empty()
            )
        );
  }

  protected Node visitGroupBy(final GroupBy node, final Object context) {
    return node.getLocation()
        .map(location ->
            new GroupBy(
                node.getLocation().get(),
                node.isDistinct(),
                node.getGroupingElements().stream()
                    .map(groupingElement -> (GroupingElement) process(groupingElement, context))
                    .collect(Collectors.toList())
            )
        )
        .orElse(
            new GroupBy(
                node.isDistinct(),
                node.getGroupingElements().stream()
                    .map(groupingElement -> (GroupingElement) process(groupingElement, context))
                    .collect(Collectors.toList())
            )
        );
  }


  protected Node visitSimpleGroupBy(final SimpleGroupBy node, final Object context) {
    return node.getLocation()
        .map(location ->
            new SimpleGroupBy(node.getLocation().get(),
                node.getColumnExpressions().stream()
                    .map(ce -> (Expression) process(ce, context))
                    .collect(Collectors.toList())
            )
        )
        .orElse(
            new SimpleGroupBy(node.getColumnExpressions().stream()
                .map(ce -> (Expression) process(ce, context))
                .collect(Collectors.toList())
            )
        );
  }

  protected Node visitSymbolReference(final SymbolReference node, final Object context) {
    return new SymbolReference(node.getName());
  }
}