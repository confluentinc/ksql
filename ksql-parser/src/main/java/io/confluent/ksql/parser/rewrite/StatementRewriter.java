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

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * This class will create a new AST given the input AST, but with all expressions rewritten by
 * the provided expression rewriter. An expression rewriter is simply a class that implements
 * BiFunction&lt;Expression, C, Expression%gt;, and returns a rewritten expression given an
 * expression from the AST.
 */
public final class StatementRewriter<C> {

  private final Rewriter<C> rewriter;

  /**
   * Creates a new StatementRewriter that rewrites all expressions in a statement by
   * using the provided expression-rewriter.
   * @param expressionRewriter The expression rewriter used to rewrite an expression.
   */
  StatementRewriter(final BiFunction<Expression, C, Expression> expressionRewriter) {
    this.rewriter = new Rewriter<>(expressionRewriter);
  }

  // Exposed for testing
  StatementRewriter(
      final BiFunction<Expression, C, Expression> expressionRewriter,
      final BiFunction<AstNode, C, AstNode> rewriter) {
    this.rewriter = new Rewriter<>(expressionRewriter, rewriter);
  }

  public AstNode rewrite(final AstNode node, final C context) {
    return rewriter.process(node, context);
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class Rewriter<C> extends AstVisitor<AstNode, C> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
    private final BiFunction<Expression, C, Expression> expressionRewriter;
    private final BiFunction<AstNode, C, AstNode> rewriter;

    private Rewriter(final BiFunction<Expression, C, Expression> expressionRewriter) {
      this.expressionRewriter
          = Objects.requireNonNull(expressionRewriter, "expressionRewriter");
      this.rewriter = this::process;
    }

    private Rewriter(
        final BiFunction<Expression, C, Expression> expressionRewriter,
        final BiFunction<AstNode, C, AstNode> rewriter) {
      this.expressionRewriter
          = Objects.requireNonNull(expressionRewriter, "expressionRewriter");;
      this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
    }

    private Expression processExpression(final Expression node, final C context) {
      return expressionRewriter.apply(node, context);
    }

    @Override
    protected AstNode visitStatements(final Statements node, final C context) {
      final List<Statement> rewrittenStatements = node.getStatements()
          .stream()
          .map(s -> (Statement) rewriter.apply(s, context))
          .collect(Collectors.toList());

      return new Statements(
          node.getLocation(),
          rewrittenStatements
      );
    }

    @Override
    protected Query visitQuery(final Query node, final C context) {
      final Select select = (Select) rewriter.apply(node.getSelect(), context);

      final Relation from = (Relation) rewriter.apply(node.getFrom(), context);

      final Optional<WindowExpression> windowExpression = node.getWindow()
          .map(exp -> ((WindowExpression) rewriter.apply(exp, context)));

      final Optional<Expression> where = node.getWhere()
          .map(exp -> (processExpression(exp, context)));

      final Optional<GroupBy> groupBy = node.getGroupBy()
          .map(exp -> ((GroupBy) rewriter.apply(exp, context)));

      final Optional<Expression> having = node.getHaving()
          .map(exp -> (processExpression(exp, context)));

      return new Query(
          node.getLocation(),
          select,
          from,
          windowExpression,
          where,
          groupBy,
          having,
          node.getLimit()
      );
    }

    @Override
    protected AstNode visitSelect(final Select node, final C context) {
      final List<SelectItem> rewrittenItems = node.getSelectItems()
          .stream()
          .map(selectItem -> (SelectItem) rewriter.apply(selectItem, context))
          .collect(Collectors.toList());

      return new Select(
          node.getLocation(),
          rewrittenItems
      );
    }

    @Override
    protected AstNode visitSingleColumn(final SingleColumn node, final C context) {
      return node.copyWithExpression(processExpression(node.getExpression(), context));
    }

    @Override
    protected AstNode visitAllColumns(final AllColumns node, final C context) {
      return node;
    }

    @Override
    protected AstNode visitTable(final Table node, final C context) {
      return node;
    }

    @Override
    protected AstNode visitAliasedRelation(final AliasedRelation node, final C context) {
      final Relation rewrittenRelation = (Relation) rewriter.apply(node.getRelation(), context);

      return new AliasedRelation(
          node.getLocation(),
          rewrittenRelation,
          node.getAlias());
    }

    @Override
    protected AstNode visitJoin(final Join node, final C context) {
      final Relation rewrittenLeft = (Relation) rewriter.apply(node.getLeft(), context);
      final Relation rewrittenRight = (Relation) rewriter.apply(node.getRight(), context);
      final Optional<WithinExpression> rewrittenWithin = node.getWithinExpression()
          .map(within -> (WithinExpression) rewriter.apply(within, context));

      return new Join(
          node.getLocation(),
          node.getType(),
          rewrittenLeft,
          rewrittenRight,
          node.getCriteria(),
          rewrittenWithin);
    }

    @Override
    protected AstNode visitWithinExpression(final WithinExpression node, final C context) {
      return node;
    }

    @Override
    protected AstNode visitWindowExpression(final WindowExpression node, final C context) {
      return new WindowExpression(
          node.getLocation(),
          node.getWindowName(),
          (KsqlWindowExpression) rewriter.apply(node.getKsqlWindowExpression(), context));
    }

    @Override
    protected AstNode visitKsqlWindowExpression(
        final KsqlWindowExpression node,
        final C context) {
      return node;
    }

    @Override
    protected AstNode visitTableElement(final TableElement node, final C context) {
      return node;
    }

    @Override
    protected AstNode visitCreateStream(final CreateStream node, final C context) {
      final List<TableElement> rewrittenElements = node.getElements().stream()
          .map(tableElement -> (TableElement) rewriter.apply(tableElement, context))
          .collect(Collectors.toList());

      return node.copyWith(TableElements.of(rewrittenElements), node.getProperties());
    }

    @Override
    protected AstNode visitCreateStreamAsSelect(
        final CreateStreamAsSelect node,
        final C context) {
      final Optional<Expression> partitionBy = node.getPartitionByColumn()
          .map(exp -> processExpression(exp, context));

      return new CreateStreamAsSelect(
          node.getLocation(),
          node.getName(),
          (Query) rewriter.apply(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties(),
          partitionBy
      );
    }

    @Override
    protected AstNode visitCreateTable(final CreateTable node, final C context) {
      final List<TableElement> rewrittenElements = node.getElements().stream()
          .map(tableElement -> (TableElement) rewriter.apply(tableElement, context))
          .collect(Collectors.toList());

      return node.copyWith(TableElements.of(rewrittenElements), node.getProperties());
    }

    @Override
    protected AstNode visitCreateTableAsSelect(
        final CreateTableAsSelect node,
        final C context) {
      return new CreateTableAsSelect(
          node.getLocation(),
          node.getName(),
          (Query) rewriter.apply(node.getQuery(), context),
          node.isNotExists(),
          node.getProperties()
      );
    }

    @Override
    protected AstNode visitInsertInto(final InsertInto node, final C context) {
      final Optional<Expression> rewrittenPartitionBy = node.getPartitionByColumn()
          .map(exp -> processExpression(exp, context));

      return new InsertInto(
          node.getLocation(),
          node.getTarget(),
          (Query) rewriter.apply(node.getQuery(), context),
          rewrittenPartitionBy);
    }

    @Override
    protected AstNode visitDropTable(final DropTable node, final C context) {
      return node;
    }

    @Override
    protected AstNode visitGroupBy(final GroupBy node, final C context) {
      final List<GroupingElement> rewrittenGroupings = node.getGroupingElements().stream()
          .map(groupingElement -> (GroupingElement) rewriter.apply(groupingElement, context))
          .collect(Collectors.toList());

      return new GroupBy(node.getLocation(), rewrittenGroupings);
    }

    @Override
    protected AstNode visitSimpleGroupBy(final SimpleGroupBy node, final C context) {
      final List<Expression> columns = node.getColumns().stream()
          .map(ce -> processExpression(ce, context))
          .collect(Collectors.toList());

      return new SimpleGroupBy(
          node.getLocation(),
          columns
      );
    }
  }
}