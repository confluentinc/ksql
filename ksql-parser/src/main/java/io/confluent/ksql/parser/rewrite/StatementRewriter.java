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
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Expression;
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
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
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
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * This class will create a new AST given the input AST. The new AST is exactly the clone of
 * the imput one. If you want to rewrite a query by changing the AST you can inherit from this
 * class and implemet the changes for the nodes you need. The newly generated tree will include
 * your changes and the rest of the tree will remain the same.
 *
 * <p>Implementation note: make sure if you change this class you create each node only once in the
 * if/else blocks.
 * </p>
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class StatementRewriter extends DefaultAstVisitor<Node, Object> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  protected Node visitExpression(final Expression node, final Object context) {
    return node;
  }

  protected Node visitArithmeticBinary(
      final ArithmeticBinaryExpression node,
      final Object context
  ) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitBetweenPredicate(final BetweenPredicate node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitComparisonExpression(final ComparisonExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitDoubleLiteral(final DoubleLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new DoubleLiteral(node.getLocation().get(), String.valueOf(node.getValue()));
    } else {
      return new DoubleLiteral(String.valueOf(node.getValue()));
    }
  }

  protected Node visitDecimalLiteral(final DecimalLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new DecimalLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new DecimalLiteral(node.getValue());
    }
  }

  protected Node visitStatements(final Statements node, final Object context) {
    return new Statements(
        node.statementList
            .stream()
            .map(s -> (Statement) process(s, context))
            .collect(Collectors.toList())
    );
  }


  protected Query visitQuery(final Query node, final Object context) {
    final Optional<WindowExpression> windowExpression = node.getWindowExpression().isPresent()
        ? Optional.ofNullable((WindowExpression) process(node.getWindowExpression().get(), context))
        : Optional.empty();
    final Optional<Expression> where = node.getWhere().isPresent()
        ? Optional.ofNullable((Expression) process(node.getWhere().get(),
        context))
        : Optional.empty();

    final Optional<GroupBy> groupBy = node.getGroupBy().isPresent()
        ? Optional.ofNullable((GroupBy) process(node.getGroupBy().get(), context))
        : Optional.empty();

    final Optional<Expression> having = node.getHaving().isPresent()
        ? Optional.ofNullable((Expression) process(node.getHaving().get(), context))
        : Optional.empty();

    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new Query(
          node.getLocation().get(),
          (Select) process(node.getSelect(), context),
          (Relation) process(node.getFrom(), context),
          windowExpression,
          where,
          groupBy,
          having,
          node.getLimit()
      );
    } else {
      return new Query(
          (Select) process(node.getSelect(), context),
          (Relation) process(node.getFrom(), context),
          windowExpression,
          where,
          groupBy,
          having,
          node.getLimit()
      );
    }
  }

  protected Node visitTimeLiteral(final TimeLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new TimeLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new TimeLiteral(node.getValue());
    }
  }

  protected Node visitSelect(final Select node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new Select(node.getLocation().get(),
          node.isDistinct(),
          node.getSelectItems()
              .stream()
              .map(selectItem -> (SelectItem) process(selectItem, context))
              .collect(Collectors.toList()));
    } else {
      return new Select(node.isDistinct(),
          node.getSelectItems()
              .stream()
              .map(selectItem -> (SelectItem) process(selectItem, context))
              .collect(Collectors.toList())
      );
    }
  }

  protected Node visitTimestampLiteral(final TimestampLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new TimeLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new TimeLiteral(node.getValue());
    }
  }

  protected Node visitWhenClause(final WhenClause node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new WhenClause(node.getLocation().get(),
          (Expression) process(node.getOperand(), context),
          (Expression) process(node.getResult(), context));
    } else {
      return new WhenClause((Expression) process(node.getOperand(), context),
          (Expression) process(node.getResult(), context));
    }
  }

  protected Node visitIntervalLiteral(final IntervalLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitInPredicate(final InPredicate node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new InPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context),
          (Expression) process(node.getValueList(), context));
    } else {
      return new InPredicate((Expression) process(node.getValue(), context),
          (Expression) process(node.getValueList(), context));
    }
  }

  protected Node visitFunctionCall(final FunctionCall node, final Object context) {
    if (node.getLocation().isPresent()) {
      return new FunctionCall(
          node.getLocation().get(),
          node.getName(),
          node.isDistinct(),
          node.getArguments()
              .stream()
              .map(arg -> (Expression) process(arg, context))
              .collect(Collectors.toList())
      );
    } else {
      return new FunctionCall(
          node.getName(),
          node.isDistinct(),
          node.getArguments()
              .stream()
              .map(arg -> (Expression) process(arg, context))
              .collect(Collectors.toList())
      );
    }
  }

  protected Node visitSimpleCaseExpression(final SimpleCaseExpression node, final Object context) {
    final Optional<Expression> defaultValue = node.getDefaultValue().isPresent()
        ? Optional.ofNullable((Expression) process(node.getDefaultValue().get(), context))
        : Optional.empty();
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitStringLiteral(final StringLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new StringLiteral(node.getLocation().get(),
          node.getValue());
    } else {
      return new StringLiteral(node.getValue());
    }
  }

  protected Node visitBooleanLiteral(final BooleanLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new BooleanLiteral(node.getLocation().get(), String.valueOf(node.getValue()));
    } else {
      return new BooleanLiteral(String.valueOf(node.getValue()));
    }
  }

  protected Node visitInListExpression(final InListExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitQualifiedNameReference(
      final QualifiedNameReference node,
      final Object context
  ) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new QualifiedNameReference(node.getLocation().get(),
          node.getName());
    } else {
      return new QualifiedNameReference(node.getName());
    }
  }

  protected Node visitDereferenceExpression(
      final DereferenceExpression node,
      final Object context
  ) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitNullLiteral(final NullLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new NullLiteral(node.getLocation().get());
    } else {
      return new NullLiteral();
    }
  }

  protected Node visitArithmeticUnary(final ArithmeticUnaryExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitNotExpression(final NotExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new NotExpression(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new NotExpression((Expression) process(node.getValue(), context));
    }
  }

  protected Node visitSingleColumn(final SingleColumn node, final Object context) {
    return node.copyWithExpression((Expression) process(node.getExpression(), context));
  }

  protected Node visitAllColumns(final AllColumns node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitLikePredicate(final LikePredicate node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new IsNotNullPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new IsNotNullPredicate((Expression) process(node.getValue(), context));
    }
  }

  protected Node visitIsNullPredicate(final IsNullPredicate node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new IsNullPredicate(node.getLocation().get(),
          (Expression) process(node.getValue(), context)
      );
    } else {
      return new IsNullPredicate((Expression) process(node.getValue(), context));
    }
  }

  protected Node visitSubscriptExpression(final SubscriptExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitLongLiteral(final LongLiteral node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new LongLiteral(node.getLocation().get(), node.getValue());
    } else {
      return new LongLiteral(node.getValue());
    }
  }

  protected Node visitLogicalBinaryExpression(
      final LogicalBinaryExpression node,
      final Object context
  ) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitTable(final Table node, final Object context) {
    return node;
  }

  protected Node visitStruct(final Struct node, final Object context) {
    return Struct.builder()
        .addFields(node.getFields())
        .build();
  }

  protected Node visitAliasedRelation(final AliasedRelation node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new AliasedRelation(node.getLocation().get(),
          (Relation) process(node.getRelation(), context),
          node.getAlias());
    } else {
      return new AliasedRelation((Relation) process(node.getRelation(), context),
          node.getAlias());
    }
  }

  protected Node visitJoin(final Join node, final Object context) {
    //TODO: Will have to look into Criteria later (includes Expression)
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new Join(node.getLocation().get(),
          node.getType(),
          (Relation) process(node.getLeft(), context),
          (Relation) process(node.getRight(), context),
          node.getCriteria(), node.getWithinExpression()
      );
    } else {
      return new Join(node.getType(),
          (Relation) process(node.getLeft(), context),
          (Relation) process(node.getRight(), context),
          node.getCriteria()
      );
    }
  }

  protected Node visitCast(final Cast node, final Object context) {
    final Expression expression = (Expression) process(node.getExpression(), context);
    return new Cast(node.getLocation(), expression, node.getType());
  }

  protected Node visitWindowExpression(final WindowExpression node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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

  protected Node visitTableElement(final TableElement node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new TableElement(node.getLocation().get(),
          node.getName(),
          node.getType());
    } else {
      return new TableElement(node.getName(),
          node.getType());
    }
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
        (Table) process(node.getStream(), context),
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
        (Table) process(node.getTable(), context),
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
        (Table) process(node.getTarget(), context),
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

  protected Node visitGroupBy(final GroupBy node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
    if (node.getLocation().isPresent()) {
      return new GroupBy(
          node.getLocation().get(),
          node.isDistinct(),
          node.getGroupingElements().stream()
              .map(groupingElement -> (GroupingElement) process(groupingElement, context))
              .collect(Collectors.toList()));
    } else {
      return new GroupBy(
          node.isDistinct(),
          node.getGroupingElements().stream()
              .map(groupingElement -> (GroupingElement) process(groupingElement, context))
              .collect(Collectors.toList())
      );
    }
  }


  protected Node visitSimpleGroupBy(final SimpleGroupBy node, final Object context) {
    // use an if/else block here (instead of isPresent.map(...).orElse(...)) so only one object
    // gets instantiated (issue #1784)
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
}