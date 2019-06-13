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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.schema.ksql.TypeContextUtil.getType;
import static io.confluent.ksql.util.ParserUtil.getLocation;
import static io.confluent.ksql.util.ParserUtil.processIntegerNumber;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.SqlBaseParser.InsertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.IntervalClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.LimitClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
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
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

public class AstBuilder {

  private final MetaStore metaStore;

  AstBuilder(final MetaStore metaStore) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
  }

  public Statement build(final SingleStatementContext statement) {
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(statement);

    final Node result = new Visitor(dataSourceExtractor, metaStore).visit(statement);
    return (Statement) result;
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class Visitor extends SqlBaseBaseVisitor<Node> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private static final String DEFAULT_WINDOW_NAME = "StreamWindow";

    private final DataSourceExtractor dataSourceExtractor;
    private final MetaStore metaStore;

    private int selectItemIndex = 0;

    Visitor(
        final DataSourceExtractor dataSourceExtractor,
        final MetaStore metaStore
    ) {
      this.dataSourceExtractor = Objects.requireNonNull(dataSourceExtractor, "dataSourceExtractor");
      this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    }

    @Override
    public Node visitStatements(final SqlBaseParser.StatementsContext context) {
      final List<Statement> statementList = new ArrayList<>();
      for (final SqlBaseParser.SingleStatementContext stmtContext : context.singleStatement()) {
        final Statement statement = (Statement) visitSingleStatement(stmtContext);
        statementList.add(statement);
      }
      return new Statements(getLocation(context), statementList);
    }

    @Override
    public Node visitSingleStatement(final SqlBaseParser.SingleStatementContext context) {
      return visit(context.statement());
    }

    @Override
    public Node visitQuerystatement(final SqlBaseParser.QuerystatementContext ctx) {
      return visitChildren(ctx);
    }

    @Override
    public Node visitSingleExpression(final SqlBaseParser.SingleExpressionContext context) {
      return visit(context.expression());
    }

    private Map<String, Literal> processTableProperties(
        final TablePropertiesContext tablePropertiesContext
    ) {
      final ImmutableMap.Builder<String, Literal> properties = ImmutableMap.builder();
      if (tablePropertiesContext != null) {
        for (final TablePropertyContext prop : tablePropertiesContext.tableProperty()) {
          properties.put(
              ParserUtil.getIdentifierText(prop.identifier()),
              (Literal) visit(prop.literal())
          );
        }
      }
      return properties.build();
    }

    @Override
    public Node visitCreateTable(final SqlBaseParser.CreateTableContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);
      return new CreateTable(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          elements,
          context.EXISTS() != null,
          processTableProperties(context.tableProperties())
      );
    }

    @Override
    public Node visitCreateStream(final SqlBaseParser.CreateStreamContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);
      return new CreateStream(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          elements,
          context.EXISTS() != null,
          processTableProperties(context.tableProperties())
      );
    }

    @Override
    public Node visitCreateStreamAs(final SqlBaseParser.CreateStreamAsContext context) {

      return new CreateStreamAsSelect(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          visitQuery(context.query()),
          context.EXISTS() != null,
          processTableProperties(context.tableProperties()),
          getPartitionBy(context.identifier())
      );
    }

    @Override
    public Node visitCreateTableAs(final SqlBaseParser.CreateTableAsContext context) {
      return new CreateTableAsSelect(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          visitQuery(context.query()),
          context.EXISTS() != null,
          processTableProperties(context.tableProperties())
      );
    }

    @Override
    public Node visitInsertInto(final SqlBaseParser.InsertIntoContext context) {

      final QualifiedName targetName = ParserUtil.getQualifiedName(context.qualifiedName());
      final Optional<NodeLocation> targetLocation = getLocation(context.qualifiedName());

      final DataSource<?> target =
          getSource(targetName.getSuffix(), targetLocation);

      if (target.getDataSourceType() != DataSourceType.KSTREAM) {
        throw new KsqlException(
            "INSERT INTO can only be used to insert into a stream. "
                + targetName + " is a table.");
      }

      return new InsertInto(
          getLocation(context),
          targetName,
          visitQuery(context.query()),
          getPartitionBy(context.identifier()));
    }

    @Override
    public Node visitInsertValues(final InsertValuesContext context) {
      final QualifiedName targetName = ParserUtil.getQualifiedName(context.qualifiedName());
      final Optional<NodeLocation> targetLocation = getLocation(context.qualifiedName());

      final List<String> columns;
      if (context.columns() != null) {
        columns = context.columns().identifier()
            .stream()
            .map(ParserUtil::getIdentifierText)
            .collect(Collectors.toList());
      } else {
        columns = ImmutableList.of();
      }

      return new InsertValues(
          targetLocation,
          targetName,
          columns,
          visit(context.values().literal(), Expression.class));
    }

    @Override
    public Node visitDropTable(final SqlBaseParser.DropTableContext context) {
      return new DropTable(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          context.EXISTS() != null,
          context.DELETE() != null
      );
    }

    @Override
    public Node visitDropStream(final SqlBaseParser.DropStreamContext context) {
      return new DropStream(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          context.EXISTS() != null,
          context.DELETE() != null
      );
    }

    @Override
    public Query visitQuery(final SqlBaseParser.QueryContext context) {
      final Relation from = (Relation) visit(context.from);

      final Select select = new Select(
          getLocation(context.SELECT()),
          visit(context.selectItem(), SelectItem.class)
      );

      final OptionalInt limit = getLimit(context.limitClause());

      return new Query(
          getLocation(context),
          select,
          from,
          visitIfPresent(context.windowExpression(), WindowExpression.class),
          visitIfPresent(context.where, Expression.class),
          visitIfPresent(context.groupBy(), GroupBy.class),
          visitIfPresent(context.having, Expression.class),
          limit
      );
    }

    @Override
    public Node visitWindowExpression(final SqlBaseParser.WindowExpressionContext ctx) {
      String windowName = DEFAULT_WINDOW_NAME;
      if (ctx.IDENTIFIER() != null) {
        windowName = ctx.IDENTIFIER().getText();
      }
      windowName = windowName.toUpperCase();
      if (ctx.tumblingWindowExpression() != null) {
        final TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression)
            visitTumblingWindowExpression(ctx.tumblingWindowExpression());
        return new WindowExpression(
            getLocation(ctx.tumblingWindowExpression()),
            windowName,
            tumblingWindowExpression);
      } else if (ctx.hoppingWindowExpression() != null) {
        final HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression)
            visitHoppingWindowExpression(ctx.hoppingWindowExpression());
        return new WindowExpression(
            getLocation(ctx.hoppingWindowExpression()),
            windowName,
            hoppingWindowExpression);
      } else if (ctx.sessionWindowExpression() != null) {
        final SessionWindowExpression sessionWindowExpression = (SessionWindowExpression)
            visitSessionWindowExpression(ctx.sessionWindowExpression());
        return new WindowExpression(
            getLocation(ctx.sessionWindowExpression()),
            windowName,
            sessionWindowExpression);
      }
      throw new KsqlException("Window description is not correct.");
    }

    @Override
    public Node visitHoppingWindowExpression(
        final SqlBaseParser.HoppingWindowExpressionContext ctx) {

      final List<SqlBaseParser.NumberContext> numberList = ctx.number();
      final List<SqlBaseParser.WindowUnitContext> windowUnits = ctx.windowUnit();
      final String sizeStr = numberList.get(0).getText();
      final String advanceByStr = numberList.get(1).getText();

      final String sizeUnit = windowUnits.get(0).getText();
      final String advanceByUnit = windowUnits.get(1).getText();
      return new HoppingWindowExpression(
          getLocation(ctx),
          Long.parseLong(sizeStr),
          WindowExpression.getWindowUnit(sizeUnit.toUpperCase()),
          Long.parseLong(advanceByStr),
          WindowExpression.getWindowUnit(advanceByUnit.toUpperCase())
      );
    }

    @Override
    public Node visitTumblingWindowExpression(
        final SqlBaseParser.TumblingWindowExpressionContext ctx
    ) {
      final String sizeStr = ctx.number().getText();
      final String sizeUnit = ctx.windowUnit().getText();
      return new TumblingWindowExpression(
          getLocation(ctx),
          Long.parseLong(sizeStr),
          WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
      );
    }

    @Override
    public Node visitSessionWindowExpression(
        final SqlBaseParser.SessionWindowExpressionContext ctx
    ) {
      final String sizeStr = ctx.number().getText();
      final String sizeUnit = ctx.windowUnit().getText();
      return new SessionWindowExpression(
          getLocation(ctx),
          Long.parseLong(sizeStr),
          WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
      );
    }

    private static Node visitWithinExpression(final SqlBaseParser.WithinExpressionContext ctx) {
      final Pair<Long, TimeUnit> beforeSize;
      final Pair<Long, TimeUnit> afterSize;

      if (ctx instanceof SqlBaseParser.SingleJoinWindowContext) {

        final SqlBaseParser.SingleJoinWindowContext singleWithin
            = (SqlBaseParser.SingleJoinWindowContext) ctx;

        beforeSize = getSizeAndUnitFromJoinWindowSize(singleWithin.joinWindowSize());
        afterSize = beforeSize;
      } else if (ctx instanceof SqlBaseParser.JoinWindowWithBeforeAndAfterContext) {
        final SqlBaseParser.JoinWindowWithBeforeAndAfterContext beforeAndAfterJoinWindow
            = (SqlBaseParser.JoinWindowWithBeforeAndAfterContext) ctx;

        beforeSize = getSizeAndUnitFromJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(0));
        afterSize = getSizeAndUnitFromJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(1));

      } else {
        throw new RuntimeException("Expecting either a single join window, ie \"WITHIN 10 "
            + "seconds\", or a join window with before and after specified, "
            + "ie. \"WITHIN (10 seconds, 20 seconds)");
      }
      return new WithinExpression(
          getLocation(ctx),
          beforeSize.left,
          afterSize.left,
          beforeSize.right,
          afterSize.right
      );
    }

    private static Pair<Long, TimeUnit> getSizeAndUnitFromJoinWindowSize(
        final SqlBaseParser.JoinWindowSizeContext joinWindowSize
    ) {
      return new Pair<>(Long.parseLong(joinWindowSize.number().getText()),
          WindowExpression.getWindowUnit(
              joinWindowSize.windowUnit().getText().toUpperCase()));
    }

    @Override
    public Node visitGroupBy(final SqlBaseParser.GroupByContext context) {
      return new GroupBy(
          getLocation(context),
          visit(context.groupingElement(), GroupingElement.class)
      );
    }

    @Override
    public Node visitSingleGroupingSet(final SqlBaseParser.SingleGroupingSetContext context) {
      return new SimpleGroupBy(
          getLocation(context),
          visit(context.groupingExpressions().expression(), Expression.class)
      );
    }

    @Override
    public Node visitSelectAll(final SqlBaseParser.SelectAllContext context) {
      final Optional<QualifiedName> prefix = Optional.ofNullable(context.qualifiedName())
          .map(ParserUtil::getQualifiedName);

      prefix.ifPresent(name -> throwOnUnknownNameOrAlias(name.toString()));

      return new AllColumns(getLocation(context), prefix);
    }

    @Override
    public Node visitSelectSingle(final SqlBaseParser.SelectSingleContext context) {
      final Expression selectItem = (Expression) visit(context.expression());

      final String alias;
      if (context.identifier() != null) {
        alias = ParserUtil.getIdentifierText(context.identifier());
      } else {
        if (selectItem instanceof QualifiedNameReference) {
          alias = ((QualifiedNameReference) selectItem).getName().getSuffix();
        } else if (selectItem instanceof DereferenceExpression) {
          final DereferenceExpression dereferenceExp = (DereferenceExpression) selectItem;
          final String dereferenceExpressionString = dereferenceExp.toString();
          if (dataSourceExtractor.isJoin()
              && dataSourceExtractor
              .getCommonFieldNames()
              .contains(dereferenceExp.getFieldName())
          ) {
            alias = replaceDotFieldRef(dereferenceExpressionString);
          } else if (dereferenceExpressionString.contains(KsqlConstants.STRUCT_FIELD_REF)) {
            alias = replaceDotFieldRef(
                    dereferenceExpressionString.substring(
                        dereferenceExpressionString.indexOf(KsqlConstants.DOT) + 1));
          } else {
            alias = dereferenceExp.getFieldName();
          }
        } else {
          alias = "KSQL_COL_" + selectItemIndex;
        }
      }
      selectItemIndex++;
      return new SingleColumn(getLocation(context), selectItem, alias);
    }

    private static String replaceDotFieldRef(final String input) {
      return input
          .replace(KsqlConstants.DOT, "_")
          .replace(KsqlConstants.STRUCT_FIELD_REF, "__");
    }

    @Override
    public Node visitQualifiedName(final SqlBaseParser.QualifiedNameContext context) {
      return visitChildren(context);
    }

    @Override
    public Node visitRunScript(final SqlBaseParser.RunScriptContext context) {
      return new RunScript(getLocation(context));
    }

    @Override
    public Node visitListTopics(final SqlBaseParser.ListTopicsContext context) {
      return new ListTopics(getLocation(context));
    }

    @Override
    public Node visitListStreams(final SqlBaseParser.ListStreamsContext context) {
      return new ListStreams(
          getLocation(context), context.EXTENDED() != null);
    }

    @Override
    public Node visitListTables(final SqlBaseParser.ListTablesContext context) {
      return new ListTables(
          getLocation(context), context.EXTENDED() != null);
    }

    @Override
    public Node visitListQueries(final SqlBaseParser.ListQueriesContext context) {
      return new ListQueries(
          getLocation(context), context.EXTENDED() != null);
    }

    @Override
    public Node visitListFunctions(final SqlBaseParser.ListFunctionsContext ctx) {
      return new ListFunctions(getLocation(ctx));
    }

    @Override
    public Node visitTerminateQuery(final SqlBaseParser.TerminateQueryContext context) {
      return new TerminateQuery(getLocation(context), context.qualifiedName().getText());
    }

    @Override
    public Node visitShowColumns(final SqlBaseParser.ShowColumnsContext context) {
      return new ShowColumns(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          context.TOPIC() != null,
          context.EXTENDED() != null
      );
    }

    @Override
    public Node visitListProperties(final SqlBaseParser.ListPropertiesContext context) {
      return new ListProperties(getLocation(context));
    }

    @Override
    public Node visitSetProperty(final SqlBaseParser.SetPropertyContext context) {
      final String propertyName = ParserUtil.unquote(context.STRING(0).getText(), "'");
      final String propertyValue = ParserUtil.unquote(context.STRING(1).getText(), "'");
      return new SetProperty(getLocation(context), propertyName, propertyValue);
    }

    @Override
    public Node visitUnsetProperty(final SqlBaseParser.UnsetPropertyContext context) {
      final String propertyName = ParserUtil.unquote(context.STRING().getText(), "'");
      return new UnsetProperty(getLocation(context), propertyName);
    }

    @Override
    public Node visitPrintTopic(final SqlBaseParser.PrintTopicContext context) {
      final boolean fromBeginning = context.printClause().FROM() != null;

      final QualifiedName topicName;
      if (context.STRING() != null) {
        topicName = QualifiedName.of(ParserUtil.unquote(context.STRING().getText(), "'"));
      } else {
        topicName = ParserUtil.getQualifiedName(context.qualifiedName());
      }

      final IntervalClauseContext intervalContext = context.printClause().intervalClause();
      final OptionalInt interval = intervalContext == null
          ? OptionalInt.empty()
          : OptionalInt.of(processIntegerNumber(intervalContext.number(), "INTERVAL"));

      final OptionalInt limit = getLimit(context.printClause().limitClause());

      return new PrintTopic(
          getLocation(context),
          topicName,
          fromBeginning,
          interval,
          limit
      );
    }

    @Override
    public Node visitNumericLiteral(final SqlBaseParser.NumericLiteralContext ctx) {
      return visitChildren(ctx);
    }

    @Override
    public Node visitLogicalNot(final SqlBaseParser.LogicalNotContext context) {
      return new NotExpression(getLocation(context),
          (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(final SqlBaseParser.LogicalBinaryContext context) {
      return new LogicalBinaryExpression(
          getLocation(context.operator),
          getLogicalBinaryOperator(context.operator),
          (Expression) visit(context.left),
          (Expression) visit(context.right)
      );
    }

    @Override
    public Node visitJoinRelation(final SqlBaseParser.JoinRelationContext context) {
      if (context.joinCriteria().ON() == null) {
        throw new KsqlException("Invalid join criteria specified. KSQL only supports joining on "
            + "column values. For example `... left JOIN right on left.col = "
            + "right.col ...`. Tables can only be joined on the Table's key "
            + "column. KSQL will repartition streams if the column in the join "
            + "criteria is not the key column.");
      }

      final JoinCriteria criteria =
          new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
      final Join.Type joinType;
      final SqlBaseParser.JoinTypeContext joinTypeContext = context.joinType();
      if (joinTypeContext instanceof SqlBaseParser.LeftJoinContext) {
        joinType = Join.Type.LEFT;
      } else if (joinTypeContext instanceof SqlBaseParser.OuterJoinContext) {
        joinType = Join.Type.OUTER;
      } else {
        joinType = Join.Type.INNER;
      }

      WithinExpression withinExpression = null;
      if (context.joinWindow() != null) {
        withinExpression = (WithinExpression) visitWithinExpression(
            context.joinWindow().withinExpression());
      }
      final AliasedRelation left = (AliasedRelation) visit(context.left);
      final AliasedRelation right = (AliasedRelation) visit(context.right);
      return new Join(getLocation(context), joinType, left, right, criteria,
          Optional.ofNullable(withinExpression));
    }

    @Override
    public Node visitAliasedRelation(final SqlBaseParser.AliasedRelationContext context) {
      final Relation child = (Relation) visit(context.relationPrimary());

      final String alias;
      switch (context.children.size()) {
        case 1:
          final Table table = (Table) visit(context.relationPrimary());
          alias = table.getName().getSuffix();
          break;

        case 2:
          alias = context.children.get(1).getText();
          break;

        case 3:
          alias = context.children.get(2).getText();
          break;

        default:
          throw new IllegalArgumentException(
              "AliasedRelationContext must have between 1 and 3 children, but has:"
                  + context.children.size()
          );
      }

      return new AliasedRelation(getLocation(context), child, alias);
    }

    @Override
    public Node visitTableName(final SqlBaseParser.TableNameContext context) {
      return new Table(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName())
      );
    }

    @Override
    public Node visitPredicated(final SqlBaseParser.PredicatedContext context) {
      if (context.predicate() != null) {
        return visit(context.predicate());
      }

      return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(final SqlBaseParser.ComparisonContext context) {
      return new ComparisonExpression(
          getLocation(context.comparisonOperator()),
          getComparisonOperator(
              ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
          (Expression) visit(context.value),
          (Expression) visit(context.right)
      );
    }

    @Override
    public Node visitDistinctFrom(final SqlBaseParser.DistinctFromContext context) {
      Expression expression = new ComparisonExpression(
          getLocation(context),
          ComparisonExpression.Type.IS_DISTINCT_FROM,
          (Expression) visit(context.value),
          (Expression) visit(context.right)
      );
      if (context.NOT() != null) {
        expression = new NotExpression(getLocation(context), expression);
      }
      return expression;
    }

    @Override
    public Node visitBetween(final SqlBaseParser.BetweenContext context) {
      Expression expression = new BetweenPredicate(
          getLocation(context),
          (Expression) visit(context.value),
          (Expression) visit(context.lower),
          (Expression) visit(context.upper)
      );

      if (context.NOT() != null) {
        expression = new NotExpression(getLocation(context), expression);
      }

      return expression;
    }

    @Override
    public Node visitNullPredicate(final SqlBaseParser.NullPredicateContext context) {
      final Expression child = (Expression) visit(context.value);

      if (context.NOT() == null) {
        return new IsNullPredicate(getLocation(context), child);
      }

      return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLike(final SqlBaseParser.LikeContext context) {
      final Expression result = new LikePredicate(
          getLocation(context),
          (Expression) visit(context.value),
          (Expression) visit(context.pattern)
      );

      if (context.NOT() == null) {
        return result;
      }

      return new NotExpression(getLocation(context), result);
    }

    @Override
    public Node visitInList(final SqlBaseParser.InListContext context) {
      Expression result = new InPredicate(
          getLocation(context),
          (Expression) visit(context.value),
          new InListExpression(getLocation(context), visit(context.expression(), Expression.class))
      );

      if (context.NOT() != null) {
        result = new NotExpression(getLocation(context), result);
      }

      return result;
    }

    @Override
    public Node visitArithmeticUnary(final SqlBaseParser.ArithmeticUnaryContext context) {
      final Expression child = (Expression) visit(context.valueExpression());

      switch (context.operator.getType()) {
        case SqlBaseLexer.MINUS:
          return ArithmeticUnaryExpression.negative(getLocation(context), child);
        case SqlBaseLexer.PLUS:
          return ArithmeticUnaryExpression.positive(getLocation(context), child);
        default:
          throw new UnsupportedOperationException(
              "Unsupported sign: " + context.operator.getText());
      }
    }

    @Override
    public Node visitArithmeticBinary(final SqlBaseParser.ArithmeticBinaryContext context) {
      return new ArithmeticBinaryExpression(
          getLocation(context.operator),
          getArithmeticBinaryOperator(context.operator),
          (Expression) visit(context.left),
          (Expression) visit(context.right)
      );
    }

    @Override
    public Node visitConcatenation(final SqlBaseParser.ConcatenationContext context) {
      return new FunctionCall(
          getLocation(context.CONCAT()),
          QualifiedName.of("concat"), ImmutableList.of(
          (Expression) visit(context.left),
          (Expression) visit(context.right)
      )
      );
    }

    @Override
    public Node visitTimeZoneString(final SqlBaseParser.TimeZoneStringContext context) {
      return new StringLiteral(
          getLocation(context),
          ParserUtil.unquote(context.STRING().getText(), "'")
      );
    }

    @Override
    public Node visitParenthesizedExpression(
        final SqlBaseParser.ParenthesizedExpressionContext context) {
      return visit(context.expression());
    }

    @Override
    public Node visitCast(final SqlBaseParser.CastContext context) {
      return new Cast(
          getLocation(context),
          (Expression) visit(context.expression()),
          getType(context.type())
      );
    }

    @Override
    public Node visitSubscript(final SqlBaseParser.SubscriptContext context) {
      return new SubscriptExpression(
          getLocation(context),
          (Expression) visit(context.value),
          (Expression) visit(context.index)
      );
    }

    @Override
    public Node visitDereference(final SqlBaseParser.DereferenceContext context) {
      final String fieldName = ParserUtil.getIdentifierText(context.identifier());
      final Expression baseExpression = (Expression) visit(context.base);
      return new DereferenceExpression(getLocation(context), baseExpression, fieldName);
    }

    @Override
    public Node visitColumnReference(final SqlBaseParser.ColumnReferenceContext context) {

      final String columnName;
      final String prefixName;
      final Optional<NodeLocation> columnLocation;
      if (context.identifier(1) == null) {
        prefixName = null;
        columnName = ParserUtil.getIdentifierText(context.identifier(0));
        columnLocation = getLocation(context.identifier(0));
      } else {
        prefixName = ParserUtil.getIdentifierText(context.identifier(0));
        columnName = ParserUtil.getIdentifierText(context.identifier(1));
        columnLocation = getLocation(context.identifier(1));
      }

      if (prefixName != null) {
        throwOnUnknownNameOrAlias(prefixName);

        final Expression baseExpression =
            new QualifiedNameReference(
                getLocation(context),
                QualifiedName.of(prefixName)
            );
        return new DereferenceExpression(
            getLocation(context),
            baseExpression,
            columnName
        );
      }

      if (dataSourceExtractor.isJoin()) {
        if (dataSourceExtractor.getCommonFieldNames().contains(columnName)) {
          throw new KsqlException("Field " + columnName + " is ambiguous.");
        }

        if (dataSourceExtractor.getLeftFieldNames().contains(columnName)) {
          final Expression baseExpression =
              new QualifiedNameReference(
                  getLocation(context),
                  QualifiedName.of(dataSourceExtractor.getLeftAlias())
              );
          return new DereferenceExpression(getLocation(context), baseExpression, columnName);
        }

        if (dataSourceExtractor.getRightFieldNames().contains(columnName)) {
          final Expression baseExpression =
              new QualifiedNameReference(
                  getLocation(context),
                  QualifiedName.of(dataSourceExtractor.getRightAlias())
              );
          return new DereferenceExpression(getLocation(context), baseExpression, columnName);
        }

        throw new InvalidColumnReferenceException(
            columnLocation,
            "Field " + columnName + " is ambiguous."
        );
      }

      final Expression baseExpression =
          new QualifiedNameReference(
              getLocation(context),
              QualifiedName.of(dataSourceExtractor.getFromAlias())
          );
      return new DereferenceExpression(getLocation(context), baseExpression, columnName);
    }

    private boolean isValidNameOrAlias(final String name) {
      if (dataSourceExtractor.isJoin()) {
        final boolean sameAsLeft = name.equalsIgnoreCase(dataSourceExtractor.getLeftAlias())
            || name.equalsIgnoreCase(dataSourceExtractor.getLeftName());
        final boolean sameAsRight = name.equalsIgnoreCase(dataSourceExtractor.getRightAlias())
            || name.equalsIgnoreCase(dataSourceExtractor.getRightName());
        return sameAsLeft || sameAsRight;
      }
      return ((name.equalsIgnoreCase(dataSourceExtractor.getFromAlias())
          || name.equalsIgnoreCase(dataSourceExtractor.getFromName())));
    }

    @Override
    public Node visitSimpleCase(final SqlBaseParser.SimpleCaseContext context) {
      return new SimpleCaseExpression(
          getLocation(context),
          (Expression) visit(context.valueExpression()),
          visit(context.whenClause(), WhenClause.class),
          visitIfPresent(context.elseExpression, Expression.class)
      );
    }

    @Override
    public Node visitSearchedCase(final SqlBaseParser.SearchedCaseContext context) {
      return new SearchedCaseExpression(
          getLocation(context),
          visit(context.whenClause(), WhenClause.class),
          visitIfPresent(context.elseExpression, Expression.class)
      );
    }

    @Override
    public Node visitWhenClause(final SqlBaseParser.WhenClauseContext context) {
      return new WhenClause(
          getLocation(context),
          (Expression) visit(context.condition),
          (Expression) visit(context.result)
      );
    }

    @Override
    public Node visitFunctionCall(final SqlBaseParser.FunctionCallContext context) {
      return new FunctionCall(
          getLocation(context),
          ParserUtil.getQualifiedName(context.qualifiedName()),
          visit(context.expression(), Expression.class)
      );
    }

    @Override
    public Node visitTableElement(final SqlBaseParser.TableElementContext context) {
      return new TableElement(
          getLocation(context),
          ParserUtil.getIdentifierText(context.identifier()),
          getType(context.type())
      );
    }

    @Override
    public Node visitNullLiteral(final SqlBaseParser.NullLiteralContext context) {
      return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitStringLiteral(final SqlBaseParser.StringLiteralContext context) {
      return new StringLiteral(
          getLocation(context),
          ParserUtil.unquote(context.STRING().getText(), "'")
      );
    }

    @Override
    public Node visitTypeConstructor(final SqlBaseParser.TypeConstructorContext context) {
      final String type = ParserUtil.getIdentifierText(context.identifier());
      final String value = ParserUtil.unquote(context.STRING().getText(), "'");
      final Optional<NodeLocation> location = getLocation(context);

      if (type.equals("TIME")) {
        return new TimeLiteral(location, value);
      }
      if (type.equals("TIMESTAMP")) {
        return new TimestampLiteral(location, value);
      }
      if (type.equals("DECIMAL")) {
        return new DecimalLiteral(location, value);
      }

      throw new KsqlException("Unknown type: " + type + ", location:" + location);
    }

    @Override
    public Node visitIntegerLiteral(final SqlBaseParser.IntegerLiteralContext context) {
      return ParserUtil.visitIntegerLiteral(context);
    }

    @Override
    public Node visitDecimalLiteral(final SqlBaseParser.DecimalLiteralContext context) {
      return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(final SqlBaseParser.BooleanValueContext context) {
      return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitExplain(final SqlBaseParser.ExplainContext ctx) {
      final SqlBaseParser.QualifiedNameContext qualifiedName = ctx.qualifiedName();
      final Optional<String> queryId = Optional.ofNullable(qualifiedName)
          .map(RuleContext::getText);

      final Optional<Statement> statement = Optional.ofNullable(ctx.statement())
          .map(s -> (Statement) visit(s));

      return new Explain(getLocation(ctx), queryId, statement);
    }

    @Override
    public Node visitDescribeFunction(final SqlBaseParser.DescribeFunctionContext ctx) {
      return new DescribeFunction(getLocation(ctx), ctx.qualifiedName().getText());
    }

    @Override
    protected Node defaultResult() {
      return null;
    }

    @Override
    protected Node aggregateResult(final Node aggregate, final Node nextResult) {
      if (nextResult == null) {
        throw new UnsupportedOperationException("not yet implemented");
      }

      if (aggregate == null) {
        return nextResult;
      }

      throw new UnsupportedOperationException("not yet implemented");
    }

    private void throwOnUnknownNameOrAlias(final String name) {
      if (!isValidNameOrAlias(name)) {
        throw new KsqlException("'" + name + "' is not a valid stream/table name or alias.");
      }
    }

    private <T> Optional<T> visitIfPresent(final ParserRuleContext context, final Class<T> clazz) {
      return Optional.ofNullable(context)
          .map(this::visit)
          .map(clazz::cast);
    }

    private <T> List<T> visit(final List<? extends ParserRuleContext> contexts,
        final Class<T> clazz) {
      return contexts.stream()
          .map(this::visit)
          .map(clazz::cast)
          .collect(toList());
    }

    private static Optional<Expression> getPartitionBy(
        final SqlBaseParser.IdentifierContext identifier
    ) {
      if (identifier == null) {
        return Optional.empty();
      }

      final Optional<NodeLocation> location = getLocation(identifier);
      final QualifiedName name = QualifiedName.of(ParserUtil.getIdentifierText(identifier));
      return Optional.of(new QualifiedNameReference(location, name));
    }

    private static Operator getArithmeticBinaryOperator(
        final Token operator) {
      switch (operator.getType()) {
        case SqlBaseLexer.PLUS:
          return Operator.ADD;
        case SqlBaseLexer.MINUS:
          return Operator.SUBTRACT;
        case SqlBaseLexer.ASTERISK:
          return Operator.MULTIPLY;
        case SqlBaseLexer.SLASH:
          return Operator.DIVIDE;
        case SqlBaseLexer.PERCENT:
          return Operator.MODULUS;
        default:
          throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
      }
    }

    private static ComparisonExpression.Type getComparisonOperator(final Token symbol) {
      switch (symbol.getType()) {
        case SqlBaseLexer.EQ:
          return ComparisonExpression.Type.EQUAL;
        case SqlBaseLexer.NEQ:
          return ComparisonExpression.Type.NOT_EQUAL;
        case SqlBaseLexer.LT:
          return ComparisonExpression.Type.LESS_THAN;
        case SqlBaseLexer.LTE:
          return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
        case SqlBaseLexer.GT:
          return ComparisonExpression.Type.GREATER_THAN;
        case SqlBaseLexer.GTE:
          return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
        default:
          throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
      }
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(final Token token) {
      switch (token.getType()) {
        case SqlBaseLexer.AND:
          return LogicalBinaryExpression.Type.AND;
        case SqlBaseLexer.OR:
          return LogicalBinaryExpression.Type.OR;
        default:
          throw new IllegalArgumentException("Unsupported operator: " + token.getText());
      }
    }

    private OptionalInt getLimit(final LimitClauseContext limitContext) {
      return limitContext == null
          ? OptionalInt.empty()
          : OptionalInt.of(processIntegerNumber(limitContext.number(), "LIMIT"));
    }

    private DataSource<?> getSource(
        final String name,
        final Optional<NodeLocation> location
    ) {
      final DataSource<?> source = metaStore.getSource(name);
      if (source == null) {
        throw new InvalidColumnReferenceException(location, name + " does not exist.");
      }

      return source;
    }

    private static final class InvalidColumnReferenceException extends KsqlException {

      private InvalidColumnReferenceException(
          final Optional<NodeLocation> location,
          final String message
      ) {
        super(location.map(loc -> loc + ": ").orElse("") + message);
      }
    }
  }
}
