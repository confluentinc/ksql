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

import static io.confluent.ksql.util.ParserUtil.getIdentifierText;
import static io.confluent.ksql.util.ParserUtil.getLocation;
import static io.confluent.ksql.util.ParserUtil.processIntegerNumber;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.SqlBaseParser.CreateConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DescribeConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DropConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DropTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.IdentifierContext;
import io.confluent.ksql.parser.SqlBaseParser.InsertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.IntervalClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.LimitClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.ListConnectorsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListTypesContext;
import io.confluent.ksql.parser.SqlBaseParser.RegisterTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.SourceNameContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.ListConnectors;
import io.confluent.ksql.parser.tree.ListConnectors.Scope;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.ListTypes;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.ResultMaterialization;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

public class AstBuilder {

  private final TypeRegistry typeRegistry;

  public AstBuilder(final TypeRegistry typeRegistry) {
    this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry");
  }

  public Statement buildStatement(final ParserRuleContext parseTree) {
    return build(Optional.of(getSources(parseTree)), parseTree);
  }

  public Expression buildExpression(final ParserRuleContext parseTree) {
    return build(Optional.empty(), parseTree);
  }

  public WindowExpression buildWindowExpression(final ParserRuleContext parseTree) {
    return build(Optional.empty(), parseTree);
  }

  @SuppressWarnings("unchecked")
  private <T extends Node> T build(
      final Optional<Set<SourceName>> sources,
      final ParserRuleContext parseTree) {
    return (T) new Visitor(sources, typeRegistry).visit(parseTree);
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class Visitor extends SqlBaseBaseVisitor<Node> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private static final String DEFAULT_WINDOW_NAME = "StreamWindow";

    private final Optional<Set<SourceName>> sources;
    private final SqlTypeParser typeParser;

    private boolean buildingPersistentQuery = false;

    Visitor(final Optional<Set<SourceName>> sources, final TypeRegistry typeRegistry) {
      this.sources = Objects.requireNonNull(sources, "sources").map(ImmutableSet::copyOf);
      this.typeParser = SqlTypeParser.create(typeRegistry);
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
    public Node visitSingleExpression(final SqlBaseParser.SingleExpressionContext context) {
      return visit(context.expression());
    }

    private Map<String, Literal> processTableProperties(
        final TablePropertiesContext tablePropertiesContext
    ) {
      final ImmutableMap.Builder<String, Literal> properties = ImmutableMap.builder();
      if (tablePropertiesContext != null) {
        for (final TablePropertyContext prop : tablePropertiesContext.tableProperty()) {
          if (prop.identifier() != null) {
            properties.put(
                ParserUtil.getIdentifierText(prop.identifier()),
                (Literal) visit(prop.literal())
            );
          } else {
            properties.put(
                ParserUtil.unquote(prop.STRING().getText(), "'"),
                (Literal) visit(prop.literal()));
          }
        }
      }
      return properties.build();
    }

    @Override
    public Node visitCreateTable(final SqlBaseParser.CreateTableContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);

      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      return new CreateTable(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          TableElements.of(elements),
          context.EXISTS() != null,
          CreateSourceProperties.from(properties)
      );
    }

    @Override
    public Node visitCreateStream(final SqlBaseParser.CreateStreamContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);

      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      return new CreateStream(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          TableElements.of(elements),
          context.EXISTS() != null,
          CreateSourceProperties.from(properties)
      );
    }

    @Override
    public Node visitCreateStreamAs(final SqlBaseParser.CreateStreamAsContext context) {
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));

      return new CreateStreamAsSelect(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          query,
          context.EXISTS() != null,
          CreateSourceAsProperties.from(properties)
      );
    }

    @Override
    public Node visitCreateTableAs(final SqlBaseParser.CreateTableAsContext context) {
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));

      return new CreateTableAsSelect(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          query,
          context.EXISTS() != null,
          CreateSourceAsProperties.from(properties)
      );
    }

    @Override
    public Node visitCreateConnector(final CreateConnectorContext context) {
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());
      final String name = ParserUtil.getIdentifierText(context.identifier());
      final CreateConnector.Type type = context.SOURCE() != null ? Type.SOURCE : Type.SINK;

      return new CreateConnector(
          getLocation(context),
          name,
          properties,
          type
      );
    }

    @Override
    public Node visitInsertInto(final SqlBaseParser.InsertIntoContext context) {
      final SourceName targetName = ParserUtil.getSourceName(context.sourceName());
      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));
      return new InsertInto(
          getLocation(context),
          targetName,
          query);
    }

    @Override
    public Node visitInsertValues(final InsertValuesContext context) {
      final SourceName targetName = ParserUtil.getSourceName(context.sourceName());
      final Optional<NodeLocation> targetLocation = getLocation(context.sourceName());

      final List<ColumnName> columns;
      if (context.columns() != null) {
        columns = context.columns().identifier()
            .stream()
            .map(ParserUtil::getIdentifierText)
            .map(ColumnName::of)
            .collect(Collectors.toList());
      } else {
        columns = ImmutableList.of();
      }

      return new InsertValues(
          targetLocation,
          targetName,
          columns,
          visit(context.values().valueExpression(), Expression.class));
    }

    @Override
    public Node visitDropTable(final SqlBaseParser.DropTableContext context) {
      return new DropTable(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          context.EXISTS() != null,
          context.DELETE() != null
      );
    }

    @Override
    public Node visitDropStream(final SqlBaseParser.DropStreamContext context) {
      return new DropStream(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          context.EXISTS() != null,
          context.DELETE() != null
      );
    }

    @Override
    public Node visitDropConnector(final DropConnectorContext context) {
      return new DropConnector(
          getLocation(context),
          ParserUtil.getIdentifierText(context.identifier())
      );
    }

    @Override
    public Query visitQuery(final SqlBaseParser.QueryContext context) {
      final Relation from = (Relation) visit(context.from);

      final Select select = new Select(
          getLocation(context.SELECT()),
          visit(context.selectItem(), SelectItem.class)
      );

      final boolean pullQuery = context.EMIT() == null && !buildingPersistentQuery;

      final ResultMaterialization resultMaterialization = Optional
          .ofNullable(context.resultMaterialization())
          .map(rm -> rm.CHANGES() == null
              ? ResultMaterialization.FINAL
              : ResultMaterialization.CHANGES
          )
          .orElse(buildingPersistentQuery
              ? ResultMaterialization.CHANGES
              : ResultMaterialization.FINAL
          );

      final OptionalInt limit = getLimit(context.limitClause());

      return new Query(
          getLocation(context),
          select,
          from,
          visitIfPresent(context.windowExpression(), WindowExpression.class),
          visitIfPresent(context.where, Expression.class),
          visitIfPresent(context.groupBy(), GroupBy.class),
          visitIfPresent(context.partitionBy, Expression.class),
          visitIfPresent(context.having, Expression.class),
          resultMaterialization,
          pullQuery,
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
      final Optional<SourceName> prefix = Optional.ofNullable(context.identifier())
          .map(ParserUtil::getIdentifierText)
          .map(SourceName::of);

      prefix.ifPresent(this::throwOnUnknownNameOrAlias);
      return new AllColumns(getLocation(context), prefix);
    }

    @Override
    public Node visitSelectSingle(final SqlBaseParser.SelectSingleContext context) {
      final Expression selectItem = (Expression) visit(context.expression());

      if (context.identifier() != null) {
        return new SingleColumn(
            getLocation(context),
            selectItem,
            Optional.of(ColumnName.of(ParserUtil.getIdentifierText(context.identifier())))
        );
      } else {
        return new SingleColumn(getLocation(context), selectItem, Optional.empty());
      }
    }

    @Override
    public Node visitRunScript(final SqlBaseParser.RunScriptContext context) {
      return new RunScript(getLocation(context));
    }

    @Override
    public Node visitListTopics(final SqlBaseParser.ListTopicsContext context) {
      return new ListTopics(getLocation(context), context.EXTENDED() != null);
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
    public Node visitListConnectors(final ListConnectorsContext ctx) {
      final ListConnectors.Scope scope;
      if (ctx.SOURCE() != null) {
        scope = Scope.SOURCE;
      } else if (ctx.SINK() != null) {
        scope = Scope.SINK;
      } else {
        scope = Scope.ALL;
      }

      return new ListConnectors(getLocation(ctx), scope);
    }

    @Override
    public Node visitDropType(final DropTypeContext ctx) {
      return new DropType(getLocation(ctx), getIdentifierText(ctx.identifier()));
    }

    @Override
    public Node visitListTypes(final ListTypesContext ctx) {
      return new ListTypes(getLocation(ctx));
    }

    @Override
    public Node visitTerminateQuery(final SqlBaseParser.TerminateQueryContext context) {
      final Optional<NodeLocation> location = getLocation(context);

      return context.ALL() != null
          ? TerminateQuery.all(location)
          : TerminateQuery.query(
              location,
              // use case sensitive parsing here to maintain backwards compatibility
              new QueryId(ParserUtil.getIdentifierText(true, context.identifier()))
          );
    }

    @Override
    public Node visitShowColumns(final SqlBaseParser.ShowColumnsContext context) {
      return new ShowColumns(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
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

      final String topicName;
      if (context.STRING() != null) {
        topicName = ParserUtil.unquote(context.STRING().getText(), "'");
      } else {
        topicName = ParserUtil.getIdentifierText(true, context.identifier());
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

      final SourceName alias;
      switch (context.children.size()) {
        case 1:
          final Table table = (Table) visit(context.relationPrimary());
          alias = table.getName();
          break;

        case 2:
          alias = ParserUtil.getSourceName((SourceNameContext) context.children.get(1));
          break;

        case 3:
          alias = ParserUtil.getSourceName((SourceNameContext) context.children.get(2));
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
          ParserUtil.getSourceName(context.sourceName())
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
          FunctionName.of("concat"), ImmutableList.of(
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
          typeParser.getType(context.type())
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
      final ColumnReferenceExp columnReferenceExp = ColumnReferenceParser.resolve(context);
      final ColumnRef reference = columnReferenceExp.getReference();
      reference.source().ifPresent(this::throwOnUnknownNameOrAlias);
      return columnReferenceExp;
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
          FunctionName.of(ParserUtil.getIdentifierText(context.identifier())),
          visit(context.expression(), Expression.class)
      );
    }

    @Override
    public Node visitTableElement(final SqlBaseParser.TableElementContext context) {
      return new TableElement(
          getLocation(context),
          context.KEY() == null ? Namespace.VALUE : Namespace.KEY,
          ColumnName.of(ParserUtil.getIdentifierText(context.identifier())),
          typeParser.getType(context.type())
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
      return ParserUtil.parseDecimalLiteral(context);
    }

    @Override
    public Node visitBooleanValue(final SqlBaseParser.BooleanValueContext context) {
      return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitExplain(final SqlBaseParser.ExplainContext ctx) {
      final IdentifierContext queryIdentifier = ctx.identifier();
      final Optional<String> queryId = Optional.ofNullable(queryIdentifier)
          .map(RuleContext::getText);

      final Optional<Statement> statement = Optional.ofNullable(ctx.statement())
          .map(s -> (Statement) visit(s));

      return new Explain(getLocation(ctx), queryId, statement);
    }

    @Override
    public Node visitDescribeFunction(final SqlBaseParser.DescribeFunctionContext ctx) {
      return new DescribeFunction(getLocation(ctx), ctx.identifier().getText());
    }

    @Override
    public Node visitDescribeConnector(final DescribeConnectorContext ctx) {
      return new DescribeConnector(
          getLocation(ctx),
          ParserUtil.getIdentifierText(ctx.identifier())
      );
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

    @Override
    public Node visitRegisterType(final RegisterTypeContext context) {
      return new RegisterType(
          getLocation(context),
          ParserUtil.getIdentifierText(context.identifier()),
          typeParser.getType(context.type())
      );
    }

    private void throwOnUnknownNameOrAlias(final SourceName name) {
      if (sources.isPresent() && !sources.get().contains(name)) {
        throw new KsqlException("'" + name.name() + "' is not a valid stream/table name or alias.");
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

    private static OptionalInt getLimit(final LimitClauseContext limitContext) {
      return limitContext == null
          ? OptionalInt.empty()
          : OptionalInt.of(processIntegerNumber(limitContext.number(), "LIMIT"));
    }

    private <T> T withinPersistentQuery(final Supplier<T> task) {
      if (buildingPersistentQuery) {
        throw new UnsupportedOperationException("Nested query building not supported yet");
      }

      try {
        buildingPersistentQuery = true;
        return task.get();
      } finally {
        buildingPersistentQuery = false;
      }
    }
  }

  private static Set<SourceName> getSources(final ParseTree parseTree) {
    final SourceAccumulator accumulator = new SourceAccumulator();
    accumulator.visit(parseTree);
    return accumulator.getSources();
  }

  private static class SourceAccumulator extends SqlBaseBaseVisitor<Void> {
    final Set<SourceName> sources = new HashSet<>();

    public Set<SourceName> getSources() {
      return ImmutableSet.copyOf(sources);
    }

    @Override
    public Void visitAliasedRelation(final SqlBaseParser.AliasedRelationContext context) {
      super.visitAliasedRelation(context);
      switch (context.children.size()) {
        case 1:
          break;
        case 2:
          sources.add(ParserUtil.getSourceName((SourceNameContext) context.children.get(1)));
          break;
        case 3:
          sources.add(ParserUtil.getSourceName((SourceNameContext) context.children.get(2)));
          break;
        default:
          throw new IllegalArgumentException(
              "AliasedRelationContext must have between 1 and 3 children, but has:"
                  + context.children.size()
          );
      }
      return null;
    }

    @Override
    public Void visitTableName(final SqlBaseParser.TableNameContext context) {
      sources.add(ParserUtil.getSourceName(context.sourceName()));
      return null;
    }
  }
}
