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

import static io.confluent.ksql.schema.ksql.SystemColumns.HEADERS_TYPE;
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
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntervalUnit;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.SqlBaseParser.AlterOptionContext;
import io.confluent.ksql.parser.SqlBaseParser.AlterSourceContext;
import io.confluent.ksql.parser.SqlBaseParser.ArrayConstructorContext;
import io.confluent.ksql.parser.SqlBaseParser.AssertStreamContext;
import io.confluent.ksql.parser.SqlBaseParser.AssertTableContext;
import io.confluent.ksql.parser.SqlBaseParser.AssertTombstoneContext;
import io.confluent.ksql.parser.SqlBaseParser.AssertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DescribeConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DropConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DropTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.ExpressionContext;
import io.confluent.ksql.parser.SqlBaseParser.FloatLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.GracePeriodClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.IdentifierContext;
import io.confluent.ksql.parser.SqlBaseParser.InsertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.IntervalClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.JoinedSourceContext;
import io.confluent.ksql.parser.SqlBaseParser.LimitClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.ListConnectorsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListTypesContext;
import io.confluent.ksql.parser.SqlBaseParser.NumberContext;
import io.confluent.ksql.parser.SqlBaseParser.RegisterTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.RetentionClauseContext;
import io.confluent.ksql.parser.SqlBaseParser.SourceNameContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.WindowUnitContext;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.InsertIntoProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AlterOption;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.AssertStatement;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertTombstone;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateConnector.Type;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DefineVariable;
import io.confluent.ksql.parser.tree.DescribeConnector;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.DropConnector;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.ListConnectorPlugins;
import io.confluent.ksql.parser.tree.ListConnectors;
import io.confluent.ksql.parser.tree.ListConnectors.Scope;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.ListTypes;
import io.confluent.ksql.parser.tree.ListVariables;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.ParserUtil;
import java.math.BigDecimal;
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

  public AssertStatement buildAssertStatement(final ParserRuleContext parseTree) {
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
    private final Set<String> lambdaArgs;

    private boolean buildingPersistentQuery = false;

    Visitor(final Optional<Set<SourceName>> sources, final TypeRegistry typeRegistry) {
      this.sources = Objects.requireNonNull(sources, "sources").map(ImmutableSet::copyOf);
      this.typeParser = SqlTypeParser.create(typeRegistry);
      this.lambdaArgs = new HashSet<>();
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
          context.REPLACE() != null,
          context.EXISTS() != null,
          CreateSourceProperties.from(properties),
          context.SOURCE() != null
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
          context.REPLACE() != null,
          context.EXISTS() != null,
          CreateSourceProperties.from(properties),
          context.SOURCE() != null
      );
    }

    @Override
    public Node visitCreateStreamAs(final SqlBaseParser.CreateStreamAsContext context) {
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));

      disallowLimitClause(query, "STREAM");

      return new CreateStreamAsSelect(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          query,
          context.EXISTS() != null,
          context.REPLACE() != null,
          CreateSourceAsProperties.from(properties)
      );
    }

    @Override
    public Node visitCreateTableAs(final SqlBaseParser.CreateTableAsContext context) {
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));

      disallowLimitClause(query, "TABLE");

      return new CreateTableAsSelect(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          query,
          context.EXISTS() != null,
          context.REPLACE() != null,
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
          type,
          context.EXISTS() != null
      );
    }

    @Override
    public Node visitInsertInto(final SqlBaseParser.InsertIntoContext context) {
      final SourceName targetName = ParserUtil.getSourceName(context.sourceName());
      final Query query = withinPersistentQuery(() -> visitQuery(context.query()));
      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      return new InsertInto(
          getLocation(context),
          targetName,
          query,
          InsertIntoProperties.from(properties));
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
          context.EXISTS() != null,
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

      final Optional<OutputRefinement> outputRefinement;

      if (pullQuery) {
        outputRefinement = Optional.empty();
      } else if (buildingPersistentQuery) {
        outputRefinement = Optional.of(Optional
            .ofNullable(context.resultMaterialization())
            .map(rm -> rm.FINAL() == null
                ? OutputRefinement.CHANGES
                : OutputRefinement.FINAL
            )
            .orElse(OutputRefinement.CHANGES));
        // Else must be a push query, which must specify a materialization
      } else {
        outputRefinement = Optional
            .of(context.resultMaterialization().CHANGES() == null
                ? OutputRefinement.FINAL
                : OutputRefinement.CHANGES
            );
      }
      final Optional<RefinementInfo> refinementInfo = outputRefinement.map(RefinementInfo::of);


      final OptionalInt limit = getLimit(context.limitClause());

      return new Query(
          getLocation(context),
          select,
          from,
          visitIfPresent(context.windowExpression(), WindowExpression.class),
          visitIfPresent(context.where, Expression.class),
          visitIfPresent(context.groupBy(), GroupBy.class),
          visitIfPresent(context.partitionBy(), PartitionBy.class),
          visitIfPresent(context.having, Expression.class),
          refinementInfo,
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

    private static WindowTimeClause getTimeClause(
        final NumberContext number,
        final WindowUnitContext unitCtx) {
      return new WindowTimeClause(
          Long.parseLong(number.getText()),
          WindowExpression.getWindowUnit(unitCtx.getText().toUpperCase())
      );
    }

    private static Optional<WindowTimeClause> gracePeriodClause(
        final GracePeriodClauseContext graceCtx) {
      return graceCtx != null
          ? Optional.of(getTimeClause(graceCtx.number(), graceCtx.windowUnit()))
          : Optional.empty();
    }

    private static Optional<WindowTimeClause> retentionClause(
        final RetentionClauseContext retentionCtx) {
      return retentionCtx != null
          ? Optional.of(getTimeClause(retentionCtx.number(), retentionCtx.windowUnit()))
          : Optional.empty();
    }

    @Override
    public Node visitHoppingWindowExpression(
        final SqlBaseParser.HoppingWindowExpressionContext ctx) {

      final List<SqlBaseParser.NumberContext> numberList = ctx.number();
      final List<SqlBaseParser.WindowUnitContext> windowUnits = ctx.windowUnit();
      return new HoppingWindowExpression(
          getLocation(ctx),
          getTimeClause(numberList.get(0), windowUnits.get(0)),
          getTimeClause(numberList.get(1), windowUnits.get(1)),
          retentionClause(ctx.retentionClause()),
          gracePeriodClause(ctx.gracePeriodClause())
      );
    }

    @Override
    public Node visitTumblingWindowExpression(
        final SqlBaseParser.TumblingWindowExpressionContext ctx
    ) {
      return new TumblingWindowExpression(
          getLocation(ctx),
          getTimeClause(ctx.number(), ctx.windowUnit()),
          retentionClause(ctx.retentionClause()),
          gracePeriodClause(ctx.gracePeriodClause())
      );
    }

    @Override
    public Node visitSessionWindowExpression(
        final SqlBaseParser.SessionWindowExpressionContext ctx
    ) {
      return new SessionWindowExpression(
          getLocation(ctx),
          getTimeClause(ctx.number(), ctx.windowUnit()),
          retentionClause(ctx.retentionClause()),
          gracePeriodClause(ctx.gracePeriodClause())
      );
    }

    private static Node visitWithinExpression(final SqlBaseParser.WithinExpressionContext ctx) {
      final Pair<Long, TimeUnit> beforeSize;
      final Pair<Long, TimeUnit> afterSize;
      final Optional<WindowTimeClause> gracePeriod;

      if (ctx instanceof SqlBaseParser.SingleJoinWindowContext) {

        final SqlBaseParser.SingleJoinWindowContext singleWithin
            = (SqlBaseParser.SingleJoinWindowContext) ctx;

        beforeSize = getSizeAndUnitFromJoinWindowSize(singleWithin.joinWindowSize());
        afterSize = beforeSize;
        gracePeriod = gracePeriodClause(singleWithin.gracePeriodClause());
      } else if (ctx instanceof SqlBaseParser.JoinWindowWithBeforeAndAfterContext) {
        final SqlBaseParser.JoinWindowWithBeforeAndAfterContext beforeAndAfterJoinWindow
            = (SqlBaseParser.JoinWindowWithBeforeAndAfterContext) ctx;

        beforeSize = getSizeAndUnitFromJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(0));
        afterSize = getSizeAndUnitFromJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(1));
        gracePeriod = gracePeriodClause(beforeAndAfterJoinWindow.gracePeriodClause());
      } else {
        throw new RuntimeException("Expecting either a single join window, ie \"WITHIN 10 "
            + "seconds\" or \"WITHIN 10 seconds GRACE PERIOD 2 seconds\", or a join window with "
            + "before and after specified, ie. \"WITHIN (10 seconds, 20 seconds)\" or "
            + "WITHIN (10 seconds, 20 seconds) GRACE PERIOD 5 seconds");
      }
      return new WithinExpression(
          getLocation(ctx),
          beforeSize.left,
          afterSize.left,
          beforeSize.right,
          afterSize.right,
          gracePeriod
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
    public Node visitGroupBy(final SqlBaseParser.GroupByContext ctx) {
      final List<Expression> expressions = visit(ctx.valueExpression(), Expression.class);

      return new GroupBy(getLocation(ctx), expressions);
    }

    @Override
    public Node visitPartitionBy(final SqlBaseParser.PartitionByContext ctx) {
      final List<Expression> expressions = visit(ctx.valueExpression(), Expression.class);

      return new PartitionBy(getLocation(ctx), expressions);
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
    public Node visitLambda(final SqlBaseParser.LambdaContext context) {
      final List<String> arguments = context.identifier().stream()
          .map(ParserUtil::getIdentifierText)
          .collect(toList());

      final Set<String> previousLambdaArgs = new HashSet<>(lambdaArgs);
      lambdaArgs.addAll(arguments);
      final Expression body = (Expression) visit(context.expression());
      lambdaArgs.clear();
      lambdaArgs.addAll(previousLambdaArgs);
      return new LambdaFunctionCall(getLocation(context), arguments, body);
    }

    @Override
    public Node visitListTopics(final SqlBaseParser.ListTopicsContext context) {
      return new ListTopics(getLocation(context),
          context.ALL() != null, context.EXTENDED() != null);
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
    public Node visitListConnectorPlugins(final SqlBaseParser.ListConnectorPluginsContext ctx) {
      return new ListConnectorPlugins((getLocation(ctx)));
    }

    @Override
    public Node visitDropType(final DropTypeContext ctx) {
      return new DropType(
          getLocation(ctx),
          getIdentifierText(ctx.identifier()),
          ctx.EXISTS() != null
      );
    }

    @Override
    public Node visitAlterSource(final AlterSourceContext ctx) {
      return new AlterSource(
          ParserUtil.getSourceName(ctx.sourceName()),
          ctx.STREAM() != null ? DataSourceType.KSTREAM : DataSourceType.KTABLE,
          ctx.alterOption()
              .stream()
              .map(gf -> (AlterOption) visit(gf))
              .collect(Collectors.toList())
      );
    }

    @Override
    public Node visitAlterOption(final AlterOptionContext ctx) {
      return new AlterOption(
          getIdentifierText(ctx.identifier()),
          typeParser.getType(ctx.type())
      );
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
              new QueryId(ParserUtil.getIdentifierText(false, context.identifier()))
          );
    }

    @Override
    public Node visitShowColumns(final SqlBaseParser.ShowColumnsContext context) {
      // Special check to allow `DESCRIBE TABLES` while still allowing
      // users to maintain statements that used TABLES as a column name
      if (context.sourceName().identifier() instanceof SqlBaseParser.UnquotedIdentifierContext
          && context.sourceName().getText().toUpperCase().equals("TABLES")) {
        return new DescribeTables(getLocation(context), context.EXTENDED() != null);
      }
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
    public Node visitListVariables(final SqlBaseParser.ListVariablesContext context) {
      return new ListVariables(getLocation(context));
    }

    @Override
    public Node visitSetProperty(final SqlBaseParser.SetPropertyContext context) {
      final String propertyName = ParserUtil.unquote(context.STRING(0).getText(), "'");
      final String propertyValue = ParserUtil.unquote(context.STRING(1).getText(), "'");
      return new SetProperty(getLocation(context), propertyName, propertyValue);
    }

    @Override
    public Node visitAlterSystemProperty(final SqlBaseParser.AlterSystemPropertyContext context) {
      final String propertyName = ParserUtil.unquote(context.STRING(0).getText(), "'");
      final String propertyValue = ParserUtil.unquote(context.STRING(1).getText(), "'");
      return new AlterSystemProperty(getLocation(context), propertyName, propertyValue);
    }

    @Override
    public Node visitUnsetProperty(final SqlBaseParser.UnsetPropertyContext context) {
      final String propertyName = ParserUtil.unquote(context.STRING().getText(), "'");
      return new UnsetProperty(getLocation(context), propertyName);
    }

    @Override
    public Node visitDefineVariable(final SqlBaseParser.DefineVariableContext context) {
      final String variableName = context.variableName().getText();
      final String variableValue = ParserUtil.unquote(context.variableValue().getText(), "'");
      return new DefineVariable(getLocation(context), variableName, variableValue);
    }

    @Override
    public Node visitUndefineVariable(final SqlBaseParser.UndefineVariableContext context) {
      final String variableName = context.variableName().getText();
      return new UndefineVariable(getLocation(context), variableName);
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
    public Node visitJoinRelation(final SqlBaseParser.JoinRelationContext joinRelationContext) {
      final AliasedRelation left = (AliasedRelation) visit(joinRelationContext.left);
      final ImmutableList<JoinedSource> rights = joinRelationContext
          .joinedSource()
          .stream()
          .map(this::visitJoinedSource)
          .collect(ImmutableList.toImmutableList());

      return new Join(getLocation(joinRelationContext), left, rights);
    }

    @Override
    public JoinedSource visitJoinedSource(final JoinedSourceContext context) {
      if (context.joinCriteria().ON() == null) {
        throw new KsqlException("Invalid join criteria specified. KSQL only supports joining on "
            + "column values. For example `... left JOIN right on left.col = "
            + "right.col ...`. Tables can only be joined on the Table's key "
            + "column. KSQL will repartition streams if the column in the join "
            + "criteria is not the key column.");
      }

      final JoinCriteria criteria =
          new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
      final JoinedSource.Type joinType;
      final SqlBaseParser.JoinTypeContext joinTypeContext = context.joinType();
      if (joinTypeContext instanceof SqlBaseParser.LeftJoinContext) {
        joinType = JoinedSource.Type.LEFT;
      } else if (joinTypeContext instanceof SqlBaseParser.RightJoinContext) {
        joinType = JoinedSource.Type.RIGHT;
      } else if (joinTypeContext instanceof SqlBaseParser.OuterJoinContext) {
        joinType = JoinedSource.Type.OUTER;
      }  else if (joinTypeContext instanceof SqlBaseParser.InnerJoinContext) {
        joinType = JoinedSource.Type.INNER;
      } else {
        throw new KsqlException("Invalid join type - " + joinTypeContext.getText());
      }

      WithinExpression withinExpression = null;
      if (context.joinWindow() != null) {
        withinExpression = (WithinExpression) visitWithinExpression(
            context.joinWindow().withinExpression());
      }
      final AliasedRelation right = (AliasedRelation) visit(context.aliasedRelation());

      return new JoinedSource(
          getLocation(context),
          right,
          joinType,
          criteria,
          Optional.ofNullable(withinExpression)
      );
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

      final Optional<String> escape = Optional.ofNullable(context.escape)
          .map(Token::getText)
          .map(s -> ParserUtil.unquote(s, "'"));

      escape.ifPresent(s -> {
        if (s.length() != 1) {
          throw new KsqlException(
              getLocation(context.escape) + ": Expected single character escape but got: " + s
          );
        }
      });

      final Expression result = new LikePredicate(
          getLocation(context),
          (Expression) visit(context.value),
          (Expression) visit(context.pattern),
          escape.map(s -> s.charAt(0))
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
    public Node visitArrayConstructor(final ArrayConstructorContext context) {
      final ImmutableList.Builder<Expression> values = ImmutableList.builder();

      for (ExpressionContext exp : context.expression()) {
        values.add((Expression) visit(exp));
      }

      return new CreateArrayExpression(
          getLocation(context),
          values.build()
      );
    }

    @Override
    public Node visitMapConstructor(final SqlBaseParser.MapConstructorContext context) {
      final ImmutableMap.Builder<Expression, Expression> values = ImmutableMap.builder();

      final List<ExpressionContext> expression = context.expression();
      for (int i = 0; i < expression.size(); i += 2) {
        values.put(
            (Expression) visit(expression.get(i)),
            (Expression) visit(expression.get(i + 1))
        );
      }

      return new CreateMapExpression(
          getLocation(context),
          values.build()
      );
    }

    @Override
    public Node visitStructConstructor(final SqlBaseParser.StructConstructorContext context) {
      final ImmutableList.Builder<Field> fields = ImmutableList.builder();

      for (int i = 0; i < context.identifier().size(); i++) {
        fields.add(new Field(
            ParserUtil.getIdentifierText(context.identifier(i)),
            (Expression) visit(context.expression(i))
        ));
      }

      return new CreateStructExpression(
          getLocation(context),
          fields.build()
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
      final UnqualifiedColumnReferenceExp column = ColumnReferenceParser.resolve(context);
      if (lambdaArgs.contains(column.toString())) {
        return new LambdaVariable(column.toString());
      }
      return column;
    }

    @Override
    public Node visitQualifiedColumnReference(
        final SqlBaseParser.QualifiedColumnReferenceContext context) {
      final QualifiedColumnReferenceExp columnReferenceExp = ColumnReferenceParser.resolve(context);
      throwOnUnknownNameOrAlias(columnReferenceExp.getQualifier());
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
      final List<Expression> expressionList = visit(context.functionArgument(), Expression.class);
      expressionList.addAll(visit(context.lambdaFunction(), Expression.class));
      return new FunctionCall(
          getLocation(context),
          FunctionName.of(ParserUtil.getIdentifierText(context.identifier())),
          expressionList
      );
    }

    @Override
    public Node visitFunctionArgument(final SqlBaseParser.FunctionArgumentContext context) {
      if (context.windowUnit() != null) {
        return visitWindowUnit(context.windowUnit());
      } else {
        return visit(context.expression());
      }
    }

    @Override
    public Node visitWindowUnit(final SqlBaseParser.WindowUnitContext context) {
      return new IntervalUnit(WindowExpression.getWindowUnit(context.getText().toUpperCase()));
    }

    @Override
    public Node visitTableElement(final SqlBaseParser.TableElementContext context) {
      final io.confluent.ksql.execution.expression.tree.Type type =
          typeParser.getType(context.type());
      final ColumnConstraints constraints =
          ParserUtil.getColumnConstraints(context.columnConstraints());
      if (constraints.isHeaders()) {
        throwOnIncorrectHeaderColumnType(type.getSqlType(), constraints.getHeaderKey());
      }
      return new TableElement(
          getLocation(context),
          ColumnName.of(ParserUtil.getIdentifierText(context.identifier())),
          type,
          constraints);
    }

    private void throwOnIncorrectHeaderColumnType(
        final SqlType type, final Optional<String> headerKey) {
      if (headerKey.isPresent()) {
        if (type != SqlTypes.BYTES) {
          throw new KsqlException(String.format(
              "Invalid type for HEADER('%s') column: expected BYTES, got %s",
              headerKey.get(),
              type)
          );
        }
      } else {
        if (!type.toString().equals(HEADERS_TYPE.toString())) {
          throw new KsqlException(
              "Invalid type for HEADERS column: expected " + HEADERS_TYPE + ", got " + type);
        }
      }
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

      if (type.equals("DECIMAL")) {
        return new DecimalLiteral(location, new BigDecimal(value));
      }

      throw new KsqlException("Unknown type: " + type + ", location:" + location);
    }

    @Override
    public Node visitIntegerLiteral(final SqlBaseParser.IntegerLiteralContext context) {
      return ParserUtil.visitIntegerLiteral(context);
    }

    @Override
    public Node visitFloatLiteral(final FloatLiteralContext context) {
      return ParserUtil.parseFloatLiteral(context);
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
          .map(ParserUtil::getIdentifierText);

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
    public Node visitDescribeStreams(final SqlBaseParser.DescribeStreamsContext context) {
      return new DescribeStreams(
          getLocation(context), context.EXTENDED() != null);
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
          typeParser.getType(context.type()),
          context.EXISTS() != null
      );
    }

    @Override
    public Node visitAssertValues(final AssertValuesContext context) {
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

      final InsertValues insertValues = new InsertValues(
          targetLocation,
          targetName,
          columns,
          visit(context.values().valueExpression(), Expression.class));

      return new AssertValues(targetLocation, insertValues);
    }

    @Override
    public Node visitAssertTombstone(final AssertTombstoneContext context) {
      final SourceName targetName = ParserUtil.getSourceName(context.sourceName());
      final Optional<NodeLocation> targetLocation = getLocation(context.sourceName());

      final List<ColumnName> keyColumns;
      if (context.columns() != null) {
        keyColumns = context.columns().identifier()
            .stream()
            .map(ParserUtil::getIdentifierText)
            .map(ColumnName::of)
            .collect(Collectors.toList());
      } else {
        keyColumns = ImmutableList.of();
      }

      final InsertValues insertValues = new InsertValues(
          targetLocation,
          targetName,
          keyColumns,
          visit(context.values().valueExpression(), Expression.class));

      return new AssertTombstone(targetLocation, insertValues);
    }

    @Override
    public Node visitAssertStream(final AssertStreamContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);

      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final CreateStream createStream = new CreateStream(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          TableElements.of(elements),
          false,
          false,
          CreateSourceProperties.from(properties),
          false
      );

      return new AssertStream(getLocation(context), createStream);
    }

    @Override
    public Node visitAssertTable(final AssertTableContext context) {
      final List<TableElement> elements = context.tableElements() == null
          ? ImmutableList.of()
          : visit(context.tableElements().tableElement(), TableElement.class);

      final Map<String, Literal> properties = processTableProperties(context.tableProperties());

      final CreateTable createTable = new CreateTable(
          getLocation(context),
          ParserUtil.getSourceName(context.sourceName()),
          TableElements.of(elements),
          false,
          false,
          CreateSourceProperties.from(properties),
          false
      );

      return new AssertTable(getLocation(context), createTable);
    }


    private void throwOnUnknownNameOrAlias(final SourceName name) {
      if (sources.isPresent() && !sources.get().contains(name)) {
        throw new KsqlException("'" + name.text() + "' is not a valid stream/table name or alias.");
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
    try {
      final SourceAccumulator accumulator = new SourceAccumulator();
      accumulator.visit(parseTree);
      return accumulator.getSources();
    } catch (final StackOverflowError e) {
      throw new KsqlException("Error processing statement: Statement is too large to parse. "
          + "This may be caused by having too many nested expressions in the statement.");
    }
  }

  private static void disallowLimitClause(final Query query, final String streamOrTable)
          throws KsqlException {
    if (query.getLimit().isPresent()) {
      final String errorMessage = String.format(
              "CREATE %s AS SELECT statements don't support LIMIT clause.",
              streamOrTable);
      throw new KsqlException(errorMessage);
    }
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
