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

package io.confluent.ksql.parser;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.Array;
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
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropTopic;
import io.confluent.ksql.parser.tree.ExistsPredicate;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ExplainFormat;
import io.confluent.ksql.parser.tree.ExplainType;
import io.confluent.ksql.parser.tree.ExportCatalog;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GenericLiteral;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.IntervalLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryBody;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RunScript;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.ShowFunctions;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.SimpleGroupBy;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Statements;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithQuery;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class AstBuilder extends SqlBaseBaseVisitor<Node> {


  private int selectItemIndex = 0;

  private static final String DEFAULT_WINDOW_NAME = "StreamWindow";

  private DataSourceExtractor dataSourceExtractor;

  public AstBuilder(final DataSourceExtractor dataSourceExtractor) {
    this.dataSourceExtractor = dataSourceExtractor;
  }

  @Override
  public Node visitStatements(final SqlBaseParser.StatementsContext context) {
    final List<Statement> statementList = new ArrayList<>();
    for (final SqlBaseParser.SingleStatementContext stmtContext : context.singleStatement()) {
      final Statement statement = (Statement) visitSingleStatement(stmtContext);
      statementList.add(statement);
    }
    return new Statements(statementList);
  }

  @Override
  public Node visitSingleStatement(final SqlBaseParser.SingleStatementContext context) {
    return (Statement) visit(context.statement());
  }

  @Override
  public Node visitQuerystatement(final SqlBaseParser.QuerystatementContext ctx) {
    return (Statement) visitChildren(ctx);
  }

  @Override
  public Node visitSingleExpression(final SqlBaseParser.SingleExpressionContext context) {
    return visit(context.expression());
  }

  // ******************* statements **********************


  private Map<String, Expression> processTableProperties(
      final TablePropertiesContext tablePropertiesContext
  ) {
    final ImmutableMap.Builder<String, Expression> properties = ImmutableMap.builder();
    if (tablePropertiesContext != null) {
      for (final TablePropertyContext tablePropContext : tablePropertiesContext.tableProperty()) {
        properties.put(
            getIdentifierText(tablePropContext.identifier()),
            (Expression) visit(tablePropContext.expression())
        );
      }
    }
    return properties.build();
  }

  @Override
  public Node visitCreateTable(final SqlBaseParser.CreateTableContext context) {
    return new CreateTable(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        visit(context.tableElement(), TableElement.class),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitRegisterTopic(final SqlBaseParser.RegisterTopicContext context) {
    return new RegisterTopic(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitCreateStream(final SqlBaseParser.CreateStreamContext context) {
    return new CreateStream(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        visit(context.tableElement(), TableElement.class),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitCreateStreamAs(final SqlBaseParser.CreateStreamAsContext context) {
    Optional<Expression> partitionByColumn = Optional.empty();
    if (context.identifier() != null) {
      partitionByColumn = Optional.of(new QualifiedNameReference(
          QualifiedName.of(getIdentifierText(context.identifier()))));
    }

    return new CreateStreamAsSelect(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        (Query) visitQuery(context.query()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties()),
        partitionByColumn
    );
  }

  @Override
  public Node visitCreateTableAs(final SqlBaseParser.CreateTableAsContext context) {
    return new CreateTableAsSelect(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        (Query) visitQuery(context.query()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitInsertInto(final SqlBaseParser.InsertIntoContext context) {
    Optional<Expression> partitionByColumn = Optional.empty();
    if (context.identifier() != null) {
      partitionByColumn = Optional.of(new QualifiedNameReference(
          QualifiedName.of(getIdentifierText(context.identifier()))));
    }
    return new InsertInto(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        (Query) visitQuery(context.query()),
        partitionByColumn);
  }

  @Override
  public Node visitDropTopic(final SqlBaseParser.DropTopicContext context) {
    return new DropTopic(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null
    );
  }

  @Override
  public Node visitDropTable(final SqlBaseParser.DropTableContext context) {
    return new DropTable(
        Optional.of(getLocation(context)),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null,
        context.DELETE() != null
    );
  }

  @Override
  public Node visitDropStream(final SqlBaseParser.DropStreamContext context) {
    return new DropStream(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null,
        context.DELETE() != null
    );
  }

  // ********************** query expressions ********************

  @Override
  public Node visitQuery(final SqlBaseParser.QueryContext context) {
    final Query body = (Query) visit(context.queryNoWith());

    return new Query(
        getLocation(context),
        body.getQueryBody(),
        body.getLimit()
    );
  }


  @Override
  public Node visitNamedQuery(final SqlBaseParser.NamedQueryContext context) {
    return new WithQuery(
        getLocation(context),
        context.name.getText(),
        (Query) visit(context.query()),
        Optional.ofNullable(getColumnAliases(context.columnAliases()))
    );
  }

  @Override
  public Node visitQueryNoWith(final SqlBaseParser.QueryNoWithContext context) {

    final QueryBody term = (QueryBody) visit(context.queryTerm());

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by limit, fold the order by and limit
      // clauses into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      final QuerySpecification query = (QuerySpecification) term;
      return new Query(
          getLocation(context),
          new QuerySpecification(
              getLocation(context),
              query.getSelect(),
              query.getInto(),
              query.isShouldCreateInto(),
              query.getFrom(),
              query.getWindowExpression(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              getTextIfPresent(context.limit)
          ),
          Optional.<String>empty()
      );
    }

    return new Query(
        getLocation(context),
        term,
        getTextIfPresent(context.limit)
    );
  }


  @Override
  public Node visitQuerySpecification(final SqlBaseParser.QuerySpecificationContext context) {
    final Table into;
    if (context.into != null) {
      into = (Table) visit(context.into);
    } else {
      // TODO: Generate a unique name
      final String intoName = "KSQL_Stream_" + System.currentTimeMillis();
      into = new Table(QualifiedName.of(intoName), true);
    }

    final Relation from = (Relation) visit(context.from);

    Select select = new Select(
        getLocation(context.SELECT()),
        false,
        visit(context.selectItem(), SelectItem.class)
    );
    select = new Select(
        getLocation(context.SELECT()),
        select.isDistinct(),
        extractSelectItems(select, from)
    );
    getResultDatasource(select, into);

    return new QuerySpecification(
        getLocation(context),
        select,
        into,
        true,
        from,
        visitIfPresent(context.windowExpression(), WindowExpression.class),
        visitIfPresent(context.where, Expression.class),
        visitIfPresent(context.groupBy(), GroupBy.class),
        visitIfPresent(context.having, Expression.class),
        Optional.<String>empty()
    );
  }

  private List<SelectItem> extractSelectItems(final Select select, final Relation from) {
    final List<SelectItem> selectItems = new ArrayList<>();
    for (final SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof AllColumns) {
        selectItems.addAll(getSelectStarItems(selectItem, from));

      } else if (selectItem instanceof SingleColumn) {
        selectItems.add(selectItem);
      } else {
        throw new IllegalArgumentException(
            "Unsupported SelectItem type: " + selectItem.getClass().getName());
      }
    }
    return selectItems;
  }

  private List<SelectItem> getSelectStarItems(final SelectItem selectItem, final Relation from) {
    final List<SelectItem> selectItems = new ArrayList<>();
    final AllColumns allColumns = (AllColumns) selectItem;

    final NodeLocation location = allColumns.getLocation().orElse(null);
    if (from instanceof Join) {
      final Join join = (Join) from;
      if (allColumns.getPrefix().isPresent()) {
        final String alias = allColumns.getPrefix().get().toString();
        final StructuredDataSource source
            = getDataSourceForAlias(join,
            alias);
        if (source == null) {
          throw new InvalidColumnReferenceException("Source for alias '"
            + allColumns.getPrefix().get() + "' doesn't exist");
        }
        addFieldsFromDataSource(selectItems, source, location, alias);
      } else {
        final AliasedRelation left = (AliasedRelation) join.getLeft();
        final StructuredDataSource
            leftDataSource =
            dataSourceExtractor.getMetaStore().getSource(left.getRelation().toString());
        if (leftDataSource == null) {
          throw new InvalidColumnReferenceException(left.getRelation().toString()
              + " does not exist.");
        }
        final AliasedRelation right = (AliasedRelation) join.getRight();
        final StructuredDataSource rightDataSource =
            dataSourceExtractor.getMetaStore().getSource(right.getRelation().toString());
        if (rightDataSource == null) {
          throw new InvalidColumnReferenceException(right.getRelation().toString()
              + " does not exist.");
        }
        addFieldsFromDataSource(selectItems, leftDataSource, location, left.getAlias());
        addFieldsFromDataSource(selectItems, rightDataSource, location, right.getAlias());
      }
    } else {
      final AliasedRelation fromRel = (AliasedRelation) from;
      final StructuredDataSource fromDataSource =
          dataSourceExtractor.getMetaStore()
              .getSource(((Table) fromRel.getRelation()).getName().getSuffix());
      if (fromDataSource == null) {
        throw new InvalidColumnReferenceException(
            ((Table) fromRel.getRelation()).getName().getSuffix() + " does not exist."
        );
      }
      for (final Field field : fromDataSource.getSchema().fields()) {
        final QualifiedNameReference qualifiedNameReference =
            new QualifiedNameReference(location, QualifiedName
                .of(fromDataSource.getName() + "." + field.name()));
        final SingleColumn newSelectItem =
            new SingleColumn(qualifiedNameReference, field.name());
        selectItems.add(newSelectItem);
      }
    }
    return selectItems;
  }

  private void addFieldsFromDataSource(final List<SelectItem> selectItems,
                                       final StructuredDataSource dataSource,
                                       final NodeLocation location,
                                       final String alias) {
    for (final Field field : dataSource.getSchema().fields()) {
      final QualifiedNameReference qualifiedNameReference =
          new QualifiedNameReference(
              location,
              QualifiedName.of(alias + "." + field.name())
          );
      selectItems.add(new SingleColumn(
          qualifiedNameReference,
          alias + "_" + field.name()
      ));
    }
  }

  private StructuredDataSource getDataSourceForAlias(final Join join,
                                                     final String alias) {
    final AliasedRelation leftAliased = (AliasedRelation) join.getLeft();
    final AliasedRelation rightAliased = (AliasedRelation) join.getRight();
    if (leftAliased.getAlias().equalsIgnoreCase(alias)) {
      return dataSourceExtractor
          .getMetaStore()
          .getSource(leftAliased.getRelation().toString());
    } else if (rightAliased.getAlias().equalsIgnoreCase(alias)) {
      return dataSourceExtractor
          .getMetaStore()
          .getSource(rightAliased.getRelation().toString());
    }
    throw new KsqlException("Invalid alias used in join: alias='"
        + alias + "'. Available aliases '"
        + leftAliased.getAlias() + "' and '"
        + rightAliased.getAlias() + "'");
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
      return new WindowExpression(windowName, tumblingWindowExpression);
    } else if (ctx.hoppingWindowExpression() != null) {
      final HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression)
          visitHoppingWindowExpression(ctx.hoppingWindowExpression());

      return new WindowExpression(windowName, hoppingWindowExpression);
    } else if (ctx.sessionWindowExpression() != null) {
      final SessionWindowExpression sessionWindowExpression = (SessionWindowExpression)
          visitSessionWindowExpression(ctx.sessionWindowExpression());
      return new WindowExpression(windowName, sessionWindowExpression);
    }
    throw new KsqlException("Window description is not correct.");
  }

  @Override
  public Node visitHoppingWindowExpression(final SqlBaseParser.HoppingWindowExpressionContext ctx) {

    final List<SqlBaseParser.NumberContext> numberList = ctx.number();
    final List<SqlBaseParser.WindowUnitContext> windowUnits = ctx.windowUnit();
    final String sizeStr = numberList.get(0).getText();
    final String advanceByStr = numberList.get(1).getText();

    final String sizeUnit = windowUnits.get(0).getText();
    final String advanceByUnit = windowUnits.get(1).getText();
    return new HoppingWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase()),
        Long.parseLong(advanceByStr),
        WindowExpression.getWindowUnit(advanceByUnit.toUpperCase())
    );
  }

  @Override
  public Node visitTumblingWindowExpression(
      final SqlBaseParser.TumblingWindowExpressionContext ctx) {
    final String sizeStr = ctx.number().getText();
    final String sizeUnit = ctx.windowUnit().getText();
    return new TumblingWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
    );
  }

  @Override
  public Node visitSessionWindowExpression(final SqlBaseParser.SessionWindowExpressionContext ctx) {
    final String sizeStr = ctx.number().getText();
    final String sizeUnit = ctx.windowUnit().getText();
    return new SessionWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
    );
  }

  public Node visitWithinExpression(final SqlBaseParser.WithinExpressionContext ctx) {
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
    return new WithinExpression(beforeSize.left, afterSize.left, beforeSize.right, afterSize.right);
  }

  private Pair<Long, TimeUnit> getSizeAndUnitFromJoinWindowSize(
      final SqlBaseParser.JoinWindowSizeContext joinWindowSize) {
    return new Pair<>(Long.parseLong(joinWindowSize.number().getText()),
                      WindowExpression.getWindowUnit(
                          joinWindowSize.windowUnit().getText().toUpperCase()));
  }

  @Override
  public Node visitGroupBy(final SqlBaseParser.GroupByContext context) {
    return new GroupBy(
        getLocation(context),
        false,
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
    if (context.qualifiedName() != null) {
      return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    return new AllColumns(getLocation(context));
  }

  @Override
  public Node visitSelectSingle(final SqlBaseParser.SelectSingleContext context) {
    final Expression selectItemExpression = (Expression) visit(context.expression());
    Optional<String> alias = Optional
        .ofNullable(context.identifier())
        .map(AstBuilder::getIdentifierText);
    if (!alias.isPresent()) {
      if (selectItemExpression instanceof QualifiedNameReference) {
        final QualifiedNameReference
            qualifiedNameReference =
            (QualifiedNameReference) selectItemExpression;
        alias = Optional.of(qualifiedNameReference.getName().getSuffix());
      } else if (selectItemExpression instanceof DereferenceExpression) {
        final DereferenceExpression dereferenceExpression =
            (DereferenceExpression) selectItemExpression;
        final String dereferenceExpressionString = dereferenceExpression.toString();
        if ((dataSourceExtractor.getJoinLeftSchema() != null) && (
            dataSourceExtractor
                .getCommonFieldNames()
                .contains(
                    dereferenceExpression.getFieldName()
                )
          )) {
          alias = Optional.of(replaceDotFieldRef(dereferenceExpressionString));
        } else if (dereferenceExpressionString.contains(KsqlConstants.STRUCT_FIELD_REF)) {
          alias = Optional.of(
              replaceDotFieldRef(
                  dereferenceExpressionString.substring(
                      dereferenceExpressionString.indexOf(KsqlConstants.DOT) + 1)));
        } else {
          alias = Optional.of(dereferenceExpression.getFieldName());
        }
      } else {
        alias = Optional.of("KSQL_COL_" + selectItemIndex);
      }
    } else {
      alias = Optional.of(alias.get());
    }
    selectItemIndex++;
    return new SingleColumn(getLocation(context), selectItemExpression, alias);
  }

  private static String replaceDotFieldRef(final String input) {
    return input.replace(KsqlConstants.DOT, "_").replace(KsqlConstants.STRUCT_FIELD_REF, "__");
  }

  @Override
  public Node visitQualifiedName(final SqlBaseParser.QualifiedNameContext context) {
    return visitChildren(context);
  }

  @Override
  public Node visitTable(final SqlBaseParser.TableContext context) {
    return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
  }

  @Override
  public Node visitExportCatalog(final SqlBaseParser.ExportCatalogContext context) {
    return new ExportCatalog(Optional.ofNullable(getLocation(context)), context.STRING().getText());
  }

  @Override
  public Node visitRunScript(final SqlBaseParser.RunScriptContext context) {
    return new RunScript(Optional.ofNullable(getLocation(context)), context.STRING().getText());
  }

  @Override
  public Node visitListRegisteredTopics(final SqlBaseParser.ListRegisteredTopicsContext context) {
    return new ListRegisteredTopics(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitListTopics(final SqlBaseParser.ListTopicsContext context) {
    return new ListTopics(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitListStreams(final SqlBaseParser.ListStreamsContext context) {
    return new ListStreams(
        Optional.ofNullable(getLocation(context)), context.EXTENDED() != null);
  }

  @Override
  public Node visitListTables(final SqlBaseParser.ListTablesContext context) {
    return new ListTables(
        Optional.ofNullable(getLocation(context)), context.EXTENDED() != null);
  }


  @Override
  public Node visitListQueries(final SqlBaseParser.ListQueriesContext context) {
    return new ListQueries(
        Optional.ofNullable(getLocation(context)), context.EXTENDED() != null);
  }

  @Override
  public Node visitListFunctions(final SqlBaseParser.ListFunctionsContext ctx) {
    return new ShowFunctions(Optional.of(getLocation(ctx)));
  }

  @Override
  public Node visitTerminateQuery(final SqlBaseParser.TerminateQueryContext context) {
    return new TerminateQuery(getLocation(context), context.qualifiedName().getText());
  }

  @Override
  public Node visitShowColumns(final SqlBaseParser.ShowColumnsContext context) {
    return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()),
                           context.TOPIC() != null, context.EXTENDED() != null
    );
  }

  @Override
  public Node visitListProperties(final SqlBaseParser.ListPropertiesContext context) {
    return new ListProperties(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitSetProperty(final SqlBaseParser.SetPropertyContext context) {
    final String propertyName = unquote(context.STRING(0).getText(), "'");
    final String propertyValue = unquote(context.STRING(1).getText(), "'");
    return new SetProperty(Optional.ofNullable(getLocation(context)), propertyName, propertyValue);
  }

  @Override
  public Node visitUnsetProperty(final SqlBaseParser.UnsetPropertyContext context) {
    final String propertyName = unquote(context.STRING().getText(), "'");
    return new UnsetProperty(Optional.ofNullable(getLocation(context)), propertyName);
  }

  @Override
  public Node visitPrintTopic(final SqlBaseParser.PrintTopicContext context) {
    final boolean fromBeginning = context.FROM() != null;

    QualifiedName topicName = null;
    if (context.STRING() != null) {
      topicName = QualifiedName.of(unquote(context.STRING().getText(), "'"));
    } else {
      topicName = getQualifiedName(context.qualifiedName());
    }
    if (context.number() == null) {
      return new PrintTopic(
          getLocation(context),
          topicName,
          fromBeginning,
          Optional.empty()
      );
    } else if (context.number() instanceof SqlBaseParser.IntegerLiteralContext) {
      final SqlBaseParser.IntegerLiteralContext integerLiteralContext =
          (SqlBaseParser.IntegerLiteralContext) context.number();
      final IntegerLiteral literal = (IntegerLiteral) visitIntegerLiteral(integerLiteralContext);
      return new PrintTopic(
          getLocation(context),
          topicName,
          fromBeginning,
          Optional.of(literal.getValue())
      );
    } else {
      throw new KsqlException("Interval value should be integer in 'PRINT' command!");
    }

  }

  @Override
  public Node visitNumericLiteral(final SqlBaseParser.NumericLiteralContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Node visitSubquery(final SqlBaseParser.SubqueryContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
  }

  @Override
  public Node visitInlineTable(final SqlBaseParser.InlineTableContext context) {
    return new Values(getLocation(context), visit(context.expression(), Expression.class));
  }

  @Override
  public Node visitExplainFormat(final SqlBaseParser.ExplainFormatContext context) {
    switch (context.value.getType()) {
      case SqlBaseLexer.GRAPHVIZ:
        return new ExplainFormat(getLocation(context), ExplainFormat.Type.GRAPHVIZ);
      case SqlBaseLexer.TEXT:
        return new ExplainFormat(getLocation(context), ExplainFormat.Type.TEXT);
      default:
        throw new IllegalArgumentException("Unsupported EXPLAIN format: "
                                           + context.value.getText());
    }
  }

  @Override
  public Node visitExplainType(final SqlBaseParser.ExplainTypeContext context) {
    switch (context.value.getType()) {
      case SqlBaseLexer.LOGICAL:
        return new ExplainType(getLocation(context), ExplainType.Type.LOGICAL);
      case SqlBaseLexer.DISTRIBUTED:
        return new ExplainType(getLocation(context), ExplainType.Type.DISTRIBUTED);
      default:
        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }
  }

  // ***************** boolean expressions ******************

  @Override
  public Node visitLogicalNot(final SqlBaseParser.LogicalNotContext context) {
    return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
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

  // *************** from clause *****************

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
    if (context.joinWindow() != null && context.joinWindow().withinExpression() != null) {
      withinExpression = (WithinExpression) visitWithinExpression(
          context.joinWindow().withinExpression());
    }
    final AliasedRelation left = (AliasedRelation) visit(context.left);
    final AliasedRelation right = (AliasedRelation) visit(context.right);
    return new Join(getLocation(context), joinType, left, right, Optional.of(criteria),
                    Optional.ofNullable(withinExpression));
  }

  @Override
  public Node visitAliasedRelation(final SqlBaseParser.AliasedRelationContext context) {
    final Relation child = (Relation) visit(context.relationPrimary());

    final String alias;
    if (context.children.size() == 1) {
      final Table table = (Table) visit(context.relationPrimary());
      alias = table.getName().getSuffix();
    } else if (context.children.size() == 2) {
      alias = context.children.get(1).getText();
    } else {
      throw new IllegalArgumentException(
          "AliasedRelationContext must have either 1 or 2 children, but has:"
          + context.children.size()
      );
    }

    return new AliasedRelation(
        getLocation(context),
        child,
        alias,
        getColumnAliases(context.columnAliases())
    );

  }

  @Override
  public Node visitTableName(final SqlBaseParser.TableNameContext context) {

    final Table table = new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    if (context.tableProperties() != null) {
      table.setProperties(processTableProperties(context.tableProperties()));
    }
    return table;
  }

  @Override
  public Node visitSubqueryRelation(final SqlBaseParser.SubqueryRelationContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitParenthesizedRelation(final SqlBaseParser.ParenthesizedRelationContext context) {
    return visit(context.relation());
  }

  // ********************* predicates *******************

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
    Expression escape = null;
    if (context.escape != null) {
      escape = (Expression) visit(context.escape);
    }

    Expression
        result =
        new LikePredicate(
            getLocation(context),
            (Expression) visit(context.value),
            (Expression) visit(context.pattern),
            escape
        );

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
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
  public Node visitInSubquery(final SqlBaseParser.InSubqueryContext context) {
    Expression result = new InPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        new SubqueryExpression(getLocation(context), (Query) visit(context.query()))
    );

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
  }

  @Override
  public Node visitExists(final SqlBaseParser.ExistsContext context) {
    return new ExistsPredicate(getLocation(context), (Query) visit(context.query()));
  }

  // ************** value expressions **************

  @Override
  public Node visitArithmeticUnary(final SqlBaseParser.ArithmeticUnaryContext context) {
    final Expression child = (Expression) visit(context.valueExpression());

    switch (context.operator.getType()) {
      case SqlBaseLexer.MINUS:
        return ArithmeticUnaryExpression.negative(getLocation(context), child);
      case SqlBaseLexer.PLUS:
        return ArithmeticUnaryExpression.positive(getLocation(context), child);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
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
  public Node visitTimeZoneInterval(final SqlBaseParser.TimeZoneIntervalContext context) {
    return visit(context.interval());
  }

  @Override
  public Node visitTimeZoneString(final SqlBaseParser.TimeZoneStringContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText(), "'"));
  }

  // ********************* primary expressions **********************

  @Override
  public Node visitParenthesizedExpression(
      final SqlBaseParser.ParenthesizedExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitCast(final SqlBaseParser.CastContext context) {
    final boolean isTryCast = context.TRY_CAST() != null;
    return new Cast(
        getLocation(context),
        (Expression) visit(context.expression()),
        getType(context.type()).toString(),
        isTryCast
    );
  }

  @Override
  public Node visitExtract(final SqlBaseParser.ExtractContext context) {
    final String fieldString = getIdentifierText(context.identifier());
    final Extract.Field field;
    try {
      field = Extract.Field.valueOf(fieldString);
    } catch (final IllegalArgumentException e) {
      throw new ParsingException(
          format("Invalid EXTRACT field: %s", fieldString),
          null,
          context.getStart().getLine(),
          context.getStart().getCharPositionInLine()
      );
    }
    return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
  }

  @Override
  public Node visitSubstring(final SqlBaseParser.SubstringContext context) {
    return new FunctionCall(
        getLocation(context),
        QualifiedName.of("SUBSTR"),
        visit(context.valueExpression(), Expression.class)
    );
  }

  @Override
  public Node visitPosition(final SqlBaseParser.PositionContext context) {
    final List<Expression> arguments =
        Lists.reverse(visit(context.valueExpression(), Expression.class));
    return new FunctionCall(getLocation(context), QualifiedName.of("STRPOS"), arguments);
  }

  @Override
  public Node visitNormalize(final SqlBaseParser.NormalizeContext context) {
    final Expression str = (Expression) visit(context.valueExpression());
    final String normalForm =
        Optional.ofNullable(context.normalForm())
            .map(ParserRuleContext::getText)
            .orElse("NFC");
    return new FunctionCall(
        getLocation(context),
        QualifiedName.of("NORMALIZE"),
        ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm))
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
  public Node visitSubqueryExpression(final SqlBaseParser.SubqueryExpressionContext context) {
    return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitDereference(final SqlBaseParser.DereferenceContext context) {
    final String fieldName = getIdentifierText(context.identifier());
    final Expression baseExpression = (Expression) visit(context.base);
    return new DereferenceExpression(getLocation(context), baseExpression, fieldName);
  }

  @Override
  public Node visitColumnReference(final SqlBaseParser.ColumnReferenceContext context) {
    final String columnName = context.identifier(1) == null
        ? getIdentifierText(context.identifier(0))
        : getIdentifierText(context.identifier(1));
    final String prefixName = context.identifier(1) == null
        ? null
        : getIdentifierText(context.identifier(0));
    if (prefixName != null) {
      if (!isValidNameOrAlias(prefixName)) {
        throw new KsqlException(String.format(
            "'%s' is not a valid stream/table name or alias.", prefixName));
      }
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

    // If this is join.
    if (dataSourceExtractor.getJoinLeftSchema() != null) {
      if (dataSourceExtractor.getCommonFieldNames().contains(columnName)) {
        throw new KsqlException("Field " + columnName + " is ambiguous.");
      } else if (dataSourceExtractor.getLeftFieldNames().contains(columnName)) {
        final Expression baseExpression =
            new QualifiedNameReference(
                getLocation(context),
                QualifiedName.of(dataSourceExtractor.getLeftAlias())
            );
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else if (dataSourceExtractor.getRightFieldNames().contains(columnName)) {
        final Expression baseExpression =
            new QualifiedNameReference(
                getLocation(context),
                QualifiedName.of(dataSourceExtractor.getRightAlias())
            );
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else {
        throw new InvalidColumnReferenceException("Field " + columnName + " is ambiguous.");
      }
    }
    final Expression baseExpression =
        new QualifiedNameReference(
            getLocation(context),
            QualifiedName.of(dataSourceExtractor.getFromAlias())
        );
    return new DereferenceExpression(getLocation(context), baseExpression, columnName);
  }

  private boolean isValidNameOrAlias(final String name) {
    // If this is join.
    if (dataSourceExtractor.getJoinLeftSchema() != null) {
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
    final Optional<Window> window = visitIfPresent(context.over(), Window.class);

    final QualifiedName name = getQualifiedName(context.qualifiedName());

    final boolean distinct = false;

    if (name.toString().equals("NULLIF")) {
      check(
          context.expression().size() == 2,
          "Invalid number of arguments for 'nullif' function",
          context
      );
      check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
      check(!distinct, "DISTINCT not valid for 'nullif' function", context);

      return new NullIfExpression(
          getLocation(context),
          (Expression) visit(context.expression(0)),
          (Expression) visit(context.expression(1))
      );
    }

    return new FunctionCall(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        window,
        distinct,
        visit(context.expression(), Expression.class)
    );
  }


  @Override
  public Node visitTableElement(final SqlBaseParser.TableElementContext context) {
    return new TableElement(
        getLocation(context),
        getIdentifierText(context.identifier()),
        getType(context.type())
    );
  }

  // ************** literals **************

  @Override
  public Node visitNullLiteral(final SqlBaseParser.NullLiteralContext context) {
    return new NullLiteral(getLocation(context));
  }

  @Override
  public Node visitStringLiteral(final SqlBaseParser.StringLiteralContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText(), "'"));
  }

  @Override
  public Node visitBinaryLiteral(final SqlBaseParser.BinaryLiteralContext context) {
    final String raw = context.BINARY_LITERAL().getText();
    return new BinaryLiteral(getLocation(context), unquote(raw.substring(1), "'"));
  }

  @Override
  public Node visitTypeConstructor(final SqlBaseParser.TypeConstructorContext context) {
    final String type = getIdentifierText(context.identifier());
    final String value = unquote(context.STRING().getText(), "'");

    if (type.equals("TIME")) {
      return new TimeLiteral(getLocation(context), value);
    }
    if (type.equals("TIMESTAMP")) {
      return new TimestampLiteral(getLocation(context), value);
    }
    if (type.equals("DECIMAL")) {
      return new DecimalLiteral(getLocation(context), value);
    }

    return new GenericLiteral(getLocation(context), type, value);
  }

  @Override
  public Node visitIntegerLiteral(final SqlBaseParser.IntegerLiteralContext context) {
    final Long valueAsLong;
    try {
      valueAsLong = Long.parseLong(context.getText());
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText());
    }
    if (valueAsLong < 0) {
      throw new RuntimeException("Unexpected negative value in literal: " + valueAsLong);
    }
    if (valueAsLong <= Integer.MAX_VALUE) {
      return new IntegerLiteral(getLocation(context), valueAsLong.intValue());
    } else {
      return new LongLiteral(getLocation(context), valueAsLong);
    }
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
  public Node visitInterval(final SqlBaseParser.IntervalContext context) {
    return new IntervalLiteral(
        getLocation(context),
        unquote(context.STRING().getText(), "'"),
        Optional.ofNullable(context.sign)
            .map(AstBuilder::getIntervalSign)
            .orElse(IntervalLiteral.Sign.POSITIVE),
        getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
        Optional.ofNullable(context.to)
            .map((x) -> x.getChild(0).getPayload())
            .map(Token.class::cast)
            .map(AstBuilder::getIntervalFieldType)
    );
  }


  @Override
  public Node visitExplain(final SqlBaseParser.ExplainContext ctx) {
    final SqlBaseParser.QualifiedNameContext qualifiedName = ctx.qualifiedName();
    String queryId = null;
    if (qualifiedName != null) {
      queryId = qualifiedName.getText();
    }

    Statement statement = null;
    if (ctx.statement() != null) {
      statement = (Statement) visit(ctx.statement());
    }

    // Only simple explain is supported for now.
    //TODO: Expand to support other parts of EXPLAIN

    return new Explain(queryId, statement, false, Arrays.asList());
  }

  @Override
  public Node visitDescribeFunction(final SqlBaseParser.DescribeFunctionContext ctx) {
    return new DescribeFunction(getLocation(ctx), ctx.qualifiedName().getText());
  }

  // ***************** helpers *****************

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

  private <T> Optional<T> visitIfPresent(final ParserRuleContext context, final Class<T> clazz) {
    return Optional.ofNullable(context)
        .map(this::visit)
        .map(clazz::cast);
  }

  private <T> List<T> visit(
      final List<? extends ParserRuleContext> contexts,
      final Class<T> clazz) {
    return contexts.stream()
        .map(this::visit)
        .map(clazz::cast)
        .collect(toList());
  }

  public static String getIdentifierText(final SqlBaseParser.IdentifierContext context) {
    if (context instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
      return unquote(context.getText(), "\"");
    } else if (context instanceof SqlBaseParser.BackQuotedIdentifierContext) {
      return unquote(context.getText(), "`");
    } else {
      return context.getText().toUpperCase();
    }
  }

  public static String unquote(final String value, final String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
  }

  private static QualifiedName getQualifiedName(final SqlBaseParser.QualifiedNameContext context) {
    final List<String> parts = context
        .identifier().stream()
        .map(AstBuilder::getIdentifierText)
        .collect(toList());

    return QualifiedName.of(parts);
  }

  private static Optional<String> getTextIfPresent(final Token token) {
    return Optional.ofNullable(token)
        .map(Token::getText);
  }

  private static List<String> getColumnAliases(
      final SqlBaseParser.ColumnAliasesContext columnAliasesContext
  ) {
    if (columnAliasesContext == null) {
      return null;
    }

    return columnAliasesContext
        .identifier().stream()
        .map(AstBuilder::getIdentifierText)
        .collect(toList());
  }

  private static ArithmeticBinaryExpression.Type getArithmeticBinaryOperator(final Token operator) {
    switch (operator.getType()) {
      case SqlBaseLexer.PLUS:
        return ArithmeticBinaryExpression.Type.ADD;
      case SqlBaseLexer.MINUS:
        return ArithmeticBinaryExpression.Type.SUBTRACT;
      case SqlBaseLexer.ASTERISK:
        return ArithmeticBinaryExpression.Type.MULTIPLY;
      case SqlBaseLexer.SLASH:
        return ArithmeticBinaryExpression.Type.DIVIDE;
      case SqlBaseLexer.PERCENT:
        return ArithmeticBinaryExpression.Type.MODULUS;
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

  private static IntervalLiteral.IntervalField getIntervalFieldType(final Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.YEAR:
        return IntervalLiteral.IntervalField.YEAR;
      case SqlBaseLexer.MONTH:
        return IntervalLiteral.IntervalField.MONTH;
      case SqlBaseLexer.DAY:
        return IntervalLiteral.IntervalField.DAY;
      case SqlBaseLexer.HOUR:
        return IntervalLiteral.IntervalField.HOUR;
      case SqlBaseLexer.MINUTE:
        return IntervalLiteral.IntervalField.MINUTE;
      case SqlBaseLexer.SECOND:
        return IntervalLiteral.IntervalField.SECOND;
      default:
        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }
  }

  private static IntervalLiteral.Sign getIntervalSign(final Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.MINUS:
        return IntervalLiteral.Sign.NEGATIVE;
      case SqlBaseLexer.PLUS:
        return IntervalLiteral.Sign.POSITIVE;
      default:
        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
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

  private static Type getType(final SqlBaseParser.TypeContext type) {
    if (type.baseType() != null) {
      return PrimitiveType.getPrimitiveType(baseTypeToString(type.baseType()));
    }

    if (type.ARRAY() != null) {
      return new Array(getType(type.type(0)));
    }

    if (type.MAP() != null) {
      return new io.confluent.ksql.parser.tree.Map(getType(type.type(1)));
    }

    if (type.STRUCT() != null) {
      final List<Pair<String, Type>> structItems = new ArrayList<>();
      for (int i = 0; i < type.identifier().size(); i++) {
        final String itemName = getIdentifierText(type.identifier(i));
        final Type itemType = getType(type.type(i));
        structItems.add(new Pair<>(itemName, itemType));
      }
      return new Struct(structItems);
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private static String baseTypeToString(final SqlBaseParser.BaseTypeContext baseType) {
    if (baseType.identifier() != null) {
      return getIdentifierText(baseType.identifier());
    } else {
      throw new KsqlException(
          "Base type must contain either identifier, "
          + "time with time zone, or timestamp with time zone"
      );
    }
  }

  private static void check(
      final boolean condition,
      final String message,
      final ParserRuleContext context) {
    if (!condition) {
      throw new ParsingException(
          message,
          null,
          context.getStart().getLine(),
          context.getStart().getCharPositionInLine()
      );
    }
  }

  private static NodeLocation getLocation(final TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  private static NodeLocation getLocation(final ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private static NodeLocation getLocation(final Token token) {
    requireNonNull(token, "token is null");
    return new NodeLocation(token.getLine(), token.getCharPositionInLine());
  }

  private StructuredDataSource getResultDatasource(final Select select, final Table into) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(into.toString());
    for (final SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        final SingleColumn singleColumn = (SingleColumn) selectItem;
        final String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
      }
    }

    final KsqlTopic ksqlTopic =
        new KsqlTopic(into.getName().toString(), into.getName().toString(), null);

    final StructuredDataSource resultStream = new KsqlStream(
        "AstBuilder-Into",
        into.getName().toString(),
        dataSource.schema(),
        dataSource.fields().get(0),
        null,
        ksqlTopic
    );
    return resultStream;
  }

  private static class InvalidColumnReferenceException extends KsqlException {

    InvalidColumnReferenceException(final String message) {
      super(message);
    }

    InvalidColumnReferenceException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

}
