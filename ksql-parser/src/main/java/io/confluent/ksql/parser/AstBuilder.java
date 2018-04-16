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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
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
import io.confluent.ksql.parser.tree.DereferenceExpression;
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
import io.confluent.ksql.parser.tree.IntervalLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.JoinUsing;
import io.confluent.ksql.parser.tree.LambdaExpression;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListRegisteredTopics;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NaturalJoin;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QueryBody;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Row;
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
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.parser.tree.WithQuery;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KsqlException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AstBuilder extends SqlBaseBaseVisitor<Node> {

  private int selectItemIndex = 0;

  private static final String DEFAULT_WINDOW_NAME = "StreamWindow";

  private DataSourceExtractor dataSourceExtractor;

  public AstBuilder(DataSourceExtractor dataSourceExtractor) {
    this.dataSourceExtractor = dataSourceExtractor;
  }

  @Override
  public Node visitStatements(SqlBaseParser.StatementsContext context) {
    List<Statement> statementList = new ArrayList<>();
    for (SqlBaseParser.SingleStatementContext singleStatementContext : context.singleStatement()) {
      Statement statement = (Statement) visitSingleStatement(singleStatementContext);
      statementList.add(statement);
    }
    return new Statements(statementList);
  }

  @Override
  public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
    return (Statement) visit(context.statement());
  }

  @Override
  public Node visitQuerystatement(SqlBaseParser.QuerystatementContext ctx) {
    return (Statement) visitChildren(ctx);
  }

  @Override
  public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context) {
    return visit(context.expression());
  }

  // ******************* statements **********************


  private Map<String, Expression> processTableProperties(
      TablePropertiesContext tablePropertiesContext
  ) {
    ImmutableMap.Builder<String, Expression> properties = ImmutableMap.builder();
    if (tablePropertiesContext != null) {
      for (TablePropertyContext tablePropertyContext : tablePropertiesContext.tableProperty()) {
        properties.put(
            getIdentifierText(tablePropertyContext.identifier()),
            (Expression) visit(tablePropertyContext.expression())
        );
      }
    }
    return properties.build();
  }

  @Override
  public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
    return new CreateTable(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        visit(context.tableElement(), TableElement.class),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitRegisterTopic(SqlBaseParser.RegisterTopicContext context) {
    return new RegisterTopic(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitCreateStream(SqlBaseParser.CreateStreamContext context) {
    return new CreateStream(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        visit(context.tableElement(), TableElement.class),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitCreateStreamAs(SqlBaseParser.CreateStreamAsContext context) {
    Optional<Expression> partitionByColumn = Optional.empty();
    if (context.identifier() != null) {
      partitionByColumn = Optional.of(new QualifiedNameReference(
          QualifiedName.of(getIdentifierText(context.identifier()))));
    }

    return new CreateStreamAsSelect(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        (Query) visitQuery(context.query()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties()),
        partitionByColumn
    );
  }

  @Override
  public Node visitCreateTableAs(SqlBaseParser.CreateTableAsContext context) {
    return new CreateTableAsSelect(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        (Query) visitQuery(context.query()),
        context.EXISTS() != null,
        processTableProperties(context.tableProperties())
    );
  }

  @Override
  public Node visitDropTopic(SqlBaseParser.DropTopicContext context) {
    return new DropTopic(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null
    );
  }

  @Override
  public Node visitDropTable(SqlBaseParser.DropTableContext context) {
    return new DropTable(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null
    );
  }

  @Override
  public Node visitDropStream(SqlBaseParser.DropStreamContext context) {
    return new DropStream(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        context.EXISTS() != null
    );
  }

  // ********************** query expressions ********************

  @Override
  public Node visitQuery(SqlBaseParser.QueryContext context) {
    Query body = (Query) visit(context.queryNoWith());

    return new Query(
        getLocation(context),
        body.getQueryBody(),
        body.getLimit()
    );
  }


  @Override
  public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context) {
    return new WithQuery(
        getLocation(context),
        context.name.getText(),
        (Query) visit(context.query()),
        Optional.ofNullable(getColumnAliases(context.columnAliases()))
    );
  }

  @Override
  public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context) {

    QueryBody term = (QueryBody) visit(context.queryTerm());

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by limit, fold the order by and limit
      // clauses into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      QuerySpecification query = (QuerySpecification) term;
      return new Query(
          getLocation(context),
          new QuerySpecification(
              getLocation(context),
              query.getSelect(),
              query.getInto(),
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
  public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
    Table into;
    if (context.into != null) {
      into = (Table) visit(context.into);
    } else {
      // TODO: Generate a unique name
      String intoName = "KSQL_Stream_" + System.currentTimeMillis();
      into = new Table(QualifiedName.of(intoName), true);
    }

    Relation from = (Relation) visit(context.from);

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
        from,
        visitIfPresent(context.windowExpression(), WindowExpression.class),
        visitIfPresent(context.where, Expression.class),
        visitIfPresent(context.groupBy(), GroupBy.class),
        visitIfPresent(context.having, Expression.class),
        Optional.<String>empty()
    );
  }

  private List<SelectItem> extractSelectItems(Select select, Relation from) {
    List<SelectItem> selectItems = new ArrayList<>();
    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof AllColumns) {
        selectItems.addAll(getSelectStartItems(selectItem, from));

      } else if (selectItem instanceof SingleColumn) {
        selectItems.add((SingleColumn) selectItem);
      } else {
        throw new IllegalArgumentException(
            "Unsupported SelectItem type: " + selectItem.getClass().getName());
      }
    }
    return selectItems;
  }

  private List<SelectItem> getSelectStartItems(final SelectItem selectItem, final Relation from) {
    List<SelectItem> selectItems = new ArrayList<>();
    AllColumns allColumns = (AllColumns) selectItem;

    if (from instanceof Join) {
      Join join = (Join) from;
      AliasedRelation left = (AliasedRelation) join.getLeft();
      StructuredDataSource
          leftDataSource =
          dataSourceExtractor.getMetaStore().getSource(left.getRelation().toString());
      if (leftDataSource == null) {
        throw new InvalidColumnReferenceException(left.getRelation().toString()
                                                  + " does not exist.");
      }
      AliasedRelation right = (AliasedRelation) join.getRight();
      StructuredDataSource rightDataSource =
          dataSourceExtractor.getMetaStore().getSource(right.getRelation().toString());
      if (rightDataSource == null) {
        throw new InvalidColumnReferenceException(right.getRelation().toString()
                                                  + " does not exist.");
      }
      for (Field field : leftDataSource.getSchema().fields()) {
        QualifiedNameReference qualifiedNameReference =
            new QualifiedNameReference(
                allColumns.getLocation().get(),
                QualifiedName.of(left.getAlias() + "." + field.name())
            );
        SingleColumn newSelectItem =
            new SingleColumn(
                qualifiedNameReference,
                left.getAlias() + "_" + field.name()
            );
        selectItems.add(newSelectItem);
      }
      for (Field field : rightDataSource.getSchema().fields()) {
        QualifiedNameReference qualifiedNameReference =
            new QualifiedNameReference(
                allColumns.getLocation().get(),
                QualifiedName.of(right.getAlias() + "." + field.name())
            );
        SingleColumn newSelectItem =
            new SingleColumn(
                qualifiedNameReference,
                right.getAlias() + "_" + field.name()
            );
        selectItems.add(newSelectItem);
      }
    } else {
      AliasedRelation fromRel = (AliasedRelation) from;
      StructuredDataSource fromDataSource =
          dataSourceExtractor.getMetaStore()
              .getSource(((Table) fromRel.getRelation()).getName().getSuffix());
      if (fromDataSource == null) {
        throw new InvalidColumnReferenceException(
            ((Table) fromRel.getRelation()).getName().getSuffix() + " does not exist."
        );
      }
      for (Field field : fromDataSource.getSchema().fields()) {
        QualifiedNameReference qualifiedNameReference =
            new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                .of(fromDataSource.getName() + "." + field.name()));
        SingleColumn newSelectItem =
            new SingleColumn(qualifiedNameReference, field.name());
        selectItems.add(newSelectItem);
      }
    }
    return selectItems;
  }

  @Override
  public Node visitWindowExpression(SqlBaseParser.WindowExpressionContext ctx) {
    String windowName = DEFAULT_WINDOW_NAME;
    if (ctx.IDENTIFIER() != null) {
      windowName = ctx.IDENTIFIER().getText();
    }
    windowName = windowName.toUpperCase();
    if (ctx.tumblingWindowExpression() != null) {
      TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression)
          visitTumblingWindowExpression(ctx.tumblingWindowExpression());
      return new WindowExpression(windowName, tumblingWindowExpression);
    } else if (ctx.hoppingWindowExpression() != null) {
      HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression)
          visitHoppingWindowExpression(ctx.hoppingWindowExpression());

      return new WindowExpression(windowName, hoppingWindowExpression);
    } else if (ctx.sessionWindowExpression() != null) {
      SessionWindowExpression sessionWindowExpression = (SessionWindowExpression)
          visitSessionWindowExpression(ctx.sessionWindowExpression());
      return new WindowExpression(windowName, sessionWindowExpression);
    }
    throw new KsqlException("Window description is not correct.");
  }

  @Override
  public Node visitHoppingWindowExpression(SqlBaseParser.HoppingWindowExpressionContext ctx) {

    List<SqlBaseParser.NumberContext> numberList = ctx.number();
    List<SqlBaseParser.WindowUnitContext> windowUnits = ctx.windowUnit();
    String sizeStr = numberList.get(0).getText();
    String advanceByStr = numberList.get(1).getText();

    String sizeUnit = windowUnits.get(0).getText();
    String advanceByUnit = windowUnits.get(1).getText();
    return new HoppingWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase()),
        Long.parseLong(advanceByStr),
        WindowExpression.getWindowUnit(advanceByUnit.toUpperCase())
    );
  }

  @Override
  public Node visitTumblingWindowExpression(SqlBaseParser.TumblingWindowExpressionContext ctx) {
    String sizeStr = ctx.number().getText();
    String sizeUnit = ctx.windowUnit().getText();
    return new TumblingWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
    );
  }

  @Override
  public Node visitSessionWindowExpression(SqlBaseParser.SessionWindowExpressionContext ctx) {
    String sizeStr = ctx.number().getText();
    String sizeUnit = ctx.windowUnit().getText();
    return new SessionWindowExpression(
        Long.parseLong(sizeStr),
        WindowExpression.getWindowUnit(sizeUnit.toUpperCase())
    );
  }

  @Override
  public Node visitGroupBy(SqlBaseParser.GroupByContext context) {
    return new GroupBy(
        getLocation(context),
        false,
        visit(context.groupingElement(), GroupingElement.class)
    );
  }

  @Override
  public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context) {
    return new SimpleGroupBy(
        getLocation(context),
        visit(context.groupingExpressions().expression(), Expression.class)
    );
  }

  @Override
  public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
    if (context.qualifiedName() != null) {
      return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    return new AllColumns(getLocation(context));
  }

  @Override
  public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
    Expression selectItemExpression = (Expression) visit(context.expression());
    Optional<String> alias = Optional
        .ofNullable(context.identifier())
        .map(AstBuilder::getIdentifierText);
    if (!alias.isPresent()) {
      if (selectItemExpression instanceof QualifiedNameReference) {
        QualifiedNameReference
            qualifiedNameReference =
            (QualifiedNameReference) selectItemExpression;
        alias = Optional.of(qualifiedNameReference.getName().getSuffix());
      } else if (selectItemExpression instanceof DereferenceExpression) {
        DereferenceExpression dereferenceExpression = (DereferenceExpression) selectItemExpression;
        if ((dataSourceExtractor.getJoinLeftSchema() != null) && (
            dataSourceExtractor
                .getCommonFieldNames()
                .contains(
                    dereferenceExpression.getFieldName()
                )
          )) {
          alias = Optional.of(
              dereferenceExpression.getBase().toString()
              + "_" + dereferenceExpression.getFieldName()
          );
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

  @Override
  public Node visitQualifiedName(SqlBaseParser.QualifiedNameContext context) {
    return visitChildren(context);
  }

  @Override
  public Node visitTable(SqlBaseParser.TableContext context) {
    return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
  }

  @Override
  public Node visitExportCatalog(SqlBaseParser.ExportCatalogContext context) {
    return new ExportCatalog(Optional.ofNullable(getLocation(context)), context.STRING().getText());
  }

  @Override
  public Node visitRunScript(SqlBaseParser.RunScriptContext context) {
    return new RunScript(Optional.ofNullable(getLocation(context)), context.STRING().getText());
  }

  @Override
  public Node visitListRegisteredTopics(SqlBaseParser.ListRegisteredTopicsContext context) {
    return new ListRegisteredTopics(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitListTopics(SqlBaseParser.ListTopicsContext context) {
    return new ListTopics(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitListStreams(SqlBaseParser.ListStreamsContext context) {
    return new ListStreams(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitListTables(SqlBaseParser.ListTablesContext context) {
    return new ListTables(Optional.ofNullable(getLocation(context)));
  }


  @Override
  public Node visitListQueries(SqlBaseParser.ListQueriesContext context) {
    return new ListQueries(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitTerminateQuery(SqlBaseParser.TerminateQueryContext context) {
    return new TerminateQuery(getLocation(context), context.qualifiedName().getText());
  }

  @Override
  public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
    return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()),
                           context.TOPIC() != null, context.EXTENDED() != null
    );
  }

  @Override
  public Node visitListProperties(SqlBaseParser.ListPropertiesContext context) {
    return new ListProperties(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitSetProperty(SqlBaseParser.SetPropertyContext context) {
    String propertyName = unquote(context.STRING(0).getText(), "'");
    String propertyValue = unquote(context.STRING(1).getText(), "'");
    return new SetProperty(Optional.ofNullable(getLocation(context)), propertyName, propertyValue);
  }

  @Override
  public Node visitUnsetProperty(SqlBaseParser.UnsetPropertyContext context) {
    String propertyName = unquote(context.STRING().getText(), "'");
    return new UnsetProperty(Optional.ofNullable(getLocation(context)), propertyName);
  }

  @Override
  public Node visitPrintTopic(SqlBaseParser.PrintTopicContext context) {
    boolean fromBeginning = context.FROM() != null;

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
          null
      );
    } else if (context.number() instanceof SqlBaseParser.IntegerLiteralContext) {
      SqlBaseParser.IntegerLiteralContext integerLiteralContext =
          (SqlBaseParser.IntegerLiteralContext) context.number();
      return new PrintTopic(
          getLocation(context),
          topicName,
          fromBeginning,
          (LongLiteral) visitIntegerLiteral(integerLiteralContext)
      );
    } else {
      throw new KsqlException("Interval value should be integer in 'PRINT' command!");
    }

  }

  @Override
  public Node visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Node visitSubquery(SqlBaseParser.SubqueryContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
  }

  @Override
  public Node visitInlineTable(SqlBaseParser.InlineTableContext context) {
    return new Values(getLocation(context), visit(context.expression(), Expression.class));
  }

  @Override
  public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context) {
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
  public Node visitExplainType(SqlBaseParser.ExplainTypeContext context) {
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
  public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
    return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
  }

  @Override
  public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
    return new LogicalBinaryExpression(
        getLocation(context.operator),
        getLogicalBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right)
    );
  }

  // *************** from clause *****************

  @Override
  public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
    Relation left = (Relation) visit(context.left);
    Relation right;

    if (context.CROSS() != null) {
      right = (Relation) visit(context.right);
      return new Join(
          getLocation(context),
          Join.Type.CROSS,
          left,
          right,
          Optional.<JoinCriteria>empty()
      );
    }

    JoinCriteria criteria;
    if (context.NATURAL() != null) {
      right = (Relation) visit(context.right);
      criteria = new NaturalJoin();
    } else {
      right = (Relation) visit(context.rightRelation);
      if (context.joinCriteria().ON() != null) {
        criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
      } else if (context.joinCriteria().USING() != null) {
        List<String> columns = context.joinCriteria()
            .identifier().stream()
            .map(AstBuilder::getIdentifierText)
            .collect(toList());

        criteria = new JoinUsing(columns);
      } else {
        throw new IllegalArgumentException("Unsupported join criteria");
      }
    }

    Join.Type joinType;
    if (context.joinType().LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (context.joinType().RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (context.joinType().FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }

    return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
  }

  @Override
  public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
    Relation child = (Relation) visit(context.relationPrimary());

    String alias;
    if (context.children.size() == 1) {
      Table table = (Table) visit(context.relationPrimary());
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
  public Node visitTableName(SqlBaseParser.TableNameContext context) {

    Table table = new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    if (context.tableProperties() != null) {
      table.setProperties(processTableProperties(context.tableProperties()));
    }
    return table;
  }

  @Override
  public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
    return visit(context.relation());
  }

  // ********************* predicates *******************

  @Override
  public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
    if (context.predicate() != null) {
      return visit(context.predicate());
    }

    return visit(context.valueExpression);
  }

  @Override
  public Node visitComparison(SqlBaseParser.ComparisonContext context) {
    return new ComparisonExpression(
        getLocation(context.comparisonOperator()),
        getComparisonOperator(
            ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.right)
    );
  }

  @Override
  public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
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
  public Node visitBetween(SqlBaseParser.BetweenContext context) {
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
  public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
    Expression child = (Expression) visit(context.value);

    if (context.NOT() == null) {
      return new IsNullPredicate(getLocation(context), child);
    }

    return new IsNotNullPredicate(getLocation(context), child);
  }

  @Override
  public Node visitLike(SqlBaseParser.LikeContext context) {
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
  public Node visitInList(SqlBaseParser.InListContext context) {
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
  public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
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
  public Node visitExists(SqlBaseParser.ExistsContext context) {
    return new ExistsPredicate(getLocation(context), (Query) visit(context.query()));
  }

  // ************** value expressions **************

  @Override
  public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
    Expression child = (Expression) visit(context.valueExpression());

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
  public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
    return new ArithmeticBinaryExpression(
        getLocation(context.operator),
        getArithmeticBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right)
    );
  }

  @Override
  public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
    return new FunctionCall(
        getLocation(context.CONCAT()),
        QualifiedName.of("concat"), ImmutableList.of(
        (Expression) visit(context.left),
        (Expression) visit(context.right)
    )
    );
  }

  @Override
  public Node visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext context) {
    return visit(context.interval());
  }

  @Override
  public Node visitTimeZoneString(SqlBaseParser.TimeZoneStringContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText(), "'"));
  }

  // ********************* primary expressions **********************

  @Override
  public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context) {
    return new Row(getLocation(context), visit(context.expression(), Expression.class));
  }


  @Override
  public Node visitCast(SqlBaseParser.CastContext context) {
    boolean isTryCast = context.TRY_CAST() != null;
    return new Cast(
        getLocation(context),
        (Expression) visit(context.expression()),
        getType(context.type()),
        isTryCast
    );
  }

  @Override
  public Node visitExtract(SqlBaseParser.ExtractContext context) {
    String fieldString = getIdentifierText(context.identifier());
    Extract.Field field;
    try {
      field = Extract.Field.valueOf(fieldString);
    } catch (IllegalArgumentException e) {
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
  public Node visitSubstring(SqlBaseParser.SubstringContext context) {
    return new FunctionCall(
        getLocation(context),
        QualifiedName.of("SUBSTR"),
        visit(context.valueExpression(), Expression.class)
    );
  }

  @Override
  public Node visitPosition(SqlBaseParser.PositionContext context) {
    List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
    return new FunctionCall(getLocation(context), QualifiedName.of("STRPOS"), arguments);
  }

  @Override
  public Node visitNormalize(SqlBaseParser.NormalizeContext context) {
    Expression str = (Expression) visit(context.valueExpression());
    String normalForm =
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
  public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
    return new SubscriptExpression(
        getLocation(context),
        (Expression) visit(context.value),
        (Expression) visit(context.index)
    );
  }

  @Override
  public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
    return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitDereference(SqlBaseParser.DereferenceContext context) {
    String fieldName = getIdentifierText(context.identifier());
    Expression baseExpression;
    QualifiedName tableName = QualifiedName.of(
        context.primaryExpression().getText().toUpperCase());
    baseExpression = new QualifiedNameReference(
        getLocation(context.primaryExpression()), tableName);
    DereferenceExpression dereferenceExpression =
        new DereferenceExpression(getLocation(context), baseExpression, fieldName);
    return dereferenceExpression;
  }

  @Override
  public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
    String columnName = getIdentifierText(context.identifier());
    // If this is join.
    if (dataSourceExtractor.getJoinLeftSchema() != null) {
      if (dataSourceExtractor.getCommonFieldNames().contains(columnName)) {
        throw new KsqlException("Field " + columnName + " is ambiguous.");
      } else if (dataSourceExtractor.getLeftFieldNames().contains(columnName)) {
        Expression baseExpression =
            new QualifiedNameReference(
                getLocation(context),
                QualifiedName.of(dataSourceExtractor.getLeftAlias())
            );
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else if (dataSourceExtractor.getRightFieldNames().contains(columnName)) {
        Expression baseExpression =
            new QualifiedNameReference(
                getLocation(context),
                QualifiedName.of(dataSourceExtractor.getRightAlias())
            );
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else {
        throw new InvalidColumnReferenceException("Field " + columnName + " is ambiguous.");
      }
    } else {
      Expression baseExpression =
          new QualifiedNameReference(
              getLocation(context),
              QualifiedName.of(dataSourceExtractor.getFromAlias())
          );
      return new DereferenceExpression(getLocation(context), baseExpression, columnName);
    }
  }

  @Override
  public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
    return new SimpleCaseExpression(
        getLocation(context),
        (Expression) visit(context.valueExpression()),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class)
    );
  }

  @Override
  public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
    return new SearchedCaseExpression(
        getLocation(context),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class)
    );
  }

  @Override
  public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
    return new WhenClause(
        getLocation(context),
        (Expression) visit(context.condition),
        (Expression) visit(context.result)
    );
  }

  @Override
  public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
    Optional<Window> window = visitIfPresent(context.over(), Window.class);

    QualifiedName name = getQualifiedName(context.qualifiedName());

    boolean distinct = false;

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
  public Node visitLambda(SqlBaseParser.LambdaContext context) {
    List<String> arguments = context.identifier().stream()
        .map(AstBuilder::getIdentifierText)
        .collect(toList());

    Expression body = (Expression) visit(context.expression());

    return new LambdaExpression(arguments, body);
  }


  @Override
  public Node visitTableElement(SqlBaseParser.TableElementContext context) {
    return new TableElement(
        getLocation(context),
        getIdentifierText(context.identifier()),
        getType(context.type())
    );
  }

  // ************** literals **************

  @Override
  public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
    return new NullLiteral(getLocation(context));
  }

  @Override
  public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText(), "'"));
  }

  @Override
  public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context) {
    String raw = context.BINARY_LITERAL().getText();
    return new BinaryLiteral(getLocation(context), unquote(raw.substring(1), "'"));
  }

  @Override
  public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context) {
    String type = getIdentifierText(context.identifier());
    String value = unquote(context.STRING().getText(), "'");

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
  public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
    return new LongLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
    return new DoubleLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context) {
    return new BooleanLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitInterval(SqlBaseParser.IntervalContext context) {
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
  public Node visitExplain(SqlBaseParser.ExplainContext ctx) {
    SqlBaseParser.QualifiedNameContext qualifiedName = ctx.qualifiedName();
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

  // ***************** helpers *****************

  @Override
  protected Node defaultResult() {
    return null;
  }

  @Override
  protected Node aggregateResult(Node aggregate, Node nextResult) {
    if (nextResult == null) {
      throw new UnsupportedOperationException("not yet implemented");
    }

    if (aggregate == null) {
      return nextResult;
    }

    throw new UnsupportedOperationException("not yet implemented");
  }

  private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
    return Optional.ofNullable(context)
        .map(this::visit)
        .map(clazz::cast);
  }

  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream()
        .map(this::visit)
        .map(clazz::cast)
        .collect(toList());
  }

  public static String getIdentifierText(SqlBaseParser.IdentifierContext context) {
    if (context instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
      return unquote(context.getText(), "\"");
    } else if (context instanceof SqlBaseParser.BackQuotedIdentifierContext) {
      return unquote(context.getText(), "`");
    } else {
      return context.getText().toUpperCase();
    }
  }

  public static String unquote(String value, String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
  }

  private static QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context) {
    List<String> parts = context
        .identifier().stream()
        .map(AstBuilder::getIdentifierText)
        .collect(toList());

    return QualifiedName.of(parts);
  }

  private static Optional<String> getTextIfPresent(Token token) {
    return Optional.ofNullable(token)
        .map(Token::getText);
  }

  private static List<String> getColumnAliases(
      SqlBaseParser.ColumnAliasesContext columnAliasesContext
  ) {
    if (columnAliasesContext == null) {
      return null;
    }

    return columnAliasesContext
        .identifier().stream()
        .map(AstBuilder::getIdentifierText)
        .collect(toList());
  }

  private static ArithmeticBinaryExpression.Type getArithmeticBinaryOperator(Token operator) {
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

  private static ComparisonExpression.Type getComparisonOperator(Token symbol) {
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

  private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
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

  private static IntervalLiteral.Sign getIntervalSign(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.MINUS:
        return IntervalLiteral.Sign.NEGATIVE;
      case SqlBaseLexer.PLUS:
        return IntervalLiteral.Sign.POSITIVE;
      default:
        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }
  }

  private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.AND:
        return LogicalBinaryExpression.Type.AND;
      case SqlBaseLexer.OR:
        return LogicalBinaryExpression.Type.OR;
      default:
        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }
  }

  private static String getType(SqlBaseParser.TypeContext type) {
    if (type.baseType() != null) {
      String signature = baseTypeToString(type.baseType());
      if (!type.typeParameter().isEmpty()) {
        String typeParameterSignature = type
            .typeParameter()
            .stream()
            .map(AstBuilder::typeParameterToString)
            .collect(Collectors.joining(","));
        signature += "(" + typeParameterSignature + ")";
      }
      return signature;
    }

    if (type.ARRAY() != null) {
      return "ARRAY(" + getType(type.type(0)) + ")";
    }

    if (type.MAP() != null) {
      return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
    }

    if (type.ROW() != null) {
      StringBuilder builder = new StringBuilder("(");
      for (int i = 0; i < type.identifier().size(); i++) {
        if (i != 0) {
          builder.append(",");
        }
        builder.append(getIdentifierText(type.identifier(i)))
            .append(" ")
            .append(getType(type.type(i)));
      }
      builder.append(")");
      return "ROW" + builder.toString();
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private static String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter) {
    if (typeParameter.INTEGER_VALUE() != null) {
      return typeParameter.INTEGER_VALUE().toString();
    }
    if (typeParameter.type() != null) {
      return getType(typeParameter.type());
    }
    throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
  }

  private static String baseTypeToString(SqlBaseParser.BaseTypeContext baseType) {
    if (baseType.identifier() != null) {
      return getIdentifierText(baseType.identifier());
    } else {
      throw new KsqlException(
          "Base type must contain either identifier, "
          + "time with time zone, or timestamp with time zone"
      );
    }
  }

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw new ParsingException(
          message,
          null,
          context.getStart().getLine(),
          context.getStart().getCharPositionInLine()
      );
    }
  }

  private static NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  private static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private static NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return new NodeLocation(token.getLine(), token.getCharPositionInLine());
  }

  private StructuredDataSource getResultDatasource(Select select, Table into) {

    SchemaBuilder dataSource = SchemaBuilder.struct().name(into.toString());
    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        String fieldName = singleColumn.getAlias().get();
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }
    }

    KsqlTopic ksqlTopic = new KsqlTopic(into.getName().toString(), into.getName().toString(), null);
    StructuredDataSource resultStream =
        new KsqlStream(
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

    public InvalidColumnReferenceException(String message) {
      super(message);
    }

    public InvalidColumnReferenceException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
