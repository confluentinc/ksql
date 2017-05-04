/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.kql.parser.SqlBaseParser.TablePropertyContext;
import io.confluent.kql.parser.tree.AliasedRelation;
import io.confluent.kql.parser.tree.AllColumns;
import io.confluent.kql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.kql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.kql.parser.tree.BetweenPredicate;
import io.confluent.kql.parser.tree.BinaryLiteral;
import io.confluent.kql.parser.tree.BooleanLiteral;
import io.confluent.kql.parser.tree.Cast;
import io.confluent.kql.parser.tree.ComparisonExpression;
import io.confluent.kql.parser.tree.CreateStream;
import io.confluent.kql.parser.tree.CreateStreamAsSelect;
import io.confluent.kql.parser.tree.CreateTable;
import io.confluent.kql.parser.tree.CreateTableAsSelect;
import io.confluent.kql.parser.tree.CreateTopic;
import io.confluent.kql.parser.tree.DecimalLiteral;
import io.confluent.kql.parser.tree.DereferenceExpression;
import io.confluent.kql.parser.tree.DoubleLiteral;
import io.confluent.kql.parser.tree.DropTable;
import io.confluent.kql.parser.tree.Except;
import io.confluent.kql.parser.tree.ExistsPredicate;
import io.confluent.kql.parser.tree.ExplainFormat;
import io.confluent.kql.parser.tree.ExplainType;
import io.confluent.kql.parser.tree.ExportCatalog;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.Extract;
import io.confluent.kql.parser.tree.FrameBound;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.GenericLiteral;
import io.confluent.kql.parser.tree.GroupBy;
import io.confluent.kql.parser.tree.GroupingElement;
import io.confluent.kql.parser.tree.GroupingSets;
import io.confluent.kql.parser.tree.HoppingWindowExpression;
import io.confluent.kql.parser.tree.InListExpression;
import io.confluent.kql.parser.tree.InPredicate;
import io.confluent.kql.parser.tree.Intersect;
import io.confluent.kql.parser.tree.IntervalLiteral;
import io.confluent.kql.parser.tree.IsNotNullPredicate;
import io.confluent.kql.parser.tree.IsNullPredicate;
import io.confluent.kql.parser.tree.Join;
import io.confluent.kql.parser.tree.JoinCriteria;
import io.confluent.kql.parser.tree.JoinOn;
import io.confluent.kql.parser.tree.JoinUsing;
import io.confluent.kql.parser.tree.LambdaExpression;
import io.confluent.kql.parser.tree.LikePredicate;
import io.confluent.kql.parser.tree.ListProperties;
import io.confluent.kql.parser.tree.ListQueries;
import io.confluent.kql.parser.tree.ListStreams;
import io.confluent.kql.parser.tree.ListTables;
import io.confluent.kql.parser.tree.ListTopics;
import io.confluent.kql.parser.tree.LogicalBinaryExpression;
import io.confluent.kql.parser.tree.LongLiteral;
import io.confluent.kql.parser.tree.NaturalJoin;
import io.confluent.kql.parser.tree.Node;
import io.confluent.kql.parser.tree.NodeLocation;
import io.confluent.kql.parser.tree.NotExpression;
import io.confluent.kql.parser.tree.NullIfExpression;
import io.confluent.kql.parser.tree.NullLiteral;
import io.confluent.kql.parser.tree.PrintTopic;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.QualifiedNameReference;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QueryBody;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Relation;
import io.confluent.kql.parser.tree.Row;
import io.confluent.kql.parser.tree.SampledRelation;
import io.confluent.kql.parser.tree.SearchedCaseExpression;
import io.confluent.kql.parser.tree.Select;
import io.confluent.kql.parser.tree.SelectItem;
import io.confluent.kql.parser.tree.SetProperty;
import io.confluent.kql.parser.tree.ShowColumns;
import io.confluent.kql.parser.tree.SimpleCaseExpression;
import io.confluent.kql.parser.tree.SimpleGroupBy;
import io.confluent.kql.parser.tree.SingleColumn;
import io.confluent.kql.parser.tree.SortItem;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.parser.tree.Statements;
import io.confluent.kql.parser.tree.StringLiteral;
import io.confluent.kql.parser.tree.SubqueryExpression;
import io.confluent.kql.parser.tree.SubscriptExpression;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.TableElement;
import io.confluent.kql.parser.tree.TableSubquery;
import io.confluent.kql.parser.tree.TerminateQuery;
import io.confluent.kql.parser.tree.TimeLiteral;
import io.confluent.kql.parser.tree.TimestampLiteral;
import io.confluent.kql.parser.tree.TumblingWindowExpression;
import io.confluent.kql.parser.tree.Union;
import io.confluent.kql.parser.tree.Values;
import io.confluent.kql.parser.tree.WhenClause;
import io.confluent.kql.parser.tree.Window;
import io.confluent.kql.parser.tree.WindowExpression;
import io.confluent.kql.parser.tree.WindowFrame;
import io.confluent.kql.parser.tree.With;
import io.confluent.kql.parser.tree.WithQuery;
import io.confluent.kql.util.DataSourceExtractor;
import io.confluent.kql.util.KQLException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AstBuilder
    extends SqlBaseBaseVisitor<Node> {

  int selectItemIndex = 0;

  public static final String DEFAULT_WINDOW_NAME = "StreamWindow";

  DataSourceExtractor dataSourceExtractor;
  public StructuredDataSource resultDataSource = null;

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
      TablePropertiesContext tablePropertiesContext) {
    ImmutableMap.Builder<String, Expression> properties = ImmutableMap.builder();
    if (tablePropertiesContext != null) {
      for (TablePropertyContext tablePropertyContext : tablePropertiesContext.tableProperty()) {
        properties.put(getIdentifierText(tablePropertyContext.identifier()),
                       (Expression) visit(tablePropertyContext.expression()));
      }
    }
    return properties.build();
  }

  @Override
  public Node visitIsolationLevel(SqlBaseParser.IsolationLevelContext context) {
    return visit(context.levelOfIsolation());
  }

  @Override
  public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
    return new CreateTable(getLocation(context), getQualifiedName(context.qualifiedName()),
                           visit(context.tableElement(), TableElement.class),
                           context.EXISTS() != null,
                           processTableProperties(context.tableProperties()));
  }

  @Override
  public Node visitCreateTopic(SqlBaseParser.CreateTopicContext context) {
    return new CreateTopic(getLocation(context), getQualifiedName(context.qualifiedName()),
                           context.EXISTS() != null,
                           processTableProperties(context.tableProperties()));
  }

  @Override
  public Node visitCreateStream(SqlBaseParser.CreateStreamContext context) {
    return new CreateStream(getLocation(context), getQualifiedName(context.qualifiedName()),
                            visit(context.tableElement(), TableElement.class),
                            context.EXISTS() != null,
                            processTableProperties(context.tableProperties()));
  }

  @Override
  public Node visitCreateStreamAs(SqlBaseParser.CreateStreamAsContext context) {
    return new CreateStreamAsSelect(getLocation(context), getQualifiedName(context.qualifiedName()),
                                    (Query) visitQuery(context.query()),
                                    context.EXISTS() != null,
                                    processTableProperties(context.tableProperties()));
  }

  @Override
  public Node visitCreateTableAs(SqlBaseParser.CreateTableAsContext context) {
    return new CreateTableAsSelect(getLocation(context), getQualifiedName(context.qualifiedName()),
                                   (Query) visitQuery(context.query()),
                                   context.EXISTS() != null,
                                   processTableProperties(context.tableProperties()));
  }

  @Override
  public Node visitDropTable(SqlBaseParser.DropTableContext context) {
    return new DropTable(getLocation(context), getQualifiedName(context.qualifiedName()),
                         context.EXISTS() != null);
  }

  // ********************** query expressions ********************

  @Override
  public Node visitQuery(SqlBaseParser.QueryContext context) {
    Query body = (Query) visit(context.queryNoWith());

    return new Query(
        getLocation(context),
        visitIfPresent(context.with(), With.class),
        body.getQueryBody(),
        body.getOrderBy(),
        body.getLimit());
  }

  @Override
  public Node visitWith(SqlBaseParser.WithContext context) {
    return new With(getLocation(context), context.RECURSIVE() != null,
                    visit(context.namedQuery(), WithQuery.class));
  }

  @Override
  public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context) {
    return new WithQuery(getLocation(context), context.name.getText(),
                         (Query) visit(context.query()),
                         Optional.ofNullable(getColumnAliases(context.columnAliases())));
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
          Optional.<With>empty(),
          new QuerySpecification(
              getLocation(context),
              query.getSelect(),
              query.getInto(),
              query.getFrom(),
              query.getWindowExpression(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              visit(context.sortItem(), SortItem.class),
              getTextIfPresent(context.limit)),
          ImmutableList.of(),
          Optional.<String>empty());
    }

    return new Query(
        getLocation(context),
        Optional.<With>empty(),
        term,
        visit(context.sortItem(), SortItem.class),
        getTextIfPresent(context.limit));
  }

  @Override
  public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
    Table into;
    if (context.into != null) {
      into = (Table) visit(context.into);
    } else {
      // TODO: Generate a unique name
      String intoName = "KQL_Stream_" + System.currentTimeMillis();
      into = new Table(QualifiedName.of(intoName), true);
    }

    Relation from = (Relation) visit(context.from);



    Select
        select =
        new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()),
                   visit(context.selectItem(), SelectItem.class));
    select = new Select(getLocation(context.SELECT()), select.isDistinct(), extractSelectItems(select, from));
    this.resultDataSource = getResultDatasource(select, into);

    return new QuerySpecification(
        getLocation(context),
        select,
        Optional.of(into),
        Optional.of(from),
        visitIfPresent(context.windowExpression(), WindowExpression.class),
        visitIfPresent(context.where, Expression.class),
        visitIfPresent(context.groupBy(), GroupBy.class),
        visitIfPresent(context.having, Expression.class),
        ImmutableList.of(),
        Optional.<String>empty());
  }

  private List<SelectItem> extractSelectItems(Select select, Relation from) {
    List<SelectItem> selectItems = new ArrayList<>();
    for (SelectItem selectItem : select.getSelectItems()) {
      if (selectItem instanceof AllColumns) {
        // expand * and T.*
        AllColumns allColumns = (AllColumns) selectItem;

        if (from instanceof Join) {
          Join join = (Join) from;
          AliasedRelation left = (AliasedRelation) join.getLeft();
          StructuredDataSource
              leftDataSource =
              dataSourceExtractor.getMetaStore().getSource(left.getRelation().toString());
          if (leftDataSource == null) {
            throw new InvalidColumnReferenceException(left.getRelation().toString() + " does not exist.");
          }
          AliasedRelation right = (AliasedRelation) join.getRight();
          StructuredDataSource
              rightDataSource =
              dataSourceExtractor.getMetaStore().getSource(right.getRelation().toString());
          if (rightDataSource == null) {
            throw new InvalidColumnReferenceException(right.getRelation().toString() + " does not exist.");
          }
          for (Field field : leftDataSource.getSchema().fields()) {
            QualifiedNameReference
                qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(),
                                           QualifiedName.of(left.getAlias() + "." + field.name()));
            SingleColumn
                newSelectItem =
                new SingleColumn(qualifiedNameReference,
                                 left.getAlias() + "_" + field.name());
            selectItems.add(newSelectItem);
          }
          for (Field field : rightDataSource.getSchema().fields()) {
            QualifiedNameReference
                qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(),
                                           QualifiedName.of(right.getAlias() + "." + field.name()));
            SingleColumn
                newSelectItem =
                new SingleColumn(qualifiedNameReference,
                                 right.getAlias() + "_" + field.name());
            selectItems.add(newSelectItem);
          }
        } else {
          AliasedRelation fromRel = (AliasedRelation) from;
          StructuredDataSource
              fromDataSource =
              dataSourceExtractor.getMetaStore()
                  .getSource(((Table) fromRel.getRelation()).getName().getSuffix());
          if (fromDataSource == null) {
            throw new InvalidColumnReferenceException(
                ((Table) fromRel.getRelation()).getName().getSuffix() + " does not exist."
            );
          }
          for (Field field : fromDataSource.getSchema().fields()) {
            QualifiedNameReference
                qualifiedNameReference =
                new QualifiedNameReference(allColumns.getLocation().get(), QualifiedName
                    .of(fromDataSource.getName() + "." + field.name()));
            SingleColumn
                newSelectItem =
                new SingleColumn(qualifiedNameReference, field.name());
            selectItems.add(newSelectItem);
          }
        }

      } else if (selectItem instanceof SingleColumn) {
        selectItems.add((SingleColumn) selectItem);
      } else {
        throw new IllegalArgumentException(
            "Unsupported SelectItem type: " + selectItem.getClass().getName());
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
    }
    throw new KQLException("Window description is not correct.");
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
  public Node visitGroupBy(SqlBaseParser.GroupByContext context) {
    return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()),
                       visit(context.groupingElement(), GroupingElement.class));
  }

  @Override
  public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context) {
    return new SimpleGroupBy(getLocation(context),
                             visit(context.groupingExpressions().expression(), Expression.class));
  }

  @Override
  public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context) {
    return new GroupingSets(getLocation(context), context.groupingSet().stream()
        .map(groupingSet -> groupingSet.qualifiedName().stream()
            .map(AstBuilder::getQualifiedName)
            .collect(toList()))
        .collect(toList()));
  }

  @Override
  public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
    QueryBody left = (QueryBody) visit(context.left);
    QueryBody right = (QueryBody) visit(context.right);

    boolean
        distinct =
        context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

    switch (context.operator.getType()) {
      case SqlBaseLexer.UNION:
        return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
      case SqlBaseLexer.INTERSECT:
        return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right),
                             distinct);
      case SqlBaseLexer.EXCEPT:
        return new Except(getLocation(context.EXCEPT()), left, right, distinct);
    }

    throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
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
    Optional<String> alias = Optional.ofNullable(context.identifier()).map(AstBuilder::getIdentifierText);
    if (!alias.isPresent()) {
      if (selectItemExpression instanceof QualifiedNameReference) {
        QualifiedNameReference
            qualifiedNameReference =
            (QualifiedNameReference) selectItemExpression;
        alias = Optional.of(qualifiedNameReference.getName().getSuffix());
      } else if (selectItemExpression instanceof DereferenceExpression) {
        DereferenceExpression dereferenceExpression = (DereferenceExpression) selectItemExpression;
        if ((dataSourceExtractor.getJoinLeftSchema() != null) && (dataSourceExtractor
                                                                      .getCommonFieldNames()
                                                                      .contains(
                                                                          dereferenceExpression
                                                                              .getFieldName()))) {
          alias =
              Optional.of(dereferenceExpression.getBase().toString() + "_"
                          + dereferenceExpression.getFieldName());
        } else {
          alias = Optional.of(dereferenceExpression.getFieldName());
        }
      } else {
        alias = Optional.of("KQL_COL_" + selectItemIndex);
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
  public Node visitListTopics(SqlBaseParser.ListTopicsContext context) {
    if (context.TOPICS() == null) {
      throw new KQLException("Syntax error! Did you mean: list topics");
    }
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
    return new TerminateQuery(getLocation(context), Long.parseLong(context.INTEGER_VALUE().getText()));
  }

  @Override
  public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
    return new ShowColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
  }

  @Override
  public Node visitListProperties(SqlBaseParser.ListPropertiesContext context) {
    return new ListProperties(Optional.ofNullable(getLocation(context)));
  }

  @Override
  public Node visitSetProperty(SqlBaseParser.SetPropertyContext context) {
    String propertyName = context.STRING(0).getText();
    propertyName = propertyName.substring(1, propertyName.length() - 1);
    String propertyValue = context.STRING(1).getText();
    propertyValue = propertyValue.substring(1, propertyValue.length() - 1);
    return new SetProperty(Optional.ofNullable(getLocation(context)), propertyName, propertyValue);
  }

  @Override
  public Node visitPrintTopic(SqlBaseParser.PrintTopicContext context) {
    if (context.number() == null) {
      return new PrintTopic(getLocation(context), getQualifiedName(context.qualifiedName()), null);
    } else if (context.number() instanceof SqlBaseParser.IntegerLiteralContext) {
      SqlBaseParser.IntegerLiteralContext
          integerLiteralContext =
          (SqlBaseParser.IntegerLiteralContext) context.number();
      return new PrintTopic(getLocation(context), getQualifiedName(context.qualifiedName()),
                            (LongLiteral) visitIntegerLiteral(integerLiteralContext));
    } else {
      throw new KQLException("Interval value should be integer in 'PRINT' command!");
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
    }

    throw new IllegalArgumentException("Unsupported EXPLAIN format: " + context.value.getText());
  }

  @Override
  public Node visitExplainType(SqlBaseParser.ExplainTypeContext context) {
    switch (context.value.getType()) {
      case SqlBaseLexer.LOGICAL:
        return new ExplainType(getLocation(context), ExplainType.Type.LOGICAL);
      case SqlBaseLexer.DISTRIBUTED:
        return new ExplainType(getLocation(context), ExplainType.Type.DISTRIBUTED);
    }

    throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
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
        (Expression) visit(context.right));
  }

  // *************** from clause *****************

  @Override
  public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
    Relation left = (Relation) visit(context.left);
    Relation right;

    if (context.CROSS() != null) {
      right = (Relation) visit(context.right);
      return new Join(getLocation(context), Join.Type.CROSS, left, right,
                      Optional.<JoinCriteria>empty());
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

    String alias = null;
    if (context.children.size() == 1) {
      Table table = (Table) visit(context.relationPrimary());
      alias = table.getName().getSuffix();

    } else if (context.children.size() == 2) {
      alias = context.children.get(1).getText();
    }

    // TODO: Figure out if the call to toUpperCase() here is really necessary
    return new AliasedRelation(getLocation(context), child, alias.toUpperCase(),
                               getColumnAliases(context.columnAliases()));

  }

  @Override
  public Node visitTableName(SqlBaseParser.TableNameContext context) {
    return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
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
        (Expression) visit(context.right));
  }

  @Override
  public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
    Expression expression = new ComparisonExpression(
        getLocation(context),
        ComparisonExpression.Type.IS_DISTINCT_FROM,
        (Expression) visit(context.value),
        (Expression) visit(context.right));

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
        (Expression) visit(context.upper));

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
        new LikePredicate(getLocation(context), (Expression) visit(context.value),
                          (Expression) visit(context.pattern), escape);

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
        new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

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
        new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

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
        (Expression) visit(context.right));
  }

  @Override
  public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
    return new FunctionCall(
        getLocation(context.CONCAT()),
        QualifiedName.of("concat"), ImmutableList.of(
        (Expression) visit(context.left),
        (Expression) visit(context.right)));
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
    return new Cast(getLocation(context), (Expression) visit(context.expression()),
                    getType(context.type()), isTryCast);
  }

  @Override
  public Node visitExtract(SqlBaseParser.ExtractContext context) {
    String fieldString = getIdentifierText(context.identifier());
    Extract.Field field;
    try {
      field = Extract.Field.valueOf(fieldString);
    } catch (IllegalArgumentException e) {
      throw new ParsingException(format("Invalid EXTRACT field: %s", fieldString), null,
                                 context.getStart().getLine(),
                                 context.getStart().getCharPositionInLine());
    }
    return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
  }

  @Override
  public Node visitSubstring(SqlBaseParser.SubstringContext context) {
    return new FunctionCall(getLocation(context), QualifiedName.of("SUBSTR"),
                            visit(context.valueExpression(), Expression.class));
  }

  @Override
  public Node visitPosition(SqlBaseParser.PositionContext context) {
    List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
    return new FunctionCall(getLocation(context), QualifiedName.of("STRPOS"), arguments);
  }

  @Override
  public Node visitNormalize(SqlBaseParser.NormalizeContext context) {
    Expression str = (Expression) visit(context.valueExpression());
    String
        normalForm =
        Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
    return new FunctionCall(getLocation(context), QualifiedName.of("NORMALIZE"), ImmutableList
        .of(str, new StringLiteral(getLocation(context), normalForm)));
  }

  @Override
  public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
    return new SubscriptExpression(getLocation(context), (Expression) visit(context.value),
                                   (Expression) visit(context.index));
  }

  @Override
  public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
    return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitDereference(SqlBaseParser.DereferenceContext context) {
    String fieldName = getIdentifierText(context.identifier());
    Expression baseExpression;
    // TODO: Base names for dereference expressions shouldn't be validated/resolved as columns;
    // this try/catch is just a temporary workaround
    try {
      baseExpression = (Expression) visit(context.primaryExpression());
    } catch (InvalidColumnReferenceException exception) {
      QualifiedName tableName = QualifiedName.of(context.primaryExpression().getText().toUpperCase());
      baseExpression = new QualifiedNameReference(getLocation(context.primaryExpression()), tableName);
    }
    DereferenceExpression
        dereferenceExpression =
        new DereferenceExpression(getLocation(context), baseExpression, fieldName);
    return dereferenceExpression;
  }

  @Override
  public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
    String columnName = getIdentifierText(context.identifier());
    // If this is join.
    if (dataSourceExtractor.getJoinLeftSchema() != null) {
      if (dataSourceExtractor.getCommonFieldNames().contains(columnName)) {
        throw new KQLException("Field " + columnName + " is ambiguous.");
      } else if (dataSourceExtractor.getLeftFieldNames().contains(columnName)) {
        Expression
            baseExpression =
            new QualifiedNameReference(getLocation(context),
                                       QualifiedName.of(dataSourceExtractor.getLeftAlias()));
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else if (dataSourceExtractor.getRightFieldNames().contains(columnName)) {
        Expression
            baseExpression =
            new QualifiedNameReference(getLocation(context),
                                       QualifiedName.of(dataSourceExtractor.getRightAlias()));
        return new DereferenceExpression(getLocation(context), baseExpression, columnName);
      } else {
        throw new InvalidColumnReferenceException("Field " + columnName + " is ambiguous.");
      }
    } else {
      Expression
          baseExpression =
          new QualifiedNameReference(getLocation(context),
                                     QualifiedName.of(dataSourceExtractor.getFromAlias()));
      return new DereferenceExpression(getLocation(context), baseExpression, columnName);
    }
  }

  @Override
  public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
    return new SimpleCaseExpression(
        getLocation(context),
        (Expression) visit(context.valueExpression()),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class));
  }

  @Override
  public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
    return new SearchedCaseExpression(
        getLocation(context),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class));
  }

  @Override
  public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
    return new WhenClause(getLocation(context), (Expression) visit(context.condition),
                          (Expression) visit(context.result));
  }

  @Override
  public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
    Optional<Window> window = visitIfPresent(context.over(), Window.class);

    QualifiedName name = getQualifiedName(context.qualifiedName());

    boolean distinct = isDistinct(context.setQuantifier());

    if (name.toString().equals("NULLIF")) {
      check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function",
            context);
      check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
      check(!distinct, "DISTINCT not valid for 'nullif' function", context);

      return new NullIfExpression(
          getLocation(context),
          (Expression) visit(context.expression(0)),
          (Expression) visit(context.expression(1)));
    }

    return new FunctionCall(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        window,
        distinct,
        visit(context.expression(), Expression.class));
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
    return new TableElement(getLocation(context), getIdentifierText(context.identifier()),
                            getType(context.type()));
  }

  @Override
  public Node visitSortItem(SqlBaseParser.SortItemContext context) {
    return new SortItem(
        getLocation(context),
        (Expression) visit(context.expression()),
        Optional.ofNullable(context.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING),
        Optional.ofNullable(context.nullOrdering)
            .map(AstBuilder::getNullOrderingType)
            .orElse(SortItem.NullOrdering.UNDEFINED));
  }

  @Override
  public Node visitWindowFrame(SqlBaseParser.WindowFrameContext context) {
    return new WindowFrame(
        getLocation(context),
        getFrameType(context.frameType),
        (FrameBound) visit(context.start),
        visitIfPresent(context.end, FrameBound.class));
  }

  @Override
  public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context) {
    return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
  }

  @Override
  public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context) {
    return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType),
                          (Expression) visit(context.expression()));
  }

  @Override
  public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context) {
    return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
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
            .map(AstBuilder::getIntervalFieldType));
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

  private static String unquote(String value, String quote) {
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

  private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  private static Optional<String> getTextIfPresent(Token token) {
    return Optional.ofNullable(token)
        .map(Token::getText);
  }

  private static List<String> getColumnAliases(
      SqlBaseParser.ColumnAliasesContext columnAliasesContext) {
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
    }

    throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
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
    }

    throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
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
    }

    throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
  }

  private static IntervalLiteral.Sign getIntervalSign(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.MINUS:
        return IntervalLiteral.Sign.NEGATIVE;
      case SqlBaseLexer.PLUS:
        return IntervalLiteral.Sign.POSITIVE;
    }

    throw new IllegalArgumentException("Unsupported sign: " + token.getText());
  }

  private static WindowFrame.Type getFrameType(Token type) {
    switch (type.getType()) {
      case SqlBaseLexer.RANGE:
        return WindowFrame.Type.RANGE;
      case SqlBaseLexer.ROWS:
        return WindowFrame.Type.ROWS;
    }

    throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
  }

  private static FrameBound.Type getBoundedFrameBoundType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.PRECEDING:
        return FrameBound.Type.PRECEDING;
      case SqlBaseLexer.FOLLOWING:
        return FrameBound.Type.FOLLOWING;
    }

    throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
  }

  private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.PRECEDING:
        return FrameBound.Type.UNBOUNDED_PRECEDING;
      case SqlBaseLexer.FOLLOWING:
        return FrameBound.Type.UNBOUNDED_FOLLOWING;
    }

    throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
  }

  private static SampledRelation.Type getSamplingMethod(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.BERNOULLI:
        return SampledRelation.Type.BERNOULLI;
      case SqlBaseLexer.SYSTEM:
        return SampledRelation.Type.SYSTEM;
      case SqlBaseLexer.POISSONIZED:
        return SampledRelation.Type.POISSONIZED;
    }

    throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
  }

  private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.AND:
        return LogicalBinaryExpression.Type.AND;
      case SqlBaseLexer.OR:
        return LogicalBinaryExpression.Type.OR;
    }

    throw new IllegalArgumentException("Unsupported operator: " + token.getText());
  }

  private static SortItem.NullOrdering getNullOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.FIRST:
        return SortItem.NullOrdering.FIRST;
      case SqlBaseLexer.LAST:
        return SortItem.NullOrdering.LAST;
    }

    throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
  }

  private static SortItem.Ordering getOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.ASC:
        return SortItem.Ordering.ASCENDING;
      case SqlBaseLexer.DESC:
        return SortItem.Ordering.DESCENDING;
    }

    throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
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
    } else if (baseType.TIME_WITH_TIME_ZONE() != null) {
      return baseType.TIME_WITH_TIME_ZONE().getText().toUpperCase();
    } else if (baseType.TIMESTAMP_WITH_TIME_ZONE() != null) {
      return baseType.TIMESTAMP_WITH_TIME_ZONE().getText().toUpperCase();
    } else {
      throw new KQLException(
          "Base type must contain either identifier, time with time zone, or timestamp with time zone"
      );
    }
  }

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw new ParsingException(message, null, context.getStart().getLine(),
                                 context.getStart().getCharPositionInLine());
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
        String fieldType = null;
        dataSource = dataSource.field(fieldName, Schema.BOOLEAN_SCHEMA);
      }


    }

    KQLTopic kqlTopic = new KQLTopic(into.getName().toString(), into.getName().toString(),
                                     null);
    StructuredDataSource
        resultStream =
        new KQLStream(into.getName().toString(), dataSource.schema(), dataSource.fields().get(0),
                      kqlTopic
        );
    return resultStream;
  }

  private static class InvalidColumnReferenceException extends KQLException {
    public InvalidColumnReferenceException(String message) {
      super(message);
    }

    public InvalidColumnReferenceException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
