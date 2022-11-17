/*
 * Copyright 2021 Confluent Inc.
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.confluent.io/confluent-community-license
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine.rewrite;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.AstBuilder;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.SqlBaseBaseVisitor;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.AliasedRelationContext;
import io.confluent.ksql.parser.SqlBaseParser.AlterOptionContext;
import io.confluent.ksql.parser.SqlBaseParser.AlterSourceContext;
import io.confluent.ksql.parser.SqlBaseParser.AlterSystemPropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.BooleanDefaultContext;
import io.confluent.ksql.parser.SqlBaseParser.BooleanLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateStreamAsContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateStreamContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateTableAsContext;
import io.confluent.ksql.parser.SqlBaseParser.CreateTableContext;
import io.confluent.ksql.parser.SqlBaseParser.DefineVariableContext;
import io.confluent.ksql.parser.SqlBaseParser.DescribeConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DescribeFunctionContext;
import io.confluent.ksql.parser.SqlBaseParser.DescribeStreamsContext;
import io.confluent.ksql.parser.SqlBaseParser.DropConnectorContext;
import io.confluent.ksql.parser.SqlBaseParser.DropStreamContext;
import io.confluent.ksql.parser.SqlBaseParser.DropTableContext;
import io.confluent.ksql.parser.SqlBaseParser.DropTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.ExplainContext;
import io.confluent.ksql.parser.SqlBaseParser.ExpressionContext;
import io.confluent.ksql.parser.SqlBaseParser.GroupByContext;
import io.confluent.ksql.parser.SqlBaseParser.InsertIntoContext;
import io.confluent.ksql.parser.SqlBaseParser.InsertValuesContext;
import io.confluent.ksql.parser.SqlBaseParser.IntegerLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.JoinRelationContext;
import io.confluent.ksql.parser.SqlBaseParser.JoinWindowSizeContext;
import io.confluent.ksql.parser.SqlBaseParser.JoinWindowWithBeforeAndAfterContext;
import io.confluent.ksql.parser.SqlBaseParser.JoinedSourceContext;
import io.confluent.ksql.parser.SqlBaseParser.LeftJoinContext;
import io.confluent.ksql.parser.SqlBaseParser.ListConnectorsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListFunctionsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListPropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.ListQueriesContext;
import io.confluent.ksql.parser.SqlBaseParser.ListStreamsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListTopicsContext;
import io.confluent.ksql.parser.SqlBaseParser.ListTypesContext;
import io.confluent.ksql.parser.SqlBaseParser.ListVariablesContext;
import io.confluent.ksql.parser.SqlBaseParser.LogicalBinaryContext;
import io.confluent.ksql.parser.SqlBaseParser.NumericLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.OuterJoinContext;
import io.confluent.ksql.parser.SqlBaseParser.PartitionByContext;
import io.confluent.ksql.parser.SqlBaseParser.PrintTopicContext;
import io.confluent.ksql.parser.SqlBaseParser.QueryContext;
import io.confluent.ksql.parser.SqlBaseParser.RegisterTypeContext;
import io.confluent.ksql.parser.SqlBaseParser.RelationDefaultContext;
import io.confluent.ksql.parser.SqlBaseParser.SelectItemContext;
import io.confluent.ksql.parser.SqlBaseParser.SelectSingleContext;
import io.confluent.ksql.parser.SqlBaseParser.SetPropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.ShowColumnsContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleExpressionContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleJoinWindowContext;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.SqlBaseParser.StatementsContext;
import io.confluent.ksql.parser.SqlBaseParser.StringLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.TableElementContext;
import io.confluent.ksql.parser.SqlBaseParser.TableElementsContext;
import io.confluent.ksql.parser.SqlBaseParser.TableNameContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertiesContext;
import io.confluent.ksql.parser.SqlBaseParser.TablePropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.TerminateQueryContext;
import io.confluent.ksql.parser.SqlBaseParser.TypeContext;
import io.confluent.ksql.parser.SqlBaseParser.UndefineVariableContext;
import io.confluent.ksql.parser.SqlBaseParser.UnquotedIdentifierContext;
import io.confluent.ksql.parser.SqlBaseParser.UnsetPropertyContext;
import io.confluent.ksql.parser.SqlBaseParser.ValueExpressionContext;
import io.confluent.ksql.parser.SqlBaseParser.WithinExpressionContext;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

public class QueryAnonymizer {

  public String anonymize(final ParseTree tree) {
    return build(tree);
  }

  public String anonymize(final String query) {
    final ParseTree tree = DefaultKsqlParser.getParseTree(query);
    return build(tree);
  }

  private String build(final ParseTree parseTree) {
    return new Visitor().visit(parseTree);
  }

  private static final class Visitor extends SqlBaseBaseVisitor<String> {
    private int streamCount = 1;
    private int columnCount = 1;
    private int tableCount = 1;
    private int headerCount = 1;
    private int udfCount = 1;
    private int sourceCount = 1;
    private final Hashtable<String, String> anonTable = new Hashtable<>();

    @Override
    public String visitStatements(final StatementsContext context) {
      final List<String> statementList = new ArrayList<>();
      for (final SingleStatementContext stmtContext : context.singleStatement()) {
        final String statement = visitSingleStatement(stmtContext);
        statementList.add(statement);
      }
      return StringUtils.join(statementList, "\n");
    }

    @Override
    public String visitSingleStatement(final SingleStatementContext context) {
      return String.format("%s;", visit(context.statement()));
    }

    @Override
    public String visitType(final TypeContext context) {
      if (context.type().isEmpty()) {
        return getTypeName(context);
      }

      // get type name
      String typeName = "STRUCT";
      if (context.MAP() != null) {
        typeName = "MAP";
      } else if (context.ARRAY() != null) {
        typeName = "ARRAY";
      }

      // get type list
      final List<String> typeList = new ArrayList<>();
      for (TypeContext typeContext : context.type()) {
        typeList.add(visit(typeContext));
      }

      return String.format("%s<%s>", typeName, StringUtils.join(typeList, ", "));
    }

    @Override
    public String visitAlterSource(final AlterSourceContext context) {
      final StringBuilder stringBuilder = new StringBuilder("ALTER");

      // anonymize stream or table name
      final String streamTable = ParserUtil.getIdentifierText(context.sourceName().identifier());
      if (context.STREAM() != null) {
        stringBuilder.append(String.format(" STREAM %s", getAnonStreamName(streamTable)));
      } else {
        stringBuilder.append(String.format(" TABLE %s", getAnonTableName(streamTable)));
      }

      // alter option
      final List<String> alterOptions = new ArrayList<>();
      for (AlterOptionContext alterOption : context.alterOption()) {
        alterOptions.add(visit(alterOption));
      }
      stringBuilder.append(String.format(" (%s)", StringUtils.join(alterOptions, ", ")));

      return stringBuilder.toString();
    }

    @Override
    public String visitAlterOption(final AlterOptionContext context) {
      final String columnName = context.identifier().getText();
      final String anonColumnName = getAnonColumnName(columnName);

      return String.format("ADD COLUMN %1$s %2$s", anonColumnName, visit(context.type()));
    }

    @Override
    public String visitRegisterType(final RegisterTypeContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE TYPE");

      // optional if not exists
      if (context.EXISTS() != null) {
        stringBuilder.append(" IF NOT EXISTS");
      }

      // anonymize type
      stringBuilder.append(String.format(" type AS %s", visit(context.type())));

      return stringBuilder.toString();
    }

    @Override
    public String visitCreateConnector(final CreateConnectorContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE");

      if (context.SOURCE() != null) {
        stringBuilder.append(" SOURCE");
      } else if (context.SINK() != null) {
        stringBuilder.append(" SINK");
      }

      stringBuilder.append(" CONNECTOR");

      // optional if not exists
      if (context.EXISTS() != null) {
        stringBuilder.append(" IF NOT EXISTS");
      }

      stringBuilder.append(" connector ");

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitInsertInto(final InsertIntoContext context) {
      final StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");

      // anonymize stream name
      final String streamName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonStreamName(streamName));

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      // anonymize with query
      if (context.query() != null) {
        stringBuilder.append(String.format(" %s", visit(context.query())));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitInsertValues(final InsertValuesContext context) {
      final StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");

      // anonymize stream name
      final String streamName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonStreamName(streamName));

      // visit columns
      if (context.columns() != null) {
        final List<String> columns = context.columns().identifier()
            .stream()
            .map(ParserUtil::getIdentifierText)
            .map(ColumnName::of)
            .map(ColumnName::toString)
            .map(this::getAnonColumnName)
            .collect(Collectors.toList());
        stringBuilder.append(String.format(" (%s)", StringUtils.join(columns, ", ")));
      }

      // visit values
      final List<String> values = new ArrayList<>();
      for (ValueExpressionContext value : context.values().valueExpression()) {
        values.add(visit(value));
      }
      stringBuilder.append(String.format(" VALUES (%s)", StringUtils.join(values, " ,")));

      return stringBuilder.toString();
    }

    @Override
    public String visitListConnectors(final ListConnectorsContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      final StringBuilder stringBuilder = new StringBuilder(listOrVisit.toString());

      if (context.SOURCE() != null) {
        stringBuilder.append(" SOURCE");
      } else if (context.SINK() != null) {
        stringBuilder.append(" SINK");
      }

      stringBuilder.append(" CONNECTORS");

      return stringBuilder.toString();
    }

    @Override
    public String visitListStreams(final ListStreamsContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      final StringBuilder stringBuilder = new StringBuilder(listOrVisit.toString() + " STREAMS");

      if (context.EXTENDED() != null) {
        stringBuilder.append(" EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitListTables(final SqlBaseParser.ListTablesContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      final StringBuilder stringBuilder = new StringBuilder(listOrVisit.toString() + " TABLES");

      if (context.EXTENDED() != null) {
        stringBuilder.append(" EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitListFunctions(final ListFunctionsContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      return String.format("%s FUNCTIONS", listOrVisit.toString());
    }

    @Override
    public String visitListProperties(final ListPropertiesContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      return String.format("%s PROPERTIES", listOrVisit.toString());
    }

    @Override
    public String visitListTypes(final ListTypesContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      return String.format("%s TYPES", listOrVisit.toString());
    }

    @Override
    public String visitListVariables(final ListVariablesContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      return String.format("%s VARIABLES", listOrVisit.toString());
    }

    @Override
    public String visitListQueries(final ListQueriesContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      final StringBuilder stringBuilder = new StringBuilder(listOrVisit.toString() + " QUERIES");

      if (context.EXTENDED() != null) {
        stringBuilder.append(" EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitListTopics(final ListTopicsContext context) {
      final TerminalNode listOrVisit = context.LIST() != null ? context.LIST() : context.SHOW();
      final StringBuilder stringBuilder = new StringBuilder(listOrVisit.toString());

      if (context.ALL() != null) {
        stringBuilder.append(" ALL");
      }

      stringBuilder.append(" TOPICS");

      if (context.EXTENDED() != null) {
        stringBuilder.append(" EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitDescribeFunction(final DescribeFunctionContext context) {
      return "DESCRIBE FUNCTION function";
    }

    @Override
    public String visitDescribeConnector(final DescribeConnectorContext context) {
      return "DESCRIBE CONNECTOR connector";
    }

    @Override
    public String visitPrintTopic(final PrintTopicContext context) {
      final StringBuilder stringBuilder = new StringBuilder("PRINT topic");

      if (context.printClause().FROM() != null) {
        stringBuilder.append(" FROM BEGINNING");
      }

      if (context.printClause().intervalClause() != null) {
        stringBuilder.append(" INTERVAL '0'");
      }

      if (context.printClause().limitClause() != null) {
        stringBuilder.append(" LIMIT '0'");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitTerminateQuery(final TerminateQueryContext context) {
      if (context.ALL() != null) {
        return "TERMINATE ALL";
      }
      return "TERMINATE query";
    }


    @Override
    public String visitDescribeStreams(final DescribeStreamsContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DESCRIBE STREAMS ");

      if (context.EXTENDED() != null) {
        stringBuilder.append("EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitShowColumns(final ShowColumnsContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DESCRIBE ");

      final String streamTable = ParserUtil.getIdentifierText(context.sourceName().identifier());
      if (context.sourceName().identifier() instanceof UnquotedIdentifierContext
          && context.sourceName().getText().equalsIgnoreCase("TABLES")) {
        stringBuilder.append(getAnonTableName(streamTable));
      } else {
        stringBuilder.append(getAnonStreamName(streamTable));
      }

      if (context.EXTENDED() != null) {
        stringBuilder.append(" EXTENDED");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitSetProperty(final SetPropertyContext context) {
      final String propertyName = context.STRING(0).getText();
      return String.format("SET %s='[string]'", propertyName);
    }

    @Override
    public String visitAlterSystemProperty(final AlterSystemPropertyContext context) {
      final String propertyName = context.STRING(0).getText();
      return String.format("ALTER SYSTEM %s='[string]'", propertyName);
    }

    @Override
    public String visitUnsetProperty(final UnsetPropertyContext context) {
      final String propertyName = context.STRING().getText();
      return String.format("UNSET %s", propertyName);
    }

    @Override
    public String visitDefineVariable(final DefineVariableContext context) {
      return "DEFINE variable='[string]'";
    }

    @Override
    public String visitUndefineVariable(final UndefineVariableContext context) {
      return "UNDEFINE variable";
    }

    @Override
    public String visitExplain(final ExplainContext context) {
      if (context.identifier() != null) {
        return "EXPLAIN query";
      }

      final String subQuery = visit(context.statement());
      return String.format("EXPLAIN %s", subQuery);
    }

    @Override
    public String visitExpression(final ExpressionContext context) {
      final String columnName = context.getText();

      // check if it's an udf
      if (new AstBuilder(TypeRegistry.EMPTY).buildExpression(context) instanceof FunctionCall) {
        return getAnonUdfName(columnName);
      }

      return getAnonColumnName(columnName);
    }

    @Override
    public String visitSelectSingle(final SelectSingleContext context) {
      return visit(context.expression());
    }

    @Override
    public String visitSingleExpression(final SingleExpressionContext context) {
      return visit(context.expression());
    }

    @Override
    public String visitBooleanDefault(final BooleanDefaultContext context) {
      final String columnName = context.getChild(0).getChild(0).getText();
      final String anonColumnName = getAnonColumnName(columnName);

      if (context.getChild(0).getChild(1) != null) {
        final String anonValue = visit(context.getChild(0).getChild(1));
        return String.format("%1$s=%2$s", anonColumnName, anonValue);
      }

      return anonColumnName;
    }

    @Override
    public String visitLogicalBinary(final LogicalBinaryContext context) {
      return String.format("%1$s %2$s %3$s",
          visit(context.left), context.operator.getText(), visit(context.right));
    }

    @Override
    public String visitPartitionBy(final PartitionByContext context) {
      final String columnName = context.getText();
      return getAnonColumnName(columnName);
    }

    @Override
    public String visitGroupBy(final GroupByContext context) {
      final String columnName = context.getText();
      return getAnonColumnName(columnName);
    }

    @Override
    public String visitStringLiteral(final StringLiteralContext context) {
      return "'[string]'";
    }

    @Override
    public String visitIntegerLiteral(final IntegerLiteralContext context) {
      return "'0'";
    }

    @Override
    public String visitNumericLiteral(final NumericLiteralContext context) {
      return "'0'";
    }

    @Override
    public String visitBooleanLiteral(final BooleanLiteralContext context) {
      return "'false'";
    }

    @Override
    public String visitCreateStreamAs(final CreateStreamAsContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE ");

      // optional replace
      if (context.OR() != null && context.REPLACE() != null) {
        stringBuilder.append("OR REPLACE ");
      }

      stringBuilder.append("STREAM ");

      // optional if not exists
      if (context.IF() != null && context.NOT() != null && context.EXISTS() != null) {
        stringBuilder.append("IF NOT EXISTS ");
      }

      // anonymize stream name
      final String streamName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonStreamName(streamName));

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      // rest of query
      if (context.query() != null) {
        stringBuilder.append(String.format(" AS %s", visit(context.query())));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitCreateStream(final CreateStreamContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE ");

      // optional source
      if (context.SOURCE() != null) {
        stringBuilder.append("SOURCE ");
      }

      // optional replace
      if (context.OR() != null && context.REPLACE() != null) {
        stringBuilder.append("OR REPLACE ");
      }

      stringBuilder.append("STREAM ");

      // optional if not exists
      if (context.IF() != null && context.NOT() != null && context.EXISTS() != null) {
        stringBuilder.append("IF NOT EXISTS ");
      }

      // anonymize stream name
      final String streamName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(String.format("%s ", getAnonStreamName(streamName)));

      // anonymize table elements
      if (context.tableElements() != null) {
        stringBuilder.append(visit(context.tableElements()));
      }

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitCreateTableAs(final CreateTableAsContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE ");

      // optional replace
      if (context.OR() != null && context.REPLACE() != null) {
        stringBuilder.append("OR REPLACE ");
      }

      stringBuilder.append("TABLE ");

      // optional if not exists
      if (context.IF() != null && context.NOT() != null && context.EXISTS() != null) {
        stringBuilder.append("IF NOT EXISTS ");
      }

      // anonymize table name
      final String tableName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonTableName(tableName));

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      // rest of query
      if (context.query() != null) {
        stringBuilder.append(String.format(" AS %s", visit(context.query())));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitCreateTable(final CreateTableContext context) {
      final StringBuilder stringBuilder = new StringBuilder("CREATE ");

      // optional source
      if (context.SOURCE() != null) {
        stringBuilder.append("SOURCE ");
      }

      // optional replace
      if (context.OR() != null && context.REPLACE() != null) {
        stringBuilder.append("OR REPLACE ");
      }

      stringBuilder.append("TABLE ");

      // optional if not exists
      if (context.IF() != null && context.NOT() != null && context.EXISTS() != null) {
        stringBuilder.append("IF NOT EXISTS ");
      }

      // anonymize table name
      final String tableName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(String.format("%s ", getAnonTableName(tableName)));

      // anonymize table elements
      if (context.tableElements() != null) {
        stringBuilder.append(visit(context.tableElements()));
      }

      // anonymize properties
      if (context.tableProperties() != null) {
        stringBuilder.append(visit(context.tableProperties()));
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitTableElements(final TableElementsContext context) {
      final List<String> tableElements = new ArrayList<>();
      for (final TableElementContext tableContext : context.tableElement()) {
        tableElements.add(visit(tableContext));
      }

      return String.format("(%s) ", StringUtils.join(tableElements, ", "));
    }

    @Override
    public String visitTableElement(final TableElementContext context) {
      final StringBuilder stringBuilder = new StringBuilder();
      final String columnName = ParserUtil.getIdentifierText(context.identifier());
      final String newName = getAnonColumnName(columnName);
      stringBuilder.append(String.format("%1$s %2$s", newName, visit(context.type())));

      final ColumnConstraints constraints =
          ParserUtil.getColumnConstraints(context.columnConstraints());

      if (constraints.isPrimaryKey()) {
        stringBuilder.append(" PRIMARY KEY");
      } else if (constraints.isKey()) {
        stringBuilder.append(" KEY");
      } else if (constraints.isHeaders()) {
        if (constraints.getHeaderKey().isPresent()) {
          stringBuilder.append(" HEADER('")
              .append(getAnonHeaderName(constraints.getHeaderKey().get()))
              .append("')");
        } else {
          stringBuilder.append(" HEADERS");
        }
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitTableProperties(final TablePropertiesContext context) {
      final List<String> tableProperties = new ArrayList<>();
      for (final TablePropertyContext prop : context.tableProperty()) {
        final StringBuilder formattedProp = new StringBuilder();
        if (prop.identifier() != null) {
          formattedProp.append(ParserUtil.getIdentifierText(prop.identifier()));
        } else {
          formattedProp.append(prop.STRING().getText());
        }
        formattedProp.append("=").append(visit(prop.literal()));
        tableProperties.add(formattedProp.toString());
      }

      return String.format("WITH (%s)", StringUtils.join(tableProperties, ", "));
    }

    @Override
    public String visitDropTable(final DropTableContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DROP TABLE ");

      if (context.EXISTS() != null) {
        stringBuilder.append("IF EXISTS ");
      }

      // anonymize table name
      final String tableName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonTableName(tableName));

      if (context.DELETE() != null) {
        stringBuilder.append(" DELETE TOPIC");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitDropStream(final DropStreamContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DROP STREAM ");

      if (context.EXISTS() != null) {
        stringBuilder.append("IF EXISTS ");
      }

      // anonymize stream name
      final String streamName = ParserUtil.getIdentifierText(context.sourceName().identifier());
      stringBuilder.append(getAnonStreamName(streamName));

      if (context.DELETE() != null) {
        stringBuilder.append(" DELETE TOPIC");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitDropConnector(final DropConnectorContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DROP CONNECTOR ");

      if (context.EXISTS() != null) {
        stringBuilder.append("IF EXISTS ");
      }

      stringBuilder.append("connector");

      return stringBuilder.toString();
    }

    @Override
    public String visitDropType(final DropTypeContext context) {
      final StringBuilder stringBuilder = new StringBuilder("DROP TYPE ");

      if (context.EXISTS() != null) {
        stringBuilder.append("IF EXISTS ");
      }

      stringBuilder.append("type");

      return stringBuilder.toString();
    }

    @Override
    public String visitQuery(final QueryContext context) {
      final StringBuilder stringBuilder = new StringBuilder("SELECT ");

      // visit as select items
      final List<String> selectItemList = new ArrayList<>();
      for (SelectItemContext selectItem : context.selectItem()) {
        if (selectItem.getText().equals("*")) {
          selectItemList.add("*");
        } else {
          selectItemList.add(visit(selectItem));
        }
      }
      stringBuilder.append(StringUtils.join(selectItemList, ", "));

      // visit from statement
      stringBuilder.append(String.format(" FROM %s", visit(context.from)));

      // visit where statement
      if (context.where != null) {
        stringBuilder.append(String.format(" WHERE %s", visit(context.where)));
      }

      // visit partition by
      if (context.partitionBy() != null) {
        stringBuilder.append(String.format(" PARTITION BY %s", visit(context.partitionBy())));
      }

      // visit group by
      if (context.groupBy() != null) {
        stringBuilder.append(String.format(" GROUP BY %s", visit(context.groupBy())));
      }

      // visit emit changes
      if (context.EMIT() != null) {
        stringBuilder.append(" EMIT CHANGES");
      }

      return stringBuilder.toString();
    }

    @Override
    public String visitAliasedRelation(final AliasedRelationContext context) {
      return visit(context.relationPrimary());
    }

    @Override
    public String visitRelationDefault(final RelationDefaultContext context) {
      return getAnonSourceName(context.getText());
    }

    @Override
    public String visitTableName(final TableNameContext context) {
      return getAnonSourceName(context.getText());
    }

    @Override
    public String visitJoinRelation(final JoinRelationContext context) {
      final String left = visit(context.left);
      final ImmutableList<String> rights = context
          .joinedSource()
          .stream()
          .map(this::visitJoinedSource)
          .collect(ImmutableList.toImmutableList());

      return String.format("%s %s", left, String.join(" ", rights));
    }

    @Override
    public String visitJoinedSource(final JoinedSourceContext context) {
      final StringBuilder stringBuilder = new StringBuilder();

      // get join type
      final SqlBaseParser.JoinTypeContext joinTypeContext = context.joinType();
      if (joinTypeContext instanceof LeftJoinContext) {
        stringBuilder.append("LEFT OUTER ");
      } else if (joinTypeContext instanceof OuterJoinContext) {
        stringBuilder.append("FULL OUTER ");
      } else {
        stringBuilder.append("INNER ");
      }

      // right side of join
      final String right = visit(context.aliasedRelation());
      stringBuilder.append(String.format("JOIN %s", right));

      // visit within expression
      if (context.joinWindow() != null) {
        stringBuilder.append(visitWithinExpression(context.joinWindow().withinExpression()));
      }

      // visit join on
      stringBuilder.append("ON anonKey1=anonKey2");

      return stringBuilder.toString();
    }

    private static String visitWithinExpression(final WithinExpressionContext context) {
      final StringBuilder stringBuilder = new StringBuilder(" WITHIN ");

      if (context instanceof SingleJoinWindowContext) {
        final SqlBaseParser.SingleJoinWindowContext singleWithin
            = (SqlBaseParser.SingleJoinWindowContext) context;

        stringBuilder.append(String.format("%s",
            anonymizeJoinWindowSize(singleWithin.joinWindowSize())));

        if (singleWithin.gracePeriodClause() != null) {
          stringBuilder.append(String.format(" GRACE PERIOD %s",
              anonymizeGracePeriod(singleWithin.gracePeriodClause())));
        }
      } else if (context instanceof JoinWindowWithBeforeAndAfterContext) {
        final SqlBaseParser.JoinWindowWithBeforeAndAfterContext beforeAndAfterJoinWindow
            = (SqlBaseParser.JoinWindowWithBeforeAndAfterContext) context;

        stringBuilder.append(String.format("(%s, %s)",
            anonymizeJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(0)),
            anonymizeJoinWindowSize(beforeAndAfterJoinWindow.joinWindowSize(1))));

        if (beforeAndAfterJoinWindow.gracePeriodClause() != null) {
          stringBuilder.append(String.format(" GRACE PERIOD %s",
              anonymizeGracePeriod(beforeAndAfterJoinWindow.gracePeriodClause())));
        }
      } else {
        throw new RuntimeException("Expecting either a single join window, ie \"WITHIN 10 "
            + "seconds\" or \"WITHIN 10 seconds GRACE PERIOD 2 seconds\", or a join window with "
            + "before and after specified, ie. \"WITHIN (10 seconds, 20 seconds)\" or "
            + "WITHIN (10 seconds, 20 seconds) GRACE PERIOD 5 seconds");
      }

      return stringBuilder.append(' ').toString();
    }

    private static String anonymizeGracePeriod(
        final SqlBaseParser.GracePeriodClauseContext gracePeriodClause
    ) {
      return String.format("'0' %s", gracePeriodClause.windowUnit().getText().toUpperCase());
    }

    private static String anonymizeJoinWindowSize(
        final JoinWindowSizeContext joinWindowSize
    ) {
      return String.format("'0' %s", joinWindowSize.windowUnit().getText().toUpperCase());
    }

    private String getTypeName(final TypeContext context) {
      if (context.DECIMAL() != null) {
        return "DECIMAL";
      }
      switch (context.getText().toUpperCase()) {
        case ("BOOLEAN"):
        case ("INTEGER"):
        case ("INT"):
        case ("BIGINT"):
        case ("DOUBLE"):
        case ("STRING"):
        case ("VARCHAR"):
        case ("BYTES"):
          return context.getText().toUpperCase();
        default:
          return "CUSTOM_TYPE";
      }
    }

    private String getAnonSourceName(final String originName) {
      return getAnonName(originName, "source", sourceCount++);
    }

    private String getAnonUdfName(final String originName) {
      return getAnonName(originName, "udf", udfCount++);
    }

    private String getAnonStreamName(final String originName) {
      return getAnonName(originName, "stream", streamCount++);
    }

    private String getAnonColumnName(final String originName) {
      return getAnonName(originName, "column", columnCount++);
    }

    private String getAnonTableName(final String originName) {
      return getAnonName(originName, "table", tableCount++);
    }

    private String getAnonHeaderName(final String originName) {
      return getAnonName(originName, "header", headerCount++);
    }

    private String getAnonName(final String originName, final String genericName, final int count) {
      if (anonTable.containsKey(originName + genericName)) {
        return anonTable.get(originName + genericName);
      }

      final String newName = String.format("%s%d", genericName, count);
      anonTable.put(originName + genericName, newName);
      return newName;
    }
  }
}
