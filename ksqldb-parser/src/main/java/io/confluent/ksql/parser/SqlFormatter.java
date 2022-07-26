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

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.base.Strings;
import io.confluent.ksql.execution.expression.formatter.ExpressionFormatter;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.Name;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AlterOption;
import io.confluent.ksql.parser.tree.AlterSource;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertTombstone;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DefineVariable;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.DropStatement;
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
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListVariables;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.PauseQuery;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.ResumeQuery;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.IdentifierUtil;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class SqlFormatter {

  private static final String INDENT = "   ";
  private static final FormatOptions FORMAT_OPTIONS = FormatOptions.of(IdentifierUtil::needsQuotes);

  private SqlFormatter() {
  }

  public static String formatSql(final AstNode root) {
    final StringBuilder builder = new StringBuilder();
    new Formatter(builder).process(root, 0);
    return StringUtils.stripEnd(builder.toString(), "\n");
  }

  private static final class Formatter extends AstVisitor<Void, Integer> {

    private final StringBuilder builder;

    private Formatter(final StringBuilder builder) {
      this.builder = Objects.requireNonNull(builder, "builder");
    }

    @Override
    protected Void visitNode(final AstNode node, final Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitQuery(final Query node, final Integer indent) {

      process(node.getSelect(), indent);

      append(indent, "FROM ");
      processRelation(node.getFrom(), indent);
      builder.append('\n');

      if (node.getWindow().isPresent()) {
        append(indent, "WINDOW" + node.getWindow().get().getKsqlWindowExpression().toString())
            .append('\n');
      }

      if (node.getWhere().isPresent()) {
        append(
            indent,
            "WHERE " + formatExpression(node.getWhere().get())
        ).append('\n');
      }

      node.getGroupBy()
          .ifPresent(groupBy -> process(groupBy, indent));

      node.getPartitionBy()
          .ifPresent(partitionBy -> process(partitionBy, indent));

      if (node.getHaving().isPresent()) {
        append(indent, "HAVING "
            + formatExpression(node.getHaving().get()))
            .append('\n');
      }

      if (!node.isPullQuery()) {
        if (node.getRefinement().isPresent()) {
          append(indent, "EMIT ");
          append(indent, node.getRefinement()
              .get()
              .getOutputRefinement()
              .toString())
              .append('\n');
        }
      }

      if (node.getLimit().isPresent()) {
        append(indent, "LIMIT " + node.getLimit().getAsInt())
                .append('\n');
      }

      return null;
    }

    @Override
    protected Void visitSelect(final Select node, final Integer indent) {
      append(indent, "SELECT");

      final List<SelectItem> selectItems = node.getSelectItems();

      if (selectItems.size() > 1) {
        boolean first = true;
        for (final SelectItem item : selectItems) {
          builder.append(first ? "" : ",")
                 .append("\n  ")
                 .append(indentString(indent));

          process(item, indent);
          first = false;
        }
      } else {
        builder.append(' ');
        process(getOnlyElement(selectItems), indent);
      }

      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitSingleColumn(final SingleColumn node, final Integer indent) {
      builder.append(formatExpression(node.getExpression()));
      if (node.getAlias().isPresent()) {
        builder.append(' ')
            // for backwards compatibility, we always quote with `""` here
            .append(node.getAlias().get().toString(FormatOptions.of(IdentifierUtil::needsQuotes)));
      }
      return null;
    }

    @Override
    protected Void visitAllColumns(final AllColumns node, final Integer context) {
      node.getSource()
          .ifPresent(source ->
              builder.append(escapedName(source))
                  .append("."));

      builder.append("*");

      return null;
    }

    @Override
    protected Void visitStructAll(final StructAll node, final Integer context) {
      builder.append(node.getBaseStruct() + KsqlConstants.STRUCT_FIELD_REF + "*");
      return null;
    }

    @Override
    protected Void visitTable(final Table node, final Integer indent) {
      builder.append(escapedName(node.getName()));
      return null;
    }

    @Override
    protected Void visitJoin(final Join join, final Integer indent) {
      process(join.getLeft(), indent);
      join.getRights().forEach(node -> process(node, indent));
      return null;
    }

    @Override
    protected Void visitJoinedSource(final JoinedSource node, final Integer indent) {
      final String type = node.getType().getFormatted();

      builder.append('\n');
      append(indent, type).append(" JOIN ");

      process(node.getRelation(), indent);

      final JoinCriteria criteria = node.getCriteria();

      node.getWithinExpression().map((e) -> builder.append(e.toString()));
      final JoinOn on = (JoinOn) criteria;
      builder.append(" ON (")
          .append(formatExpression(on.getExpression()))
          .append(")");

      return null;
    }

    @Override
    protected Void visitAliasedRelation(final AliasedRelation node, final Integer indent) {
      process(node.getRelation(), indent);

      builder.append(' ')
              .append(escapedName(node.getAlias()));

      return null;
    }

    @Override
    protected Void visitCreateStream(final CreateStream node, final Integer indent) {
      formatCreate(node, "STREAM");
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      formatCreate(node, "TABLE");
      return null;
    }

    @Override
    public Void visitAssertStream(final AssertStream node, final Integer context) {
      formatAssertSource(node.getStatement(), "STREAM");
      return null;
    }

    @Override
    public Void visitAssertTable(final AssertTable node, final Integer context) {
      formatAssertSource(node.getStatement(), "TABLE");
      return null;
    }

    @Override
    protected Void visitExplain(final Explain node, final Integer indent) {
      builder.append("EXPLAIN ");
      builder.append("\n");

      node.getQueryId().ifPresent(queryId -> append(indent, queryId));

      node.getStatement().ifPresent(stmt -> process(stmt, indent));

      return null;
    }

    @Override
    protected Void visitShowColumns(final ShowColumns node, final Integer context) {
      builder.append("DESCRIBE ")
              .append(escapedName(node.getTable()));
      return null;
    }

    @Override
    protected Void visitShowFunctions(final ListFunctions node, final Integer context) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitCreateStreamAsSelect(
        final CreateStreamAsSelect node,
        final Integer indent
    ) {
      formatCreateAs("STREAM", node, indent);
      return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(
        final CreateTableAsSelect node,
        final Integer indent
    ) {
      formatCreateAs("TABLE", node, indent);
      return null;
    }

    @Override
    protected Void visitInsertInto(final InsertInto node, final Integer indent) {
      builder.append("INSERT INTO ");
      builder.append(escapedName(node.getTarget()));
      builder.append(" ");

      final String insertProps = node.getProperties().toString();
      if (!insertProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(insertProps)
            .append(")");
        builder.append(" ");
      }

      process(node.getQuery(), indent);
      return null;
    }

    @Override
    protected Void visitDropStream(final DropStream node, final Integer context) {
      visitDrop(node, "STREAM");
      return null;
    }

    @Override
    protected Void visitInsertValues(final InsertValues node, final Integer context) {
      builder.append("INSERT INTO ");
      builder.append(escapedName(node.getTarget()));
      builder.append(" ");

      visitColumns(node.getColumns());

      builder.append("VALUES ");

      visitExpressionList(node.getValues());

      return null;
    }

    private void visitColumns(final List<ColumnName> columns) {
      if (!columns.isEmpty()) {
        builder.append(columns
            .stream()
            .map(SqlFormatter::escapedName)
            .collect(Collectors.joining(", ", "(", ") ")));
      }
    }

    private void visitExpressionList(final List<Expression> expressions) {
      builder.append("(");
      builder.append(
          expressions
              .stream()
              .map(SqlFormatter::formatExpression)
              .collect(Collectors.joining(", ")));
      builder.append(")");
    }

    @Override
    public Void visitAssertValues(final AssertValues node, final Integer context) {
      builder.append("ASSERT VALUES ");
      builder.append(escapedName(node.getStatement().getTarget()));
      builder.append(" ");

      visitColumns(node.getStatement().getColumns());

      builder.append("VALUES ");

      visitExpressionList(node.getStatement().getValues());

      return null;
    }

    @Override
    public Void visitAssertTombstone(final AssertTombstone node, final Integer context) {
      builder.append("ASSERT NULL VALUES ");
      builder.append(escapedName(node.getStatement().getTarget()));
      builder.append(" ");

      visitColumns(node.getStatement().getColumns());

      builder.append("KEY ");

      visitExpressionList(node.getStatement().getValues());

      return null;
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer context) {
      visitDrop(node, "TABLE");
      return null;
    }

    @Override
    protected Void visitPauseQuery(final PauseQuery node, final Integer context) {
      builder.append("PAUSE ");
      builder.append(node.getQueryId().map(QueryId::toString).orElse(PauseQuery.ALL_QUERIES));
      return null;
    }

    @Override
    protected Void visitResumeQuery(final ResumeQuery node, final Integer context) {
      builder.append("RESUME ");
      builder.append(node.getQueryId().map(QueryId::toString).orElse(ResumeQuery.ALL_QUERIES));
      return null;
    }

    @Override
    protected Void visitTerminateQuery(final TerminateQuery node, final Integer context) {
      builder.append("TERMINATE ");
      builder.append(node.getQueryId().map(QueryId::toString).orElse(TerminateQuery.ALL_QUERIES));
      return null;
    }

    @Override
    protected Void visitListStreams(final ListStreams node, final Integer context) {
      builder.append("SHOW STREAMS");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitListTables(final ListTables node, final Integer context) {
      builder.append("SHOW TABLES");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitListConnectorPlugins(
        final ListConnectorPlugins node, final Integer context) {
      builder.append("SHOW CONNECTOR PLUGINS");
      return null;
    }

    @Override
    protected Void visitDescribeStreams(final DescribeStreams node, final Integer context) {
      builder.append("DESCRIBE STREAMS");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitDescribeTables(final DescribeTables node, final Integer context) {
      builder.append("DESCRIBE TABLES");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitUnsetProperty(final UnsetProperty node, final Integer context) {
      builder.append("UNSET '");
      builder.append(node.getPropertyName());
      builder.append("'");

      return null;
    }

    @Override
    protected Void visitSetProperty(final SetProperty node, final Integer context) {
      builder.append("SET '");
      builder.append(node.getPropertyName());
      builder.append("'='");
      builder.append(node.getPropertyValue());
      builder.append("'");

      return null;
    }

    @Override
    protected Void visitAlterSystemProperty(final AlterSystemProperty node, final Integer context) {
      builder.append("ALTER SYSTEM '");
      builder.append(node.getPropertyName());
      builder.append("'='");
      builder.append(node.getPropertyValue());
      builder.append("'");

      return null;
    }

    @Override
    protected Void visitDefineVariable(final DefineVariable node, final Integer context) {
      builder.append("DEFINE ");
      builder.append(node.getVariableName());
      builder.append("='");
      builder.append(node.getVariableValue());
      builder.append("'");

      return null;
    }

    @Override
    public Void visitListVariables(final ListVariables node, final Integer context) {
      builder.append("SHOW VARIABLES");

      return null;
    }

    @Override
    protected Void visitUndefineVariable(final UndefineVariable node, final Integer context) {
      builder.append("UNDEFINE ");
      builder.append(node.getVariableName());

      return null;
    }

    private void visitExtended() {
      builder.append(" EXTENDED");
    }

    @Override
    public Void visitRegisterType(final RegisterType node, final Integer context) {
      builder.append("CREATE TYPE ");
      if (node.getIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(FORMAT_OPTIONS.escape(node.getName()));
      builder.append(" AS ");
      builder.append(formatExpression(node.getType()));
      builder.append(";");
      return null;
    }

    private void visitDrop(final DropStatement node, final String sourceType) {
      builder.append("DROP ");
      builder.append(sourceType);
      builder.append(" ");
      if (node.getIfExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(escapedName(node.getName()));
      if (node.isDeleteTopic()) {
        builder.append(" DELETE TOPIC");
      }
    }

    @Override
    protected Void visitPartitionBy(final PartitionBy node, final Integer indent) {
      final String expressions = node.getExpressions().stream()
          .map(SqlFormatter::formatExpression)
          .collect(Collectors.joining(", "));

      append(indent, "PARTITION BY " + expressions)
          .append('\n');

      return null;
    }

    @Override
    protected Void visitGroupBy(final GroupBy node, final Integer indent) {
      final String expressions = node.getGroupingExpressions().stream()
          .map(SqlFormatter::formatExpression)
          .collect(Collectors.joining(", "));

      append(indent, "GROUP BY " + expressions)
          .append('\n');

      return null;
    }

    @Override
    public Void visitAlterSource(final AlterSource node, final Integer indent) {
      append(indent, String.format(
          "ALTER %s %s%n",
          node.getDataSourceType().getKsqlType(),
          node.getName().text()));

      builder.append(
          node.getAlterOptions()
              .stream()
              .map(SqlFormatter::formatAlterOption)
              .collect(Collectors.joining(",\n"))
      );
      builder.append(";");
      return null;
    }

    private void processRelation(final Relation relation, final Integer indent) {
      if (relation instanceof Table) {
        builder.append("TABLE ")
            .append(escapedName(((Table) relation).getName()))
            .append('\n');
      } else {
        process(relation, indent);
      }
    }

    private StringBuilder append(final int indent, final String value) {
      return builder.append(indentString(indent))
              .append(value);
    }

    private static String indentString(final int indent) {
      return Strings.repeat(INDENT, indent);
    }

    private void formatCreate(final CreateSource node, final String type) {
      builder.append("CREATE ");

      if (node.isOrReplace()) {
        builder.append("OR REPLACE ");
      }

      if (node.isSource()) {
        builder.append("SOURCE ");
      }

      builder.append(type);
      builder.append(" ");

      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(escapedName(node.getName()));

      formatTableElements(node.getElements());
      formatTableProperties(node.getProperties());

      builder.append(";");
    }

    private void formatAssertSource(final CreateSource node, final String type) {
      builder.append("ASSERT ");

      builder.append(type);
      builder.append(" ");

      builder.append(escapedName(node.getName()));

      formatTableElements(node.getElements());
      formatTableProperties(node.getProperties());

      builder.append(";");
    }

    private void formatTableElements(final TableElements tableElements) {
      final String elements = tableElements.stream()
          .map(Formatter::formatTableElement)
          .collect(Collectors.joining(", "));

      if (!elements.isEmpty()) {
        builder
            .append(" (")
            .append(elements)
            .append(")");
      }
    }

    private void formatTableProperties(final CreateSourceProperties properties) {
      final String tableProps = properties.toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }
    }

    private void formatCreateAs(
        final String source,
        final CreateAsSelect node,
        final Integer indent
    ) {
      builder.append("CREATE ");

      if (node.isOrReplace()) {
        builder.append("OR REPLACE ");
      }

      builder.append(source);
      builder.append(" ");

      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(escapedName(node.getName()));

      final String tableProps = node.getProperties().toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }

      builder.append(" AS ");

      process(node.getQuery(), indent);
    }

    private static String formatTableElement(final TableElement e) {
      final String postFix;

      final ColumnConstraints columnConstraints = e.getConstraints();
      if (columnConstraints.isPrimaryKey()) {
        postFix = " PRIMARY KEY";
      } else if (columnConstraints.isKey()) {
        postFix = " KEY";
      } else if (columnConstraints.isHeaders()) {
        if (columnConstraints.getHeaderKey().isPresent()) {
          postFix = " HEADER('" + e.getConstraints().getHeaderKey().get() + "')";
        } else {
          postFix = " HEADERS";
        }
      } else {
        postFix = "";
      }

      return escapedName(e.getName())
          + " "
          + ExpressionFormatter.formatExpression(
          e.getType(), FormatOptions.of(IdentifierUtil::needsQuotes))
          + postFix;
    }
  }

  private static String formatExpression(final Expression expression) {
    return ExpressionFormatter.formatExpression(
        expression,
        FormatOptions.of(IdentifierUtil::needsQuotes)
    );
  }

  private static String escapedName(final Name<?> name) {
    return name.toString(FORMAT_OPTIONS);
  }

  private static String formatAlterOption(final AlterOption option) {
    return "ADD COLUMN " + option.getColumnName() + " " + option.getType();
  }
}
