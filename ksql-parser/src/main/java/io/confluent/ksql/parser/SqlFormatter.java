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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.base.Strings;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.util.ParserUtil;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class SqlFormatter {

  private static final String INDENT = "   ";
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

  private SqlFormatter() {
  }

  public static String formatSql(final Node root) {
    return formatSql(root, true);
  }

  public static String formatSql(final Node root, final boolean unmangleNames) {
    final StringBuilder builder = new StringBuilder();
    new Formatter(builder, unmangleNames).process(root, 0);
    return StringUtils.stripEnd(builder.toString(), "\n");
  }

  private static final class Formatter extends AstVisitor<Void, Integer> {

    private final StringBuilder builder;
    private final boolean unmangledNames;

    private Formatter(final StringBuilder builder, final boolean unmangleNames) {
      this.builder = builder;
      this.unmangledNames = unmangleNames;
    }

    @Override
    protected Void visitNode(final Node node, final Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitExpression(final Expression node, final Integer indent) {
      checkArgument(indent == 0,
              "visitExpression should only be called at root");
      builder.append(ExpressionFormatter.formatExpression(node, unmangledNames));
      return null;
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
        append(indent, "WHERE " + ExpressionFormatter.formatExpression(node.getWhere().get()))
            .append('\n');
      }

      if (node.getGroupBy().isPresent()) {
        append(indent, "GROUP BY "
            + ExpressionFormatter
            .formatGroupBy(node.getGroupBy().get().getGroupingElements()))
            .append('\n');
      }

      if (node.getHaving().isPresent()) {
        append(indent, "HAVING "
            + ExpressionFormatter.formatExpression(node.getHaving().get()))
            .append('\n');
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
      builder.append(ExpressionFormatter.formatExpression(node.getExpression()));
      builder.append(' ')
                .append('"')
                .append(node.getAlias())
                .append('"');
      return null;
    }

    @Override
    protected Void visitAllColumns(final AllColumns node, final Integer context) {
      builder.append(node.toString());

      return null;
    }

    @Override
    protected Void visitTable(final Table node, final Integer indent) {
      builder.append(node.getName().toString());
      return null;
    }

    @Override
    protected Void visitJoin(final Join node, final Integer indent) {
      final String type = node.getType().getFormatted();
      process(node.getLeft(), indent);

      builder.append('\n');
      append(indent, type).append(" JOIN ");

      process(node.getRight(), indent);

      final JoinCriteria criteria = node.getCriteria();

      node.getWithinExpression().map((e) -> builder.append(e.toString()));
      final JoinOn on = (JoinOn) criteria;
      builder.append(" ON (")
          .append(ExpressionFormatter.formatExpression(on.getExpression()))
          .append(")");

      return null;
    }

    @Override
    protected Void visitAliasedRelation(final AliasedRelation node, final Integer indent) {
      process(node.getRelation(), indent);

      builder.append(' ')
              .append(node.getAlias());

      return null;
    }

    @Override
    protected Void visitCreateStream(final CreateStream node, final Integer indent) {
      builder.append("CREATE STREAM ");
      formatCreate(node);
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      builder.append("CREATE TABLE ");
      formatCreate(node);
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
              .append(node.getTable());
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
      builder.append("CREATE STREAM ");
      formatCreateAs(node, indent);
      return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(
        final CreateTableAsSelect node,
        final Integer indent
    ) {
      builder.append("CREATE TABLE ");
      formatCreateAs(node, indent);
      return null;
    }

    private static String formatName(final String name) {
      if (NAME_PATTERN.matcher(name).matches()) {
        return name;
      }
      return "\"" + name + "\"";
    }

    @Override
    protected Void visitInsertInto(final InsertInto node, final Integer indent) {
      builder.append("INSERT INTO ");
      builder.append(node.getTarget());
      builder.append(" ");
      process(node.getQuery(), indent);
      processPartitionBy(node.getPartitionByColumn(), indent);
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
      builder.append(node.getTarget());
      builder.append(" ");

      if (!node.getColumns().isEmpty()) {
        builder.append(node.getColumns().stream().collect(Collectors.joining(", ", "(", ") ")));
      }

      builder.append("VALUES ");

      builder.append("(");
      builder.append(
          node.getValues()
              .stream()
              .map(SqlFormatter::formatSql)
              .collect(Collectors.joining(", ")));
      builder.append(")");

      return null;
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer context) {
      visitDrop(node, "TABLE");
      return null;
    }

    private void visitDrop(final DropStatement node, final String sourceType) {
      builder.append("DROP ");
      builder.append(sourceType);
      builder.append(" ");
      if (node.getIfExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getName());
      if (node.isDeleteTopic()) {
        builder.append(" DELETE TOPIC");
      }
    }

    private void processRelation(final Relation relation, final Integer indent) {
      if (relation instanceof Table) {
        builder.append("TABLE ")
                .append(((Table) relation).getName())
                .append('\n');
      } else {
        process(relation, indent);
      }
    }

    private void processPartitionBy(
        final Optional<Expression> partitionByColumn,
        final Integer indent
    ) {
      partitionByColumn.ifPresent(partitionBy -> append(indent, "PARTITION BY "
              + ExpressionFormatter.formatExpression(partitionBy))
          .append('\n'));
    }

    private StringBuilder append(final int indent, final String value) {
      return builder.append(indentString(indent))
              .append(value);
    }

    private static String indentString(final int indent) {
      return Strings.repeat(INDENT, indent);
    }

    private void formatCreate(final CreateSource node) {
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(node.getName());

      final String elements = node.getElements().stream()
          .map(Formatter::formatTableElement)
          .collect(Collectors.joining(", "));

      if (!elements.isEmpty()) {
        builder
            .append(" (")
            .append(elements)
            .append(")");
      }

      final String tableProps = node.getProperties().toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }

      builder.append(";");
    }

    private void formatCreateAs(final CreateAsSelect node, final Integer indent) {
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(node.getName());

      final String tableProps = node.getProperties().toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }

      builder.append(" AS ");

      process(node.getQuery(), indent);
      processPartitionBy(node.getPartitionByColumn(), indent);
    }

    private static String formatTableElement(final TableElement e) {
      return ParserUtil.escapeIfReservedIdentifier(e.getName())
          + " "
          + e.getType()
          + (e.getNamespace() == Namespace.KEY ? " KEY" : "");
    }
  }
}
