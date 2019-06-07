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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.util.stream.Collectors.joining;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateView;
import io.confluent.ksql.parser.tree.Delete;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropView;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ExplainFormat;
import io.confluent.ksql.parser.tree.ExplainOption;
import io.confluent.ksql.parser.tree.ExplainType;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RenameColumn;
import io.confluent.ksql.parser.tree.RenameTable;
import io.confluent.ksql.parser.tree.SampledRelation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetSession;
import io.confluent.ksql.parser.tree.ShowCatalogs;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.ShowCreate;
import io.confluent.ksql.parser.tree.ShowFunctions;
import io.confluent.ksql.parser.tree.ShowPartitions;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.ParserUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class SqlFormatter {

  private static final String INDENT = "   ";
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

  private SqlFormatter() {
  }

  public static String formatSql(final Node root) {
    final StringBuilder builder = new StringBuilder();
    new Formatter(builder, true).process(root, 0);
    return builder.toString();
  }

  public static String formatSql(final Node root, final boolean unmangleNames) {
    final StringBuilder builder = new StringBuilder();
    new Formatter(builder, unmangleNames).process(root, 0);
    return builder.toString();
  }

  private static class Formatter
          extends AstVisitor<Void, Integer> {

    private final StringBuilder builder;
    private final boolean unmangledNames;

    Formatter(final StringBuilder builder, final boolean unmangleNames) {
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

      processRelation(node.getQueryBody(), indent);

      if (node.getLimit().isPresent()) {
        append(indent, "LIMIT " + node.getLimit().get())
                .append('\n');
      }

      return null;
    }

    @Override
    protected Void visitQuerySpecification(final QuerySpecification node, final Integer indent) {
      process(node.getSelect(), indent);

      append(indent, "FROM ");
      processRelation(node.getFrom(), indent);
      builder.append('\n');
      append(indent, "  ");

      builder.append('\n');

      if (node.getWhere().isPresent()) {
        append(indent, "WHERE " + ExpressionFormatter.formatExpression(node.getWhere().get()))
                .append('\n');
      }

      if (node.getGroupBy().isPresent()) {
        append(indent, "GROUP BY "
                + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "")
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
        append(indent, "LIMIT " + node.getLimit().get())
                .append('\n');
      }
      return null;
    }

    @Override
    protected Void visitSelect(final Select node, final Integer indent) {
      append(indent, "SELECT");
      if (node.isDistinct()) {
        builder.append(" DISTINCT");
      }

      if (node.getSelectItems().size() > 1) {
        boolean first = true;
        for (final SelectItem item : node.getSelectItems()) {
          builder.append("\n")
                  .append(indentString(indent))
                  .append(first ? "  " : ", ");

          process(item, indent);
          first = false;
        }
      } else {
        builder.append(' ');
        process(getOnlyElement(node.getSelectItems()), indent);
      }

      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitSingleColumn(final SingleColumn node, final Integer indent) {
      builder.append(ExpressionFormatter.formatExpression(node.getExpression()));
      if (node.getAlias().isPresent()) {
        builder.append(' ')
                .append('"')
                .append(node.getAlias().get())
                .append('"'); // TODO: handle quoting properly
      }

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
      final String type = node.getFormattedType();
      process(node.getLeft(), indent);

      builder.append('\n');
      append(indent, type).append(" JOIN ");

      process(node.getRight(), indent);

      final JoinCriteria criteria = node.getCriteria().orElseThrow(() ->
          new KsqlException("Join criteria is missing")
      );

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

      appendAliasColumns(builder, node.getColumnNames());

      return null;
    }

    @Override
    protected Void visitSampledRelation(final SampledRelation node, final Integer indent) {
      process(node.getRelation(), indent);

      builder.append(" TABLESAMPLE ")
              .append(node.getType())
              .append(" (")
              .append(node.getSamplePercentage())
              .append(')');

      if (node.getColumnsToStratifyOn().isPresent()) {
        builder.append(" STRATIFY ON ")
                .append(" (")
                .append(Joiner.on(",").join(node.getColumnsToStratifyOn().get()));
        builder.append(')');
      }

      return null;
    }

    @Override
    protected Void visitValues(final Values node, final Integer indent) {
      builder.append(" VALUES ");

      boolean first = true;
      for (final Expression row : node.getRows()) {
        builder.append("\n")
                .append(indentString(indent))
                .append(first ? "  " : ", ");

        builder.append(ExpressionFormatter.formatExpression(row));
        first = false;
      }
      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitTableSubquery(final TableSubquery node, final Integer indent) {
      builder.append('(')
              .append('\n');

      process(node.getQuery(), indent + 1);

      append(indent, ") ");

      return null;
    }


    @Override
    protected Void visitCreateStream(final CreateStream node, final Integer indent) {
      builder.append("CREATE STREAM ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getName())
          .append(" \n");
      if (!node.getElements().isEmpty()) {
        builder.append("(");
        boolean addComma = false;
        for (final TableElement tableElement : node.getElements()) {
          if (addComma) {
            builder.append(", ");
          } else {
            addComma = true;
          }
          builder.append(ParserUtil.escapeIfLiteral(tableElement.getName()))
              .append(" ")
              .append(tableElement.getType());
        }
        builder.append(")");
      }
      if (!node.getProperties().isEmpty()) {
        builder.append(" WITH (");
        boolean addComma = false;
        for (final Map.Entry property: node.getProperties().entrySet()) {
          if (addComma) {
            builder.append(", ");
          } else {
            addComma = true;
          }
          builder.append(property.getKey().toString()).append("=").append(property.getValue()
                                                                              .toString());
        }
        builder.append(");");
      }
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getName())
          .append(" ");
      if (!node.getElements().isEmpty()) {
        builder.append("(");
        boolean addComma = false;
        for (final TableElement tableElement: node.getElements()) {
          if (addComma) {
            builder.append(", ");
          } else {
            addComma = true;
          }
          builder.append(ParserUtil.escapeIfLiteral(tableElement.getName()))
              .append(" ")
              .append(tableElement.getType());
        }
        builder.append(")").append(" WITH (");
        addComma = false;
        for (final Map.Entry property: node.getProperties().entrySet()) {
          if (addComma) {
            builder.append(", ");
          } else {
            addComma = true;
          }
          builder.append(property.getKey().toString()).append("=").append(property.getValue()
                                                                              .toString());
        }
        builder.append(");");
      }
      return null;
    }

    @Override
    protected Void visitCreateView(final CreateView node, final Integer indent) {
      builder.append("CREATE ");
      if (node.isReplace()) {
        builder.append("OR REPLACE ");
      }
      builder.append("VIEW ")
              .append(node.getName())
              .append(" AS\n");

      process(node.getQuery(), indent);

      return null;
    }

    @Override
    protected Void visitDropView(final DropView node, final Integer context) {
      builder.append("DROP VIEW ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getName());

      return null;
    }

    @Override
    protected Void visitExplain(final Explain node, final Integer indent) {
      builder.append("EXPLAIN ");
      if (node.isAnalyze()) {
        builder.append("ANALYZE ");
      }

      final List<String> options = new ArrayList<>();

      for (final ExplainOption option : node.getOptions()) {
        if (option instanceof ExplainType) {
          options.add("TYPE " + ((ExplainType) option).getType());
        } else if (option instanceof ExplainFormat) {
          options.add("FORMAT " + ((ExplainFormat) option).getType());
        } else {
          throw new UnsupportedOperationException("unhandled explain option: " + option);
        }
      }

      if (!options.isEmpty()) {
        builder.append("(");
        Joiner.on(", ").appendTo(builder, options);
        builder.append(")");
      }

      builder.append("\n");

      process(node.getStatement(), indent);

      return null;
    }

    @Override
    protected Void visitShowCatalogs(final ShowCatalogs node, final Integer context) {
      builder.append("SHOW CATALOGS");

      node.getLikePattern().ifPresent((value) ->
              builder.append(" LIKE ")
                      .append(
                              ExpressionFormatter.formatStringLiteral(value)));

      return null;
    }

    @Override
    protected Void visitShowCreate(final ShowCreate node, final Integer context) {
      if (node.getType() == ShowCreate.Type.TABLE) {
        builder.append("SHOW CREATE TABLE ")
                .append(formatName(node.getName()));
      } else if (node.getType() == ShowCreate.Type.VIEW) {
        builder.append("SHOW CREATE VIEW ")
                .append(formatName(node.getName()));
      }

      return null;
    }

    @Override
    protected Void visitShowColumns(final ShowColumns node, final Integer context) {
      builder.append("SHOW COLUMNS FROM ")
              .append(node.getTable());

      return null;
    }

    @Override
    protected Void visitShowPartitions(final ShowPartitions node, final Integer context) {
      builder.append("SHOW PARTITIONS FROM ")
              .append(node.getTable());

      if (node.getWhere().isPresent()) {
        builder.append(" WHERE ")
                .append(ExpressionFormatter.formatExpression(node.getWhere().get()));
      }

      if (node.getLimit().isPresent()) {
        builder.append(" LIMIT ")
                .append(node.getLimit().get());
      }

      return null;
    }

    @Override
    protected Void visitShowFunctions(final ShowFunctions node, final Integer context) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitDelete(final Delete node, final Integer context) {
      builder.append("DELETE FROM ")
              .append(node.getTable().getName());

      if (node.getWhere().isPresent()) {
        builder.append(" WHERE ")
                .append(ExpressionFormatter.formatExpression(node.getWhere().get()));
      }

      return null;
    }

    @Override
    protected Void visitCreateStreamAsSelect(
        final CreateStreamAsSelect node, final Integer indent) {
      builder.append("CREATE STREAM ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getName());

      if (!node.getProperties().isEmpty()) {
        builder.append(" WITH (");
        Joiner.on(", ")
            .appendTo(builder, transform(
                node.getProperties().entrySet(), entry -> entry.getKey() + " = "
                                                          + ExpressionFormatter
                                                              .formatExpression(entry.getValue())));
        builder.append(")");
      }

      builder.append(" AS ");
      process(node.getQuery(), indent);
      return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(final CreateTableAsSelect node, final Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getName());

      if (!node.getProperties().isEmpty()) {
        builder.append(" WITH (");
        Joiner.on(", ")
                .appendTo(builder, transform(
                        node.getProperties().entrySet(), entry -> entry.getKey() + " = "
                                + ExpressionFormatter
                                .formatExpression(entry.getValue())));
        builder.append(")");
      }

      builder.append(" AS ");
      process(node.getQuery(), indent);
      return null;
    }



    private static String formatName(final String name) {
      if (NAME_PATTERN.matcher(name).matches()) {
        return name;
      }
      return "\"" + name + "\"";
    }

    private static String formatName(final QualifiedName name) {
      return name.getParts().stream()
              .map(Formatter::formatName)
              .collect(joining("."));
    }

    @Override
    protected Void visitInsertInto(final InsertInto node, final Integer indent) {
      builder.append("INSERT INTO ");
      builder.append(node.getTarget());
      process(node.getQuery(), indent);
      return null;
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer context) {
      builder.append("DROP TABLE ");
      if (node.getIfExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getName());

      return null;
    }

    @Override
    protected Void visitRenameTable(final RenameTable node, final Integer context) {
      builder.append("ALTER TABLE ")
              .append(node.getSource())
              .append(" RENAME TO ")
              .append(node.getTarget());

      return null;
    }

    @Override
    protected Void visitRenameColumn(final RenameColumn node, final Integer context) {
      builder.append("ALTER TABLE ")
              .append(node.getTable())
              .append(" RENAME COLUMN ")
              .append(node.getSource())
              .append(" TO ")
              .append(node.getTarget());

      return null;
    }

    @Override
    public Void visitSetSession(final SetSession node, final Integer context) {
      builder.append("SET SESSION ")
              .append(node.getName())
              .append(" = ")
              .append(ExpressionFormatter.formatExpression(node.getValue()));

      return null;
    }


    private void processRelation(final Relation relation, final Integer indent) {
      // TODO: handle this properly
      if (relation instanceof Table) {
        builder.append("TABLE ")
                .append(((Table) relation).getName())
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
  }

  private static void appendAliasColumns(final StringBuilder builder, final List<String> columns) {
    if ((columns != null) && (!columns.isEmpty())) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, columns);
      builder.append(')');
    }
  }
}
