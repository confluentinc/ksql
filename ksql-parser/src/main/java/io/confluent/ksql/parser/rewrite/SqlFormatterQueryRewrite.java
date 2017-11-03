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

package io.confluent.ksql.parser.rewrite;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;

import io.confluent.ksql.parser.ExpressionFormatter;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.CreateView;
import io.confluent.ksql.parser.tree.Delete;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.DropView;
import io.confluent.ksql.parser.tree.Except;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ExplainFormat;
import io.confluent.ksql.parser.tree.ExplainOption;
import io.confluent.ksql.parser.tree.ExplainType;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Intersect;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.JoinUsing;
import io.confluent.ksql.parser.tree.NaturalJoin;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Relation;
import io.confluent.ksql.parser.tree.RenameColumn;
import io.confluent.ksql.parser.tree.RenameTable;
import io.confluent.ksql.parser.tree.Row;
import io.confluent.ksql.parser.tree.SampledRelation;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetSession;
import io.confluent.ksql.parser.tree.ShowCatalogs;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.parser.tree.ShowCreate;
import io.confluent.ksql.parser.tree.ShowFunctions;
import io.confluent.ksql.parser.tree.ShowPartitions;
import io.confluent.ksql.parser.tree.ShowSchemas;
import io.confluent.ksql.parser.tree.ShowSession;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableSubquery;
import io.confluent.ksql.parser.tree.Union;
import io.confluent.ksql.parser.tree.Values;
import io.confluent.ksql.parser.tree.With;
import io.confluent.ksql.parser.tree.WithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.util.stream.Collectors.joining;

public final class SqlFormatterQueryRewrite {

  private static final String INDENT = "   ";
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

  private SqlFormatterQueryRewrite() {
  }

  public static String formatSql(Node root) {
    StringBuilder builder = new StringBuilder();
    new Formatter(builder, true).process(root, 0);
    return builder.toString();
  }

  private static class Formatter
      extends AstVisitor<Void, Integer> {

    private final StringBuilder builder;
    private final boolean unmangledNames;

    public Formatter(StringBuilder builder, boolean unmangleNames) {
      this.builder = builder;
      this.unmangledNames = unmangleNames;
    }

    @Override
    protected Void visitNode(Node node, Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitExpression(Expression node, Integer indent) {
      checkArgument(indent == 0,
                    "visitExpression should only be called at root");
      builder.append(ExpressionFormatter.formatExpression(node, unmangledNames));
      return null;
    }

    @Override
    protected Void visitQuery(Query node, Integer indent) {
      if (node.getWith().isPresent()) {
        With with = node.getWith().get();
        append(indent, "WITH");
        if (with.isRecursive()) {
          builder.append(" RECURSIVE");
        }
        builder.append("\n  ");
        Iterator<WithQuery> queries = with.getQueries().iterator();
        while (queries.hasNext()) {
          WithQuery query = queries.next();
          append(indent, query.getName());
          query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
          builder.append(" AS ");
          process(new TableSubquery(query.getQuery()), indent);
          builder.append('\n');
          if (queries.hasNext()) {
            builder.append(", ");
          }
        }
      }

      processRelation(node.getQueryBody(), indent);

      if (!node.getOrderBy().isEmpty()) {
        append(indent,
               "ORDER BY "
               + ExpressionFormatter.formatSortItems(node.getOrderBy()))
            .append('\n');
      }

      if (node.getLimit().isPresent()) {
        append(indent, "LIMIT " + node.getLimit().get())
            .append('\n');
      }
      return null;
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
      process(node.getSelect(), indent);
      append(indent, "INTO");
      builder.append('\n');
      append(indent, "  ");
      process(node.getInto(), indent);
      builder.append('\n');

      append(indent, "FROM");
      builder.append('\n');
      append(indent, "  ");
      process(node.getFrom(), indent);

      builder.append('\n');

      if (node.getWhere().isPresent()) {
        append(indent,
               "WHERE "
               + ExpressionFormatter.formatExpression(node.getWhere().get()))
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
        append(indent,
               "HAVING "
               + ExpressionFormatter.formatExpression(node.getHaving().get()))
            .append('\n');
      }

      if (!node.getOrderBy().isEmpty()) {
        append(indent,
               "ORDER BY "
               + ExpressionFormatter.formatSortItems(node.getOrderBy()))
            .append('\n');
      }

      if (node.getLimit().isPresent()) {
        append(indent, "LIMIT " + node.getLimit().get())
            .append('\n');
      }
      return null;
    }

    @Override
    protected Void visitSelect(Select node, Integer indent) {
      append(indent, "SELECT");
      if (node.isDistinct()) {
        builder.append(" DISTINCT");
      }

      if (node.getSelectItems().size() > 1) {
        boolean first = true;
        for (SelectItem item : node.getSelectItems()) {
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
    protected Void visitSingleColumn(SingleColumn node, Integer indent) {
      builder.append(ExpressionFormatter.formatExpression(node.getExpression()));
      if (node.getAlias().isPresent()) {
        builder.append(' ')
            .append(" AS ")
            .append(node.getAlias().get());
        // TODO: handle quoting properly
      }

      return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Integer context) {
      builder.append(node.toString());

      return null;
    }

    @Override
    protected Void visitTable(Table node, Integer indent) {
      builder.append(node.getName().toString());
      return null;
    }

    @Override
    protected Void visitJoin(Join node, Integer indent) {
      JoinCriteria criteria = node.getCriteria().orElse(null);
      String type = node.getType().toString();
      if (criteria instanceof NaturalJoin) {
        type = "NATURAL " + type;
      }

      process(node.getLeft(), indent);

      builder.append('\n');
      if (node.getType() == Join.Type.IMPLICIT) {
        append(indent, ", ");
      } else {
        append(indent, type).append(" JOIN ");
      }

      process(node.getRight(), indent);

      if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
        if (criteria instanceof JoinUsing) {
          JoinUsing using = (JoinUsing) criteria;
          builder.append(" USING (")
              .append(Joiner.on(", ").join(using.getColumns()))
              .append(")");
        } else if (criteria instanceof JoinOn) {
          JoinOn on = (JoinOn) criteria;
          builder.append(" ON (")
              .append(ExpressionFormatter.formatExpression(on.getExpression()))
              .append(")");
        } else if (!(criteria instanceof NaturalJoin)) {
          throw new UnsupportedOperationException("unknown join criteria: " + criteria);
        }
      }

      return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
      process(node.getRelation(), indent);

      builder.append(' ')
          .append(node.getAlias());

      appendAliasColumns(builder, node.getColumnNames());

      return null;
    }

    @Override
    protected Void visitSampledRelation(SampledRelation node, Integer indent) {
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
    protected Void visitValues(Values node, Integer indent) {
      builder.append(" VALUES ");

      boolean first = true;
      for (Expression row : node.getRows()) {
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
    protected Void visitTableSubquery(TableSubquery node, Integer indent) {
      builder.append('(')
          .append('\n');

      process(node.getQuery(), indent + 1);

      append(indent, ") ");

      return null;
    }

    @Override
    protected Void visitUnion(Union node, Integer indent) {
      Iterator<Relation> relations = node.getRelations().iterator();

      while (relations.hasNext()) {
        processRelation(relations.next(), indent);

        if (relations.hasNext()) {
          builder.append("UNION ");
          if (!node.isDistinct()) {
            builder.append("ALL ");
          }
        }
      }

      return null;
    }

    @Override
    protected Void visitExcept(Except node, Integer indent) {
      processRelation(node.getLeft(), indent);

      builder.append("EXCEPT ");
      if (!node.isDistinct()) {
        builder.append("ALL ");
      }

      processRelation(node.getRight(), indent);

      return null;
    }

    @Override
    protected Void visitIntersect(Intersect node, Integer indent) {
      Iterator<Relation> relations = node.getRelations().iterator();

      while (relations.hasNext()) {
        processRelation(relations.next(), indent);

        if (relations.hasNext()) {
          builder.append("INTERSECT ");
          if (!node.isDistinct()) {
            builder.append("ALL ");
          }
        }
      }

      return null;
    }

    @Override
    protected Void visitCreateView(CreateView node, Integer indent) {
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
    protected Void visitDropView(DropView node, Integer context) {
      builder.append("DROP VIEW ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getName());

      return null;
    }

    @Override
    protected Void visitExplain(Explain node, Integer indent) {
      builder.append("EXPLAIN ");
      if (node.isAnalyze()) {
        builder.append("ANALYZE ");
      }

      List<String> options = new ArrayList<>();

      for (ExplainOption option : node.getOptions()) {
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
    protected Void visitShowCatalogs(ShowCatalogs node, Integer context) {
      builder.append("SHOW CATALOGS");

      node.getLikePattern().ifPresent((value) ->
                                          builder.append(" LIKE ")
                                              .append(ExpressionFormatter
                                                          .formatStringLiteral(value)));

      return null;
    }

    @Override
    protected Void visitShowSchemas(ShowSchemas node, Integer context) {
      builder.append("SHOW SCHEMAS");

      if (node.getCatalog().isPresent()) {
        builder.append(" FROM ")
            .append(node.getCatalog().get());
      }

      node.getLikePattern().ifPresent((value) ->
                                          builder.append(" LIKE ")
                                              .append(ExpressionFormatter
                                                          .formatStringLiteral(value)));

      return null;
    }

    @Override
    protected Void visitShowCreate(ShowCreate node, Integer context) {
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
    protected Void visitShowColumns(ShowColumns node, Integer context) {
      builder.append("SHOW COLUMNS FROM ")
          .append(node.getTable());

      return null;
    }

    @Override
    protected Void visitShowPartitions(ShowPartitions node, Integer context) {
      builder.append("SHOW PARTITIONS FROM ")
          .append(node.getTable());

      if (node.getWhere().isPresent()) {
        builder.append(" WHERE ")
            .append(ExpressionFormatter.formatExpression(node.getWhere().get()));
      }

      if (!node.getOrderBy().isEmpty()) {
        builder.append(" ORDER BY ")
            .append(ExpressionFormatter.formatSortItems(node.getOrderBy()));
      }

      if (node.getLimit().isPresent()) {
        builder.append(" LIMIT ")
            .append(node.getLimit().get());
      }

      return null;
    }

    @Override
    protected Void visitShowFunctions(ShowFunctions node, Integer context) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitShowSession(ShowSession node, Integer context) {
      builder.append("SHOW SESSION");

      return null;
    }

    @Override
    protected Void visitDelete(Delete node, Integer context) {
      builder.append("DELETE FROM ")
          .append(node.getTable().getName());

      if (node.getWhere().isPresent()) {
        builder.append(" WHERE ")
            .append(ExpressionFormatter.formatExpression(node.getWhere().get()));
      }

      return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getName());

      if (!node.getProperties().isEmpty()) {
        builder.append(" WITH (");
        Joiner.on(", ").appendTo(builder,
                      transform(node.getProperties().entrySet(), entry -> entry.getKey()
                              + " = "
                              + ExpressionFormatter.formatExpression(entry.getValue())));
        builder.append(")");
      }

      builder.append(" AS ");
      process(node.getQuery(), indent);

      return null;
    }

    @Override
    protected Void visitCreateTable(CreateTable node, Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      String tableName = formatName(node.getName());
      builder.append(tableName).append(" (\n");

      String columnList = node.getElements().stream()
          .map(column -> INDENT + formatName(column.getName()) + " " + column.getType())
          .collect(joining(",\n"));
      builder.append(columnList);
      builder.append("\n").append(")");

      if (!node.getProperties().isEmpty()) {
        builder.append("\nWITH (\n");
        // Always output the table properties in sorted order
        String propertyList = ImmutableSortedMap.copyOf(node.getProperties()).entrySet().stream()
            .map(entry -> INDENT + formatName(entry.getKey()) + " = " + entry.getValue())
            .collect(joining(",\n"));
        builder.append(propertyList);
        builder.append("\n").append(")");
      }

      return null;
    }

    private static String formatName(String name) {
      if (NAME_PATTERN.matcher(name).matches()) {
        return name;
      }
      return "\"" + name + "\"";
    }

    private static String formatName(QualifiedName name) {
      return name.getParts().stream()
          .map(Formatter::formatName)
          .collect(joining("."));
    }

    @Override
    protected Void visitDropTable(DropTable node, Integer context) {
      builder.append("DROP TABLE ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getName());

      return null;
    }

    @Override
    protected Void visitRenameTable(RenameTable node, Integer context) {
      builder.append("ALTER TABLE ")
          .append(node.getSource())
          .append(" RENAME TO ")
          .append(node.getTarget());

      return null;
    }

    @Override
    protected Void visitRenameColumn(RenameColumn node, Integer context) {
      builder.append("ALTER TABLE ")
          .append(node.getTable())
          .append(" RENAME COLUMN ")
          .append(node.getSource())
          .append(" TO ")
          .append(node.getTarget());

      return null;
    }



    @Override
    public Void visitSetSession(SetSession node, Integer context) {
      builder.append("SET SESSION ")
          .append(node.getName())
          .append(" = ")
          .append(ExpressionFormatter.formatExpression(node.getValue()));

      return null;
    }

    @Override
    protected Void visitRow(Row node, Integer indent) {
      builder.append("ROW(");
      boolean firstItem = true;
      for (Expression item : node.getItems()) {
        if (!firstItem) {
          builder.append(", ");
        }
        process(item, indent);
        firstItem = false;
      }
      builder.append(")");
      return null;
    }

    private void processRelation(Relation relation, Integer indent) {
      // TODO: handle this properly
      if (relation instanceof Table) {
        builder.append("TABLE ")
            .append(((Table) relation).getName())
            .append('\n');
      } else {
        process(relation, indent);
      }
    }

    private StringBuilder append(int indent, String value) {
      return builder.append(indentString(indent))
          .append(value);
    }

    private static String indentString(int indent) {
      return Strings.repeat(INDENT, indent);
    }
  }

  private static void appendAliasColumns(StringBuilder builder, List<String> columns) {
    if ((columns != null) && (!columns.isEmpty())) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, columns);
      builder.append(')');
    }
  }
}