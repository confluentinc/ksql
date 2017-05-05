/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser;

import com.google.common.collect.ImmutableList;
import io.confluent.kql.parser.tree.AliasedRelation;
import io.confluent.kql.parser.tree.AllColumns;
import io.confluent.kql.parser.tree.ComparisonExpression;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.GroupBy;
import io.confluent.kql.parser.tree.LogicalBinaryExpression;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.QualifiedNameReference;
import io.confluent.kql.parser.tree.Query;
import io.confluent.kql.parser.tree.QueryBody;
import io.confluent.kql.parser.tree.QuerySpecification;
import io.confluent.kql.parser.tree.Relation;
import io.confluent.kql.parser.tree.Row;
import io.confluent.kql.parser.tree.SearchedCaseExpression;
import io.confluent.kql.parser.tree.Select;
import io.confluent.kql.parser.tree.SelectItem;
import io.confluent.kql.parser.tree.SingleColumn;
import io.confluent.kql.parser.tree.SortItem;
import io.confluent.kql.parser.tree.StringLiteral;
import io.confluent.kql.parser.tree.Table;
import io.confluent.kql.parser.tree.TableSubquery;
import io.confluent.kql.parser.tree.Values;
import io.confluent.kql.parser.tree.WhenClause;

import java.util.List;
import java.util.Optional;

public final class QueryUtil {

  private QueryUtil() {
  }

  public static Expression nameReference(String name) {
    return new QualifiedNameReference(QualifiedName.of(name));
  }

  public static Select selectList(Expression... expressions) {
    ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
    int index = 0;
    for (Expression expression : expressions) {
      items.add(new SingleColumn(expression));
      index++;
    }
    return new Select(false, items.build());
  }

  public static Select selectList(SelectItem... items) {
    return new Select(false, ImmutableList.copyOf(items));
  }

  public static Select selectAll(List<SelectItem> items) {
    return new Select(false, items);
  }

  public static Table table(QualifiedName name) {
    return new Table(name);
  }

  public static Relation subquery(Query query) {
    return new TableSubquery(query);
  }

  public static SortItem ascending(String name) {
    return new SortItem(nameReference(name), SortItem.Ordering.ASCENDING,
                        SortItem.NullOrdering.UNDEFINED);
  }

  public static Expression logicalAnd(Expression left, Expression right) {
    return new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, left, right);
  }

  public static Expression equal(Expression left, Expression right) {
    return new ComparisonExpression(ComparisonExpression.Type.EQUAL, left, right);
  }

  public static Expression caseWhen(Expression operand, Expression result) {
    return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)),
                                      Optional.empty());
  }

  public static Expression functionCall(String name, Expression... arguments) {
    return new FunctionCall(QualifiedName.of(name), ImmutableList.copyOf(arguments));
  }

  public static Values values(Row... row) {
    return new Values(ImmutableList.copyOf(row));
  }

  public static Row row(Expression... values) {
    return new Row(ImmutableList.copyOf(values));
  }

  public static Relation aliased(Relation relation, String alias, List<String> columnAliases) {
    return new AliasedRelation(relation, alias, columnAliases);
  }

//    public static SelectItem aliasedNullToEmpty(String column, String alias)
//    {
//        return new SingleColumn(new CoalesceExpression(nameReference(column), new StringLiteral("")), alias);
//    }

  public static List<SortItem> ordering(SortItem... items) {
    return ImmutableList.copyOf(items);
  }

  public static Query simpleQuery(Select select) {
    return query(new QuerySpecification(
        select,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        ImmutableList.of(),
        Optional.empty()));
  }

  public static Query simpleQuery(Select select, Relation from) {
    return simpleQuery(select, from, Optional.empty(), ImmutableList.of());
  }

  public static Query simpleQuery(Select select, Relation from, List<SortItem> ordering) {
    return simpleQuery(select, from, Optional.empty(), ordering);
  }

  public static Query simpleQuery(Select select, Relation from, Expression where) {
    return simpleQuery(select, from, Optional.of(where), ImmutableList.of());
  }

  public static Query simpleQuery(Select select, Relation from, Expression where,
                                  List<SortItem> ordering) {
    return simpleQuery(select, from, Optional.of(where), ordering);
  }

  public static Query simpleQuery(Select select, Relation from, Optional<Expression> where,
                                  List<SortItem> ordering) {
    return simpleQuery(select, from, where, Optional.empty(), Optional.empty(), ordering,
                       Optional.empty());
  }

  public static Query simpleQuery(Select select, Relation from, Optional<Expression> where,
                                  Optional<GroupBy> groupBy, Optional<Expression> having,
                                  List<SortItem> ordering, Optional<String> limit) {
    return query(new QuerySpecification(
        select,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        where,
        groupBy,
        having,
        ordering,
        limit));
  }

  public static Query singleValueQuery(String columnName, String value) {
    Relation values = values(row(new StringLiteral((value))));
    return simpleQuery(
        selectList(new AllColumns()),
        aliased(values, "t", ImmutableList.of(columnName)));
  }

  public static Query query(QueryBody body) {
    return new Query(
        Optional.empty(),
        body,
        ImmutableList.of(),
        Optional.empty());
  }
}
