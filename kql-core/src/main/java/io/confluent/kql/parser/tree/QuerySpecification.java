/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QuerySpecification
    extends QueryBody {

  private final Select select;
  private final Optional<Relation> into;
  private final Optional<Relation> from;
  private final Optional<Expression> where;
  private final Optional<GroupBy> groupBy;
  private final Optional<Expression> having;
  private final List<SortItem> orderBy;
  private final Optional<String> limit;

  public QuerySpecification(
      Select select,
      Optional<Relation> into,
      Optional<Relation> from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      List<SortItem> orderBy,
      Optional<String> limit) {
    this(Optional.empty(), select, into, from, where, groupBy, having, orderBy, limit);
  }

  public QuerySpecification(
      NodeLocation location,
      Select select,
      Optional<Relation> into,
      Optional<Relation> from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      List<SortItem> orderBy,
      Optional<String> limit) {
    this(Optional.of(location), select, into, from, where, groupBy, having, orderBy, limit);
  }

  private QuerySpecification(
      Optional<NodeLocation> location,
      Select select,
      Optional<Relation> into,
      Optional<Relation> from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      List<SortItem> orderBy,
      Optional<String> limit) {
    super(location);
    requireNonNull(select, "select is null");
    requireNonNull(into, "into is null");
    requireNonNull(from, "from is null");
    requireNonNull(where, "where is null");
    requireNonNull(groupBy, "groupBy is null");
    requireNonNull(having, "having is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(limit, "limit is null");

    this.select = select;
    this.into = into;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.orderBy = orderBy;
    this.limit = limit;
  }

  public Select getSelect() {
    return select;
  }

  public Optional<Relation> getInto() {
    return into;
  }

  public Optional<Relation> getFrom() {
    return from;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public Optional<GroupBy> getGroupBy() {
    return groupBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<String> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuerySpecification(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("select", select)
        .add("from", from)
        .add("where", where.orElse(null))
        .add("groupBy", groupBy)
        .add("having", having.orElse(null))
        .add("orderBy", orderBy)
        .add("limit", limit.orElse(null))
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    QuerySpecification o = (QuerySpecification) obj;
    return Objects.equals(select, o.select) &&
           Objects.equals(from, o.from) &&
           Objects.equals(where, o.where) &&
           Objects.equals(groupBy, o.groupBy) &&
           Objects.equals(having, o.having) &&
           Objects.equals(orderBy, o.orderBy) &&
           Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(select, from, where, groupBy, having, orderBy, limit);
  }
}
