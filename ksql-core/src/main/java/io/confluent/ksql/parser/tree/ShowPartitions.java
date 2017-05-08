/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowPartitions
    extends Statement {

  private final QualifiedName table;
  private final Optional<Expression> where;
  private final List<SortItem> orderBy;
  private final Optional<String> limit;

  public ShowPartitions(QualifiedName table, Optional<Expression> where, List<SortItem> orderBy,
                        Optional<String> limit) {
    this(Optional.empty(), table, where, orderBy, limit);
  }

  public ShowPartitions(NodeLocation location, QualifiedName table, Optional<Expression> where,
                        List<SortItem> orderBy, Optional<String> limit) {
    this(Optional.of(location), table, where, orderBy, limit);
  }

  private ShowPartitions(Optional<NodeLocation> location, QualifiedName table,
                         Optional<Expression> where, List<SortItem> orderBy,
                         Optional<String> limit) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.where = requireNonNull(where, "where is null");
    this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
    this.limit = requireNonNull(limit, "limit is null");
  }

  public QualifiedName getTable() {
    return table;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<String> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowPartitions(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, where, orderBy, limit);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    ShowPartitions o = (ShowPartitions) obj;
    return Objects.equals(table, o.table) &&
           Objects.equals(where, o.where) &&
           Objects.equals(orderBy, o.orderBy) &&
           Objects.equals(limit, o.limit);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("where", where)
        .add("orderBy", orderBy)
        .add("limit", limit)
        .toString();
  }
}
