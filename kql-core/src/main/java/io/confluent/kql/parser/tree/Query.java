/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Query
    extends Statement {

  private final Optional<With> with;
  private final QueryBody queryBody;
  private final List<SortItem> orderBy;
  private final Optional<String> limit;
  private final Optional<Approximate> approximate;

  public Query(
      Optional<With> with,
      QueryBody queryBody,
      List<SortItem> orderBy,
      Optional<String> limit,
      Optional<Approximate> approximate) {
    this(Optional.empty(), with, queryBody, orderBy, limit, approximate);
  }

  public Query(
      NodeLocation location,
      Optional<With> with,
      QueryBody queryBody,
      List<SortItem> orderBy,
      Optional<String> limit,
      Optional<Approximate> approximate) {
    this(Optional.of(location), with, queryBody, orderBy, limit, approximate);
  }

  private Query(
      Optional<NodeLocation> location,
      Optional<With> with,
      QueryBody queryBody,
      List<SortItem> orderBy,
      Optional<String> limit,
      Optional<Approximate> approximate) {
    super(location);
    requireNonNull(with, "with is null");
    requireNonNull(queryBody, "queryBody is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(limit, "limit is null");
    requireNonNull(approximate, "approximate is null");

    this.with = with;
    this.queryBody = queryBody;
    this.orderBy = orderBy;
    this.limit = limit;
    this.approximate = approximate;
  }

  public Optional<With> getWith() {
    return with;
  }

  public QueryBody getQueryBody() {
    return queryBody;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<String> getLimit() {
    return limit;
  }

  public Optional<Approximate> getApproximate() {
    return approximate;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("with", with.orElse(null))
        .add("queryBody", queryBody)
        .add("orderBy", orderBy)
        .add("limit", limit.orElse(null))
        .add("approximate", approximate.orElse(null))
        .omitNullValues()
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
    Query o = (Query) obj;
    return Objects.equals(with, o.with) &&
           Objects.equals(queryBody, o.queryBody) &&
           Objects.equals(orderBy, o.orderBy) &&
           Objects.equals(limit, o.limit) &&
           Objects.equals(approximate, o.approximate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(with, queryBody, orderBy, limit, approximate);
  }
}
