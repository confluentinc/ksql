/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Insert
    extends Statement {

  private final QualifiedName target;
  private final Query query;
  private final Optional<List<String>> columns;

  public Insert(QualifiedName target, Optional<List<String>> columns, Query query) {
    this(Optional.empty(), columns, target, query);
  }

  private Insert(Optional<NodeLocation> location, Optional<List<String>> columns,
                 QualifiedName target, Query query) {
    super(location);
    this.target = requireNonNull(target, "target is null");
    this.columns = requireNonNull(columns, "columns is null");
    this.query = requireNonNull(query, "query is null");
  }

  public QualifiedName getTarget() {
    return target;
  }

  public Optional<List<String>> getColumns() {
    return columns;
  }

  public Query getQuery() {
    return query;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsert(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, columns, query);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Insert o = (Insert) obj;
    return Objects.equals(target, o.target) &&
           Objects.equals(columns, o.columns) &&
           Objects.equals(query, o.query);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("target", target)
        .add("columns", columns)
        .add("query", query)
        .toString();
  }
}
