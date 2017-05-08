/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class SubqueryExpression
    extends Expression {

  private final Query query;

  public SubqueryExpression(Query query) {
    this(Optional.empty(), query);
  }

  public SubqueryExpression(NodeLocation location, Query query) {
    this(Optional.of(location), query);
  }

  private SubqueryExpression(Optional<NodeLocation> location, Query query) {
    super(location);
    this.query = query;
  }

  public Query getQuery() {
    return query;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSubqueryExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SubqueryExpression that = (SubqueryExpression) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return query.hashCode();
  }
}
