/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExistsPredicate
    extends Expression {

  private final Query subquery;

  public ExistsPredicate(Query subquery) {
    this(Optional.empty(), subquery);
  }

  public ExistsPredicate(NodeLocation location, Query subquery) {
    this(Optional.of(location), subquery);
  }

  private ExistsPredicate(Optional<NodeLocation> location, Query subquery) {
    super(location);
    requireNonNull(subquery, "subquery is null");
    this.subquery = subquery;
  }

  public Query getSubquery() {
    return subquery;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExists(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExistsPredicate that = (ExistsPredicate) o;
    return Objects.equals(subquery, that.subquery);
  }

  @Override
  public int hashCode() {
    return subquery.hashCode();
  }
}
