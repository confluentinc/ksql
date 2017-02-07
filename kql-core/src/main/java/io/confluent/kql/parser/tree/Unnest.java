/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Unnest
    extends Relation {

  private final List<Expression> expressions;
  private final boolean withOrdinality;

  public Unnest(List<Expression> expressions, boolean withOrdinality) {
    this(Optional.empty(), expressions, withOrdinality);
  }

  public Unnest(NodeLocation location, List<Expression> expressions, boolean withOrdinality) {
    this(Optional.of(location), expressions, withOrdinality);
  }

  private Unnest(Optional<NodeLocation> location, List<Expression> expressions,
                 boolean withOrdinality) {
    super(location);
    requireNonNull(expressions, "expressions is null");
    this.expressions = ImmutableList.copyOf(expressions);
    this.withOrdinality = withOrdinality;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public boolean isWithOrdinality() {
    return withOrdinality;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }

  @Override
  public String toString() {
    String result = "UNNEST(" + Joiner.on(", ").join(expressions) + ")";
    if (withOrdinality) {
      result += " WITH ORDINALITY";
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressions);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Unnest other = (Unnest) obj;
    return Objects.equals(this.expressions, other.expressions);
  }
}
