/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SimpleGroupBy
    extends GroupingElement {

  private final List<Expression> columns;

  public SimpleGroupBy(List<Expression> simpleGroupByExpressions) {
    this(Optional.empty(), simpleGroupByExpressions);
  }

  public SimpleGroupBy(NodeLocation location, List<Expression> simpleGroupByExpressions) {
    this(Optional.of(location), simpleGroupByExpressions);
  }

  private SimpleGroupBy(Optional<NodeLocation> location,
                        List<Expression> simpleGroupByExpressions) {
    super(location);
    this.columns = requireNonNull(simpleGroupByExpressions);
  }

  public List<Expression> getColumnExpressions() {
    return columns;
  }

  @Override
  public List<Set<Expression>> enumerateGroupingSets() {
    return ImmutableList.of(ImmutableSet.copyOf(columns));
  }

  @Override
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSimpleGroupBy(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleGroupBy that = (SimpleGroupBy) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("columns", columns)
        .toString();
  }
}
