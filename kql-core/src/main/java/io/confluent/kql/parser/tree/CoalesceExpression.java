/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.parser.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CoalesceExpression
    extends Expression {

  private final List<Expression> operands;

  public CoalesceExpression(Expression... operands) {
    this(Optional.empty(), ImmutableList.copyOf(operands));
  }

  public CoalesceExpression(List<Expression> operands) {
    this(Optional.empty(), operands);
  }

  public CoalesceExpression(NodeLocation location, List<Expression> operands) {
    this(Optional.of(location), operands);
  }

  private CoalesceExpression(Optional<NodeLocation> location, List<Expression> operands) {
    super(location);
    requireNonNull(operands, "operands is null");
    Preconditions.checkArgument(!operands.isEmpty(), "operands is empty");

    this.operands = ImmutableList.copyOf(operands);
  }

  public List<Expression> getOperands() {
    return operands;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCoalesceExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoalesceExpression that = (CoalesceExpression) o;
    return Objects.equals(operands, that.operands);
  }

  @Override
  public int hashCode() {
    return operands.hashCode();
  }
}
