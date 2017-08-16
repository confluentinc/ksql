/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class WhenClause
    extends Expression {

  private final Expression operand;
  private final Expression result;

  public WhenClause(Expression operand, Expression result) {
    this(Optional.empty(), operand, result);
  }

  public WhenClause(NodeLocation location, Expression operand, Expression result) {
    this(Optional.of(location), operand, result);
  }

  private WhenClause(Optional<NodeLocation> location, Expression operand, Expression result) {
    super(location);
    this.operand = operand;
    this.result = result;
  }

  public Expression getOperand() {
    return operand;
  }

  public Expression getResult() {
    return result;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWhenClause(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WhenClause that = (WhenClause) o;
    return Objects.equals(operand, that.operand)
           && Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, result);
  }
}
