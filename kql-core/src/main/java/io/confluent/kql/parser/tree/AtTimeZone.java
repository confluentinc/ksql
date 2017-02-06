/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AtTimeZone
    extends Expression {

  private final Expression value;
  private final Expression timeZone;

  public AtTimeZone(Expression value, Expression timeZone) {
    this(Optional.empty(), value, timeZone);
  }

  public AtTimeZone(NodeLocation location, Expression value, Expression timeZone) {
    this(Optional.of(location), value, timeZone);
  }

  private AtTimeZone(Optional<NodeLocation> location, Expression value, Expression timeZone) {
    super(location);
    checkArgument(timeZone instanceof IntervalLiteral || timeZone instanceof StringLiteral,
                  "timeZone must be IntervalLiteral or StringLiteral");
    this.value = requireNonNull(value, "value is null");
    this.timeZone = requireNonNull(timeZone, "timeZone is null");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getTimeZone() {
    return timeZone;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAtTimeZone(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, timeZone);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    AtTimeZone atTimeZone = (AtTimeZone) obj;
    return Objects.equals(value, atTimeZone.value) && Objects.equals(timeZone, atTimeZone.timeZone);
  }
}
