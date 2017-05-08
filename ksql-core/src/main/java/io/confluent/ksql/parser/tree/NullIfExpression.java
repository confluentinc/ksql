/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

/**
 * NULLIF(V1,V2): CASE WHEN V1=V2 THEN NULL ELSE V1 END
 */
public class NullIfExpression
    extends Expression {

  private final Expression first;
  private final Expression second;

  public NullIfExpression(Expression first, Expression second) {
    this(Optional.empty(), first, second);
  }

  public NullIfExpression(NodeLocation location, Expression first, Expression second) {
    this(Optional.of(location), first, second);
  }

  private NullIfExpression(Optional<NodeLocation> location, Expression first, Expression second) {
    super(location);
    this.first = first;
    this.second = second;
  }

  public Expression getFirst() {
    return first;
  }

  public Expression getSecond() {
    return second;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNullIfExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NullIfExpression that = (NullIfExpression) o;
    return Objects.equals(first, that.first) &&
           Objects.equals(second, that.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }
}
