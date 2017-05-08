/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LikePredicate
    extends Expression {

  private final Expression value;
  private final Expression pattern;
  private final Expression escape;

  public LikePredicate(Expression value, Expression pattern, Expression escape) {
    this(Optional.empty(), value, pattern, escape);
  }

  public LikePredicate(NodeLocation location, Expression value, Expression pattern,
                       Expression escape) {
    this(Optional.of(location), value, pattern, escape);
  }

  private LikePredicate(Optional<NodeLocation> location, Expression value, Expression pattern,
                        Expression escape) {
    super(location);
    requireNonNull(value, "value is null");
    requireNonNull(pattern, "pattern is null");

    this.value = value;
    this.pattern = pattern;
    this.escape = escape;
  }

  public Expression getValue() {
    return value;
  }

  public Expression getPattern() {
    return pattern;
  }

  public Expression getEscape() {
    return escape;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLikePredicate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LikePredicate that = (LikePredicate) o;
    return Objects.equals(value, that.value) &&
           Objects.equals(pattern, that.pattern) &&
           Objects.equals(escape, that.escape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, pattern, escape);
  }
}
