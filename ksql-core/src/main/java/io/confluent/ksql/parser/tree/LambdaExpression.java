/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LambdaExpression
    extends Expression {

  private final List<String> arguments;
  private final Expression body;

  public LambdaExpression(List<String> arguments, Expression body) {
    this(Optional.empty(), arguments, body);
  }

  public LambdaExpression(NodeLocation location, List<String> arguments, Expression body) {
    this(Optional.of(location), arguments, body);
  }

  private LambdaExpression(Optional<NodeLocation> location, List<String> arguments,
                           Expression body) {
    super(location);
    this.arguments = requireNonNull(arguments, "arguments is null");
    this.body = requireNonNull(body, "body is null");
  }

  public List<String> getArguments() {
    return arguments;
  }

  public Expression getBody() {
    return body;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLambdaExpression(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LambdaExpression that = (LambdaExpression) obj;
    return Objects.equals(arguments, that.arguments) &&
           Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(arguments, body);
  }
}
