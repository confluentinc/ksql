/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InListExpression
    extends Expression {

  private final List<Expression> values;

  public InListExpression(List<Expression> values) {
    this(Optional.empty(), values);
  }

  public InListExpression(NodeLocation location, List<Expression> values) {
    this(Optional.of(location), values);
  }

  private InListExpression(Optional<NodeLocation> location, List<Expression> values) {
    super(location);
    this.values = values;
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInListExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InListExpression that = (InListExpression) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }
}
