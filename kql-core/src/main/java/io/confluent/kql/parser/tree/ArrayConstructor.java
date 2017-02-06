/**
 * Copyright 2017 Confluent Inc.
 *
 **/

package io.confluent.kql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrayConstructor
    extends Expression {

  public static final String ARRAY_CONSTRUCTOR = "ARRAY_CONSTRUCTOR";
  private final List<Expression> values;

  public ArrayConstructor(List<Expression> values) {
    this(Optional.empty(), values);
  }

  public ArrayConstructor(NodeLocation location, List<Expression> values) {
    this(Optional.of(location), values);
  }

  private ArrayConstructor(Optional<NodeLocation> location, List<Expression> values) {
    super(location);
    requireNonNull(values, "values is null");
    this.values = ImmutableList.copyOf(values);
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitArrayConstructor(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayConstructor that = (ArrayConstructor) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }
}
