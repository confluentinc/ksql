/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Row
    extends Expression {

  private final List<Expression> items;

  public Row(List<Expression> items) {
    this(Optional.empty(), items);
  }

  public Row(NodeLocation location, List<Expression> items) {
    this(Optional.of(location), items);
  }

  private Row(Optional<NodeLocation> location, List<Expression> items) {
    super(location);
    requireNonNull(items, "items is null");
    this.items = ImmutableList.copyOf(items);
  }

  public List<Expression> getItems() {
    return items;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRow(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    return Objects.equals(this.items, other.items);
  }
}
