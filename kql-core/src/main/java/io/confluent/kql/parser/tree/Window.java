/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Window
    extends Node {

  private final List<Expression> partitionBy;
  private final List<SortItem> orderBy;
  private final Optional<WindowFrame> frame;

  public Window(List<Expression> partitionBy, List<SortItem> orderBy, Optional<WindowFrame> frame) {
    this(Optional.empty(), partitionBy, orderBy, frame);
  }

  public Window(NodeLocation location, List<Expression> partitionBy, List<SortItem> orderBy,
                Optional<WindowFrame> frame) {
    this(Optional.of(location), partitionBy, orderBy, frame);
  }

  private Window(Optional<NodeLocation> location, List<Expression> partitionBy,
                 List<SortItem> orderBy, Optional<WindowFrame> frame) {
    super(location);
    this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
    this.frame = requireNonNull(frame, "frame is null");
  }

  public List<Expression> getPartitionBy() {
    return partitionBy;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<WindowFrame> getFrame() {
    return frame;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Window o = (Window) obj;
    return Objects.equals(partitionBy, o.partitionBy) &&
           Objects.equals(orderBy, o.orderBy) &&
           Objects.equals(frame, o.frame);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionBy, orderBy, frame);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionBy", partitionBy)
        .add("orderBy", orderBy)
        .add("frame", frame)
        .toString();
  }
}
