/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class TumblingWindowExpression extends KSQLWindowExpression {

  private final long size;
  private final WindowExpression.WindowUnit sizeUnit;

  public TumblingWindowExpression(long size, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.empty(), "", size, sizeUnit);
  }

  public TumblingWindowExpression(NodeLocation location, String windowName, long size, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.of(location), windowName, size, sizeUnit);
  }

  private TumblingWindowExpression(Optional<NodeLocation> location, String windowName, long size,
                                  WindowExpression.WindowUnit sizeUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
  }

  public long getSize() {
    return size;
  }

  public WindowExpression.WindowUnit getSizeUnit() {
    return sizeUnit;
  }

  @Override
  public String toString() {
    return " TUMBLING ( SIZE " + size + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression) o;
    return tumblingWindowExpression.size == size && tumblingWindowExpression.sizeUnit == sizeUnit;
  }
}
