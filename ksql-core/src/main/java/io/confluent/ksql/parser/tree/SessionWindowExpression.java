/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class SessionWindowExpression extends KsqlWindowExpression {

  private final long gap;
  private final WindowExpression.WindowUnit sizeUnit;

  public SessionWindowExpression(long gap, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.empty(), "", gap, sizeUnit);
  }

  public SessionWindowExpression(NodeLocation location, String windowName,
                                 long gap, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.of(location), windowName, gap, sizeUnit);
  }

  private SessionWindowExpression(Optional<NodeLocation> location, String windowName, long gap,
                                  WindowExpression.WindowUnit sizeUnit) {
    super(location);
    this.gap = gap;
    this.sizeUnit = sizeUnit;
  }

  public long getGap() {
    return gap;
  }

  public WindowExpression.WindowUnit getSizeUnit() {
    return sizeUnit;
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SessionWindowExpression sessionWindowExpression = (SessionWindowExpression) o;
    return sessionWindowExpression.gap == gap && sessionWindowExpression.sizeUnit == sizeUnit;
  }
}
