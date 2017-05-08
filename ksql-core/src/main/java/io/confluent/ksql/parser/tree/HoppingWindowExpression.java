/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class HoppingWindowExpression extends KQLWindowExpression {

  private final long size;
  private final WindowExpression.WindowUnit sizeUnit;
  private final long advanceBy;
  private final WindowExpression.WindowUnit advanceByUnit;

  public HoppingWindowExpression(long size, WindowExpression.WindowUnit sizeUnit,
                                  long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    this(Optional.empty(), "", size, sizeUnit, advanceBy, advanceByUnit);
  }

  public HoppingWindowExpression(NodeLocation location, String windowName, long size, WindowExpression.WindowUnit
      sizeUnit,
                                   long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    this(Optional.of(location), windowName, size, sizeUnit, advanceBy, advanceByUnit);
  }

  private HoppingWindowExpression(Optional<NodeLocation> location, String windowName, long size,
                                   WindowExpression.WindowUnit sizeUnit,
                             long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
    this.advanceBy = advanceBy;
    this.advanceByUnit = advanceByUnit;
  }

  public long getSize() {
    return size;
  }

  public WindowExpression.WindowUnit getSizeUnit() {
    return sizeUnit;
  }

  public long getAdvanceBy() {
    return advanceBy;
  }

  public WindowExpression.WindowUnit getAdvanceByUnit() {
    return advanceByUnit;
  }

  @Override
  public String toString() {
    return " HOPPING ( SIZE " + size + " " + sizeUnit + " , ADVANCE BY "
           + advanceBy + " "
           + "" + advanceByUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit, advanceBy, advanceByUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression) o;
    return hoppingWindowExpression.size == size && hoppingWindowExpression.sizeUnit == sizeUnit
           && hoppingWindowExpression.advanceBy == advanceBy && hoppingWindowExpression
               .advanceByUnit == advanceByUnit;
  }
}
