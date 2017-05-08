/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class WindowExpression extends Node {

  private final String windowName;
  private  final KQLWindowExpression kqlWindowExpression;

  public WindowExpression(String windowName, KQLWindowExpression kqlWindowExpression) {
    this(Optional.empty(), windowName, kqlWindowExpression);
  }

  protected WindowExpression(Optional<NodeLocation> location, String windowName, KQLWindowExpression kqlWindowExpression) {
    super(location);
    this.windowName = windowName;
    this.kqlWindowExpression = kqlWindowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  public KQLWindowExpression getKqlWindowExpression() {
    return kqlWindowExpression;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WindowExpression o = (WindowExpression) obj;
    return Objects.equals(kqlWindowExpression, o.kqlWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, kqlWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + kqlWindowExpression.toString();
  }

  public static enum WindowUnit { DAY, HOUR, MINUTE, SECOND, MILLISECOND }

  public static WindowUnit getWindowUnit(String windowUnitString) {
    switch (windowUnitString) {
      case "DAY":
        return WindowUnit.DAY;
      case "HOUR":
        return WindowUnit.HOUR;
      case "MINUTE":
        return WindowUnit.MINUTE;
      case "SECOND":
        return WindowUnit.SECOND;
      case "MILLISECOND":
        return WindowUnit.MILLISECOND;
      default:
        return null;
    }
  }

}
