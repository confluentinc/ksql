/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class WindowExpression extends Node {

  private final String windowName;
  private  final KsqlWindowExpression ksqlWindowExpression;

  public WindowExpression(String windowName, KsqlWindowExpression ksqlWindowExpression) {
    this(Optional.empty(), windowName, ksqlWindowExpression);
  }

  protected WindowExpression(Optional<NodeLocation> location, String windowName,
                             KsqlWindowExpression ksqlWindowExpression) {
    super(location);
    this.windowName = windowName;
    this.ksqlWindowExpression = ksqlWindowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  public KsqlWindowExpression getKsqlWindowExpression() {
    return ksqlWindowExpression;
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
    return Objects.equals(ksqlWindowExpression, o.ksqlWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, ksqlWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + ksqlWindowExpression.toString();
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
