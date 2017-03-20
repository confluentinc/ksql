/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

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
    if (windowUnitString.equals("day")) {
      return WindowUnit.DAY;
    } else if (windowUnitString.equals("hour")) {
      return WindowUnit.HOUR;
    } else if (windowUnitString.equals("minute")) {
      return WindowUnit.MINUTE;
    } else if (windowUnitString.equals("second")) {
      return WindowUnit.SECOND;
    } else if (windowUnitString.equals("millisecond")) {
      return WindowUnit.MILLISECOND;
    } else {
      return null;
    }
  }

}
