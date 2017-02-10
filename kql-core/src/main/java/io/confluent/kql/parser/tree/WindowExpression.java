/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class WindowExpression extends Node {

  private final String windowName;
  private  final HoppingWindowExpression hoppingWindowExpression;

  public WindowExpression(String windowName, HoppingWindowExpression hoppingWindowExpression) {
    this(Optional.empty(), windowName, hoppingWindowExpression);
  }

  protected WindowExpression(Optional<NodeLocation> location, String windowName, HoppingWindowExpression hoppingWindowExpression) {
    super(location);
    this.windowName = windowName;
    this.hoppingWindowExpression = hoppingWindowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  public HoppingWindowExpression getHoppingWindowExpression() {
    return hoppingWindowExpression;
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
    return Objects.equals(hoppingWindowExpression, o.hoppingWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, hoppingWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + hoppingWindowExpression.toString();
  }

  public static enum WindowUnit { DAY, HOUR, MINUTE, SECOND, MILLISECOND }

  public static WindowUnit getWindowUnit(String windowUnitString) {
    if (windowUnitString.equalsIgnoreCase("day")) {
      return WindowUnit.DAY;
    } else if (windowUnitString.equalsIgnoreCase("hour")) {
      return WindowUnit.HOUR;
    } else if (windowUnitString.equalsIgnoreCase("minute")) {
      return WindowUnit.MINUTE;
    } else if (windowUnitString.equalsIgnoreCase("second")) {
      return WindowUnit.SECOND;
    } else if (windowUnitString.equalsIgnoreCase("millisecond")) {
      return WindowUnit.MILLISECOND;
    } else {
      return null;
    }
  }

}
