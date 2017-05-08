/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Window
    extends Node {


  private final WindowExpression windowExpression;

  public Window(String windowName, WindowExpression windowExpression) {
    this(Optional.empty(), windowName, windowExpression);
  }

  public Window(NodeLocation location, String windowName, WindowExpression windowExpression) {
    this(Optional.of(location), windowName, windowExpression);
  }

  private Window(Optional<NodeLocation> location, String windowName, WindowExpression windowExpression) {
    super(location);
    this.windowExpression = requireNonNull(windowExpression, "windowExpression is null");
  }

  public WindowExpression getWindowExpression() {
    return windowExpression;
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
    return Objects.equals(windowExpression, o.windowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowExpression.toString();
  }
}
