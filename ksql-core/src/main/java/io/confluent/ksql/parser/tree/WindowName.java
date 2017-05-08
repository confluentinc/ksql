/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;


public class WindowName extends Node {

  private String windowName;

  public WindowName(String windowName) {
    this(Optional.empty(), windowName);
  }

  public WindowName(NodeLocation location, String windowName) {
    this(Optional.of(location), windowName);
  }

  private WindowName(Optional<NodeLocation> location, String windowName) {
    super(location);
    this.windowName = windowName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WindowName o = (WindowName) obj;
    return Objects.equals(windowName, o.windowName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName);
  }

  @Override
  public String toString() {
    return windowName;
  }
}
