/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  private Window(Optional<NodeLocation> location, String windowName,
                 WindowExpression windowExpression) {
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
