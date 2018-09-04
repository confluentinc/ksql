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

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class Window
    extends Node {


  private final WindowExpression windowExpression;
  private final String windowName;

  public Window(final String windowName, final WindowExpression windowExpression) {
    this(Optional.empty(), windowName, windowExpression);
  }

  public Window(
      final NodeLocation location,
      final String windowName,
      final WindowExpression windowExpression
  ) {
    this(Optional.of(location), windowName, windowExpression);
  }

  private Window(
      final Optional<NodeLocation> location,
      final String windowName,
      final WindowExpression windowExpression
  ) {
    super(location);
    this.windowExpression = requireNonNull(windowExpression, "windowExpression is null");
    this.windowName = windowName;
  }

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitWindow(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Window o = (Window) obj;
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
