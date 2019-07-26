/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Immutable
public class WindowExpression extends AstNode {

  private final String windowName;
  private final KsqlWindowExpression ksqlWindowExpression;

  public WindowExpression(
      final String windowName,
      final KsqlWindowExpression ksqlWindowExpression
  ) {
    this(Optional.empty(), windowName, ksqlWindowExpression);
  }

  public WindowExpression(
      final Optional<NodeLocation> location,
      final String windowName,
      final KsqlWindowExpression ksqlWindowExpression
  ) {
    super(location);
    this.windowName = requireNonNull(windowName, "windowName");
    this.ksqlWindowExpression = requireNonNull(ksqlWindowExpression, "ksqlWindowExpression");
  }

  public KsqlWindowExpression getKsqlWindowExpression() {
    return ksqlWindowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowExpression that = (WindowExpression) o;
    return Objects.equals(windowName, that.windowName)
        && Objects.equals(ksqlWindowExpression, that.ksqlWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, ksqlWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + ksqlWindowExpression;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitWindowExpression(this, context);
  }

  public static TimeUnit getWindowUnit(final String windowUnitString) {
    try {
      if (!windowUnitString.endsWith("S")) {
        return TimeUnit.valueOf(windowUnitString + "S");
      }
      return TimeUnit.valueOf(windowUnitString);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
