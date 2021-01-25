/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.expression.tree;

import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class IntervalExpression extends Expression {
  private final Expression expression;
  private final TimeUnit timeUnit;

  public IntervalExpression(final Expression expression, final TimeUnit timeUnit) {
    this(Optional.empty(), expression, timeUnit);
  }

  public IntervalExpression(
      final Optional<NodeLocation> location,
      final Expression expression,
      final TimeUnit timeUnit
  ) {
    super(location);
    this.expression = Objects.requireNonNull(expression, "left");
    this.timeUnit = Objects.requireNonNull(timeUnit, "right");
  }

  public Expression getExpression() {
    return expression;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitIntervalExpression(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, timeUnit);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IntervalExpression that = (IntervalExpression) o;
    return Objects.equals(expression, that.expression)
        && Objects.equals(timeUnit, that.timeUnit);
  }
}
