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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class BetweenPredicate extends Expression {

  private final Expression value;
  private final Expression min;
  private final Expression max;

  public BetweenPredicate(final Expression value, final Expression min, final Expression max) {
    this(Optional.empty(), value, min, max);
  }

  public BetweenPredicate(
      final Optional<NodeLocation> location,
      final Expression value,
      final Expression min,
      final Expression max
  ) {
    super(location);
    this.value = requireNonNull(value, "value");
    this.min = requireNonNull(min, "min");
    this.max = requireNonNull(max, "max");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getMin() {
    return min;
  }

  public Expression getMax() {
    return max;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitBetweenPredicate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BetweenPredicate that = (BetweenPredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(min, that.min)
        && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, min, max);
  }
}
