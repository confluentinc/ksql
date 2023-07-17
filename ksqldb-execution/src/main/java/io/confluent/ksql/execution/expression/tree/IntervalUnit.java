/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

public class IntervalUnit extends Expression {

  final TimeUnit unit;

  public IntervalUnit(final TimeUnit unit) {
    this(Optional.empty(), unit);
  }

  public IntervalUnit(final Optional<NodeLocation> location, final TimeUnit unit) {
    super(location);
    this.unit = unit;
  }

  public TimeUnit getUnit() {
    return unit;
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitIntervalUnit(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(unit);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IntervalUnit that = (IntervalUnit) o;
    return unit == that.unit;
  }
}
