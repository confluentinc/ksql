/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.planner.plan.QueryFilterNode.WindowBounds;
import java.util.Objects;
import java.util.Optional;

/**
 * An instance of this class represents what we know about the use of keys in a given disjunct
 * from an expression.  The key's value, operator associated with it, and window bounds are
 * available through the given methods. These are used as hints for the physical planning
 * layer about how to fetch the corresponding rows.
 */
public class KeyConstraint implements LookupConstraint, KsqlKey {

  private final ConstraintOperator operator;
  private final GenericKey key;
  private final Optional<WindowBounds> windowBounds;

  public KeyConstraint(final ConstraintOperator operator, final GenericKey key,
                       final Optional<WindowBounds> windowBounds) {
    this.operator = operator;
    this.key = key;
    this.windowBounds = windowBounds;
  }

  // The key value.
  public GenericKey getKey() {
    return key;
  }

  // The constraint operator associated with the value
  public ConstraintOperator getOperator() {
    return operator;
  }

  // Window bounds, if the query is for a windowed table.
  public Optional<WindowBounds> getWindowBounds() {
    return windowBounds;
  }

  public KsqlKey getKsqlKey() {
    return this;
  }

  // If the operator represents a range of keys
  public boolean isRangeOperator() {
    return operator != ConstraintOperator.EQUAL;
  }

  public enum ConstraintOperator {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, windowBounds);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyConstraint that = (KeyConstraint) o;
    return Objects.equals(this.key, that.key)
      && Objects.equals(this.windowBounds, that.windowBounds);
  }

  @Override
  public String toString() {
    return key.toString() + "-" + windowBounds.toString();
  }
}
