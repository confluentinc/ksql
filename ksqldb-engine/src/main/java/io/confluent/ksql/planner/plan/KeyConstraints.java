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
import io.confluent.ksql.planner.plan.PullFilterNode.WindowBounds;
import java.util.Optional;

public class KeyConstraints {

  /**
   * An interface for a KeyConstraint, which represents what we know about the use of keys in
   * a given expression.  If there's a key value, an operator associated with it, or window bounds,
   * they're available through the given methods. These are used as hints for the physical planning
   * layer about how to fetch the corresponding rows.
   */
  public interface KeyConstraint {
    // The key value.  Should be non-null for everything except unbound constraints
    GenericKey getKey();

    // The constraint type.  This should be used at a high level for determining which strategy to
    // use at the physical layer.
    ConstraintType getConstraintType();

    // The constraint operator associated with the value
    ConstraintOperator getConstraintOperator();

    // Window bounds, if the query is for a windowed table.
    Optional<WindowBounds> getWindowBounds();
  }

  public enum ConstraintType {
    NONE,
    EQUALITY,
    RANGE
  }

  public enum ConstraintOperator {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
  }

  /**
   * Means nothing could be extracted about the keys.  Either the expression is too complex or
   * doesn't make reference to keys, so we consider there to be no bound key constraint.
   * Obviously, we'll still have to evaluate the expression for correctness on some overly
   * permissive set of rows (e.g. table scan), but we cannot use a key as a hint for fetching the
   * data.
   */
  public static class UnboundKeyConstraint implements KeyConstraint {

    @Override
    public GenericKey getKey() {
      return null;
    }

    @Override
    public ConstraintType getConstraintType() {
      return ConstraintType.NONE;
    }

    @Override
    public ConstraintOperator getConstraintOperator() {
      return null;
    }

    @Override
    public Optional<WindowBounds> getWindowBounds() {
      return Optional.empty();
    }
  }

  /**
   * Simple key equality constraint.  This means that the expression can be filtered down to rows
   * which have the following key value.
   */
  public static class KeyEqualityConstraint implements KeyConstraint {

    private final GenericKey key;
    private final Optional<WindowBounds> windowBounds;

    public KeyEqualityConstraint(final GenericKey key, final Optional<WindowBounds> windowBounds) {
      this.key = key;
      this.windowBounds = windowBounds;
    }

    @Override
    public GenericKey getKey() {
      return key;
    }

    @Override
    public ConstraintType getConstraintType() {
      return ConstraintType.EQUALITY;
    }

    @Override
    public ConstraintOperator getConstraintOperator() {
      return ConstraintOperator.EQUAL;
    }

    @Override
    public Optional<WindowBounds> getWindowBounds() {
      return windowBounds;
    }
  }
}
