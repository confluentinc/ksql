/*
 * Copyright 2022 Confluent Inc.
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

import static com.google.common.base.MoreObjects.toStringHelper;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public class EvaluateExpression extends Statement {
  Optional<NodeLocation> location;
  Expression expression;

  public EvaluateExpression(
      final Optional<NodeLocation> location,
      final Expression expression) {
    super(location);
    this.expression = expression;
    this.location = location;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EvaluateExpression that = (EvaluateExpression) o;
    return expression == that.expression;
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("expression", expression)
        .toString();
  }
}
