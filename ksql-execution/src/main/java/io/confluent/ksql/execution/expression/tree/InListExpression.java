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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class InListExpression extends Expression {

  private final ImmutableList<Expression> values;

  public InListExpression(List<Expression> values) {
    this(Optional.empty(), values);
  }

  public InListExpression(Optional<NodeLocation> location, List<Expression> values) {
    super(location);
    this.values = ImmutableList.copyOf(requireNonNull(values, "values"));

    if (values.isEmpty()) {
      throw new IllegalArgumentException("IN statement requires at least one value.");
    }
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitInListExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InListExpression that = (InListExpression) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }
}
