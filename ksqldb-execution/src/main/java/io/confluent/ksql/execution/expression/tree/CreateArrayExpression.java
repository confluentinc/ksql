/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class CreateArrayExpression extends Expression {

  private final ImmutableList<Expression> values;

  public CreateArrayExpression(
      final Optional<NodeLocation> location,
      final List<Expression> values
  ) {
    super(location);
    this.values = ImmutableList.copyOf(values);
  }

  public CreateArrayExpression(final List<Expression> values) {
    this(Optional.empty(), values);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "values is ImmutableList")
  public ImmutableList<Expression> getValues() {
    return values;
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateArrayExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateArrayExpression that = (CreateArrayExpression) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

}
