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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class CreateMapExpression extends Expression {

  private final ImmutableMap<Expression, Expression> map;

  public CreateMapExpression(
      final Optional<NodeLocation> location,
      final Map<Expression, Expression> map
  ) {
    super(location);
    this.map = ImmutableMap.copyOf(map);
  }

  public CreateMapExpression(final Map<Expression, Expression> map) {
    this(Optional.empty(), map);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "map is ImmutableMap")
  public ImmutableMap<Expression, Expression> getMap() {
    return map;
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateMapExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateMapExpression that = (CreateMapExpression) o;
    return Objects.equals(map, that.map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(map);
  }
}
