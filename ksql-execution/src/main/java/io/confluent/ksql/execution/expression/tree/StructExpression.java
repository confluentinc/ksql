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
import io.confluent.ksql.parser.NodeLocation;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class StructExpression extends Expression {

  private final Map<String, Expression> struct;

  public StructExpression(
      final Map<String, Expression> struct
  ) {
    this(Optional.empty(), struct);
  }

  public StructExpression(
      final Optional<NodeLocation> location,
      final Map<String, Expression> struct
  ) {
    super(location);
    this.struct = ImmutableMap.copyOf(struct);
  }

  @Override
  protected <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitStructExpression(this, context);
  }

  public Map<String, Expression> getStruct() {
    return struct;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructExpression that = (StructExpression) o;
    return Objects.equals(struct, that.struct);
  }

  @Override
  public int hashCode() {
    return Objects.hash(struct);
  }

}
