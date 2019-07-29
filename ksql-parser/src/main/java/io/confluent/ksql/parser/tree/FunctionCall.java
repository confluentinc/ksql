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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class FunctionCall extends Expression {

  private final QualifiedName name;
  private final ImmutableList<Expression> arguments;

  public FunctionCall(
      final QualifiedName name,
      final List<Expression> arguments
  ) {
    this(Optional.empty(), name,  arguments);
  }

  public FunctionCall(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final List<Expression> arguments
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments"));
  }

  public QualifiedName getName() {
    return name;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitFunctionCall(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final FunctionCall o = (FunctionCall) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }
}
