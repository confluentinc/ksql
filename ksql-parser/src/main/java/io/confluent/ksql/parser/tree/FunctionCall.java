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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class FunctionCall extends Expression {

  private final QualifiedName name;
  private final boolean distinct;
  private final List<Expression> arguments;

  public FunctionCall(final QualifiedName name, final List<Expression> arguments) {
    this(Optional.empty(), name, false, arguments);
  }

  public FunctionCall(
      final NodeLocation location,
      final QualifiedName name,
      final List<Expression> arguments) {
    this(Optional.of(location), name, false, arguments);
  }

  public FunctionCall(
      final QualifiedName name,
      final boolean distinct,
      final List<Expression> arguments) {
    this(Optional.empty(), name, distinct, arguments);
  }

  public FunctionCall(
      final NodeLocation location,
      final QualifiedName name,
      final boolean distinct,
      final List<Expression> arguments) {
    this(Optional.of(location), name, distinct, arguments);
  }

  public FunctionCall(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final boolean distinct,
      final List<Expression> arguments) {
    super(location);
    requireNonNull(name, "name is null");
    requireNonNull(arguments, "arguments is null");

    this.name = name;
    this.distinct = distinct;
    this.arguments = arguments;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
           && Objects.equals(distinct, o.distinct)
           && Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, distinct, arguments);
  }
}
