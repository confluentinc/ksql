/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class IsNotNullPredicate
    extends Expression {

  private final Expression value;

  public IsNotNullPredicate(final Expression value) {
    this(Optional.empty(), value);
  }

  public IsNotNullPredicate(final NodeLocation location, final Expression value) {
    this(Optional.of(location), value);
  }

  private IsNotNullPredicate(final Optional<NodeLocation> location, final Expression value) {
    super(location);
    requireNonNull(value, "value is null");
    this.value = value;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitIsNotNullPredicate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IsNotNullPredicate that = (IsNotNullPredicate) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
