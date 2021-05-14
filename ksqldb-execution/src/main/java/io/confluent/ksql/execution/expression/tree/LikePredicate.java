/*
 * Copyright 2021 Confluent Inc.
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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class LikePredicate extends Expression {

  private final Expression value;
  private final Expression pattern;
  private final Optional<Character> escape;

  public LikePredicate(
      final Expression value,
      final Expression pattern,
      final Optional<Character> escape
  ) {
    this(Optional.empty(), value, pattern, escape);
  }

  public LikePredicate(
      final Optional<NodeLocation> location,
      final Expression value,
      final Expression pattern,
      final Optional<Character> escape
  ) {
    super(location);
    this.value = requireNonNull(value, "value");
    this.pattern = requireNonNull(pattern, "pattern");
    this.escape = requireNonNull(escape, "escape");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getPattern() {
    return pattern;
  }

  public Optional<Character> getEscape() {
    return escape;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitLikePredicate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LikePredicate that = (LikePredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(pattern, that.pattern)
        && Objects.equals(escape, that.escape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, pattern, escape);
  }
}
