/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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

import java.util.Objects;
import java.util.Optional;

public class LikePredicate
    extends Expression {

  private final Expression value;
  private final Expression pattern;
  private final Expression escape;

  public LikePredicate(final Expression value, final Expression pattern, final Expression escape) {
    this(Optional.empty(), value, pattern, escape);
  }

  public LikePredicate(
      final NodeLocation location,
      final Expression value,
      final Expression pattern,
      final Expression escape) {
    this(Optional.of(location), value, pattern, escape);
  }

  private LikePredicate(
      final Optional<NodeLocation> location, final Expression value, final Expression pattern,
                        final Expression escape) {
    super(location);
    requireNonNull(value, "value is null");
    requireNonNull(pattern, "pattern is null");

    this.value = value;
    this.pattern = pattern;
    this.escape = escape;
  }

  public Expression getValue() {
    return value;
  }

  public Expression getPattern() {
    return pattern;
  }

  public Expression getEscape() {
    return escape;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
