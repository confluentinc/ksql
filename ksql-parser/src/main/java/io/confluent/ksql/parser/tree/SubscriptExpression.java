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

public class SubscriptExpression
    extends Expression {

  private final Expression base;
  private final Expression index;

  public SubscriptExpression(final Expression base, final Expression index) {
    this(Optional.empty(), base, index);
  }

  public SubscriptExpression(
      final NodeLocation location, final Expression base, final Expression index) {
    this(Optional.of(location), base, index);
  }

  private SubscriptExpression(
      final Optional<NodeLocation> location, final Expression base, final Expression index) {
    super(location);
    this.base = requireNonNull(base, "base is null");
    this.index = requireNonNull(index, "index is null");
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSubscriptExpression(this, context);
  }

  public Expression getBase() {
    return base;
  }

  public Expression getIndex() {
    return index;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SubscriptExpression that = (SubscriptExpression) o;

    return Objects.equals(this.base, that.base) && Objects.equals(this.index, that.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, index);
  }
}
