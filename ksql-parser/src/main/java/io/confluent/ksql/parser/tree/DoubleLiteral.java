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

import com.google.errorprone.annotations.Immutable;
import java.util.Optional;

@Immutable
public class DoubleLiteral extends Literal {

  private final double value;

  public DoubleLiteral(final double value) {
    this(Optional.empty(), value);
  }

  public DoubleLiteral(final Optional<NodeLocation> location, final double value) {
    super(location);
    this.value = value;
  }

  @Override
  public Double getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDoubleLiteral(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DoubleLiteral that = (DoubleLiteral) o;

    return Double.compare(that.value, value) == 0;
  }

  @SuppressWarnings("UnaryPlus")
  @Override
  public int hashCode() {
    final long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
    return (int) (temp ^ (temp >>> 32));
  }
}
