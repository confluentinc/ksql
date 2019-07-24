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
import java.util.Objects;
import java.util.Optional;

@Immutable
public class IntegerLiteral extends Literal {

  private final int value;

  public IntegerLiteral(final int value) {
    this (Optional.empty(), value);
  }

  public IntegerLiteral(final Optional<NodeLocation> location, final int value) {
    super(location);
    this.value = value;
  }

  @Override
  public Integer getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitIntegerLiteral(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IntegerLiteral that = (IntegerLiteral) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
