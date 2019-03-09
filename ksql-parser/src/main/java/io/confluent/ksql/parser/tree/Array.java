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

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class Array extends Type {

  private final Type itemType;

  public static Array of(final Type itemType) {
    return new Array(itemType);
  }

  private Array(final Type itemType) {
    super(Optional.empty(), SqlType.ARRAY);
    this.itemType = requireNonNull(itemType, "itemType");
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitArray(this, context);
  }

  public Type getItemType() {
    return itemType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Array array = (Array) o;
    return Objects.equals(itemType, array.itemType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(itemType);
  }
}
