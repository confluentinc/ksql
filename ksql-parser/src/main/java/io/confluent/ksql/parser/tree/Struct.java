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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class Struct
    extends Type {

  private final List<Pair<String, Type>> items;

  public Struct(final List<Pair<String, Type>> items) {
    this(Optional.empty(), items);
  }

  public Struct(final NodeLocation location, final List<Pair<String, Type>> items) {
    this(Optional.of(location), items);
  }

  private Struct(final Optional<NodeLocation> location, final List<Pair<String, Type>> items) {
    super(location, KsqlType.STRUCT);
    requireNonNull(items, "items is null");
    this.items = ImmutableList.copyOf(items);
  }

  public List<Pair<String, Type>> getItems() {
    return items;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitStruct(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Struct other = (Struct) obj;
    return Objects.equals(this.items, other.items);
  }
}
