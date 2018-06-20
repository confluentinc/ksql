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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Array extends Type {

  private Type itemType;

  public Array(Type itemType) {
    this(Optional.empty(), itemType);
  }

  public Array(NodeLocation location, Type itemType) {
    this(Optional.of(location), itemType);
  }

  private Array(Optional<NodeLocation> location, Type itemType) {
    super(location, KsqlType.ARRAY);
    requireNonNull(itemType, "itemType is null");
    this.itemType = itemType;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitArray(this, context);
  }

  public Type getItemType() {
    return itemType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(itemType);
  }

  @Override
  public boolean equals(Object obj) {
    return
        obj instanceof Array
        && Objects.equals(itemType, ((Array)obj).itemType);
  }
}
