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

public class Map extends Type {

  private final Type valueType;

  public Map(Type valueType) {
    this(Optional.empty(), valueType);
  }

  public Map(NodeLocation location, Type valueType) {
    this(Optional.of(location), valueType);
  }

  private Map(Optional<NodeLocation> location, Type valueType) {
    super(location, KsqlType.MAP);
    requireNonNull(valueType, "itemType is null");
    this.valueType = valueType;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitMap(this, context);
  }

  public Type getValueType() {
    return valueType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType);
  }

  @Override
  public boolean equals(Object obj) {
    return
        obj instanceof Map
        && Objects.equals(valueType, ((Map)obj).valueType);
  }
}
