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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class ShowCreate
    extends Statement {

  public enum Type {
    TABLE,
    VIEW
  }

  private final Type type;
  private final QualifiedName name;

  public ShowCreate(final Type type, final QualifiedName name) {
    this(Optional.empty(), type, name);
  }

  public ShowCreate(final NodeLocation location, final Type type, final QualifiedName name) {
    this(Optional.of(location), type, name);
  }

  private ShowCreate(
      final Optional<NodeLocation> location,
      final Type type,
      final QualifiedName name) {
    super(location);
    this.type = requireNonNull(type, "type is null");
    this.name = requireNonNull(name, "name is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowCreate(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final ShowCreate o = (ShowCreate) obj;
    return Objects.equals(name, o.name) && Objects.equals(type, o.type);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("name", name)
        .toString();
  }
}
