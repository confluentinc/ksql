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

public class AllColumns
    extends SelectItem {

  private final Optional<QualifiedName> prefix;

  public AllColumns(NodeLocation location) {
    super(Optional.of(location));
    prefix = Optional.empty();
  }

  public AllColumns(NodeLocation location, QualifiedName prefix) {
    this(Optional.of(location), prefix);
  }

  private AllColumns(Optional<NodeLocation> location, QualifiedName prefix) {
    super(location);
    requireNonNull(prefix, "prefix is null");
    this.prefix = Optional.of(prefix);
  }

  public Optional<QualifiedName> getPrefix() {
    return prefix;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAllColumns(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AllColumns that = (AllColumns) o;
    return Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return prefix.hashCode();
  }

  @Override
  public String toString() {
    return prefix.map(qualifiedName -> qualifiedName + ".*").orElse("*");

  }
}
