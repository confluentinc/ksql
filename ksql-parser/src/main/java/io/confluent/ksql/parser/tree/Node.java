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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Node {

  private final Optional<NodeLocation> location;

  private Optional<Node> parent = Optional.empty();

  protected Node(Optional<NodeLocation> location) {
    this.location = requireNonNull(location, "location is null");
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  protected <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  public Optional<NodeLocation> getLocation() {
    return location;
  }

  public Optional<Node> getParent() {
    return parent;
  }

  public void setParent(Optional<Node> parent) {
    this.parent = parent;
  }
  
  public void setParent(Node parent) {
    if (parent == null) {
      this.parent = Optional.empty();
    } else {
      this.parent = Optional.of(parent);
    }
  }

  // Force subclasses to have a proper equals and hashcode implementation
  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();
}
