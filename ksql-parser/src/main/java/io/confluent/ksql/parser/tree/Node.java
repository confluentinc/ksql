/*
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

import java.util.Optional;

public abstract class Node {

  private final Optional<NodeLocation> location;

  private Optional<Node> parent = Optional.empty();

  protected Node(final Optional<NodeLocation> location) {
    this.location = requireNonNull(location, "location is null");
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitNode(this, context);
  }

  public Optional<NodeLocation> getLocation() {
    return location;
  }

  public Optional<Node> getParent() {
    return parent;
  }

  public void setParent(final Optional<Node> parent) {
    this.parent = parent;
  }
  
  public void setParent(final Node parent) {
    this.parent = Optional.ofNullable(parent);
  }

  // Force subclasses to have a proper equals and hashcode implementation
  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();
}
