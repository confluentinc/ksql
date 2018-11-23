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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public final class RenameTable
    extends Statement {

  private final QualifiedName source;
  private final QualifiedName target;

  public RenameTable(final QualifiedName source, final QualifiedName target) {
    this(Optional.empty(), source, target);
  }

  public RenameTable(
      final NodeLocation location,
      final QualifiedName source,
      final QualifiedName target) {
    this(Optional.of(location), source, target);
  }

  private RenameTable(
      final Optional<NodeLocation> location,
      final QualifiedName source,
      final QualifiedName target) {
    super(location);
    this.source = requireNonNull(source, "source name is null");
    this.target = requireNonNull(target, "target name is null");
  }

  public QualifiedName getSource() {
    return source;
  }

  public QualifiedName getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRenameTable(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final RenameTable o = (RenameTable) obj;
    return Objects.equals(source, o.source)
           && Objects.equals(target, o.target);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("source", source)
        .add("target", target)
        .toString();
  }
}
