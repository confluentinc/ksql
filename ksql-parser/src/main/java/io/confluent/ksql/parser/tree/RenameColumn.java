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

public class RenameColumn
    extends Statement {

  private final QualifiedName table;
  private final String source;
  private final String target;

  public RenameColumn(final QualifiedName table, final String source, final String target) {
    this(Optional.empty(), table, source, target);
  }

  public RenameColumn(
      final NodeLocation location,
      final QualifiedName table,
      final String source,
      final String target) {
    this(Optional.of(location), table, source, target);
  }

  private RenameColumn(
      final Optional<NodeLocation> location, final QualifiedName table, final String source,
                       final String target) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.source = requireNonNull(source, "source is null");
    this.target = requireNonNull(target, "target is null");
  }

  public QualifiedName getTable() {
    return table;
  }

  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRenameColumn(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RenameColumn that = (RenameColumn) o;
    return Objects.equals(table, that.table)
           && Objects.equals(source, that.source)
           && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, source, target);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("source", source)
        .add("target", target)
        .toString();
  }
}
