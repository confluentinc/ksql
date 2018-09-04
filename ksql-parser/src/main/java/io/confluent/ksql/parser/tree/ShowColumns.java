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

public class ShowColumns
    extends Statement {

  private final QualifiedName table;
  private final boolean isTopic;
  private final boolean isExtended;

  public ShowColumns(final QualifiedName table, final boolean isTopic, final boolean isExtended) {
    this(Optional.empty(), table, isTopic, isExtended);
  }

  public ShowColumns(
      final NodeLocation location,
      final QualifiedName table,
      final boolean isTopic,
      final boolean isExtended
  ) {
    this(Optional.of(location), table, isTopic, isExtended);
  }

  private ShowColumns(
      final Optional<NodeLocation> location,
      final QualifiedName table,
      final boolean isTopic,
      final boolean isExtended
  ) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.isTopic = isTopic;
    this.isExtended = isExtended;
  }

  public QualifiedName getTable() {
    return table;
  }

  public boolean isTopic() {
    return isTopic;
  }

  public boolean isExtended() {
    return isExtended;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowColumns(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final ShowColumns o = (ShowColumns) obj;
    return Objects.equals(table, o.table);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .toString();
  }
}
