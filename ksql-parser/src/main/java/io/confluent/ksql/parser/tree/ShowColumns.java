/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ShowColumns extends Statement {

  private final QualifiedName table;
  private final boolean isExtended;

  public ShowColumns(final QualifiedName table, final boolean isExtended) {
    this(Optional.empty(), table, isExtended);
  }

  public ShowColumns(
      final Optional<NodeLocation> location,
      final QualifiedName table,
      final boolean isExtended
  ) {
    super(location);
    this.table = requireNonNull(table, "table");
    this.isExtended = isExtended;
  }

  public QualifiedName getTable() {
    return table;
  }

  public boolean isExtended() {
    return isExtended;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowColumns(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ShowColumns that = (ShowColumns) o;
    return isExtended == that.isExtended
        && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, isExtended);
  }

  @Override
  public String toString() {
    return "ShowColumns{"
        + "table=" + table
        + ", isExtended=" + isExtended
        + '}';
  }
}
