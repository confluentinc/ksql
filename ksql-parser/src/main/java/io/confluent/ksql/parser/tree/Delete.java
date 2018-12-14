/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;

public class Delete
    extends Statement {

  private final Table table;
  private final Optional<Expression> where;

  public Delete(final Table table, final Optional<Expression> where) {
    this(Optional.empty(), table, where);
  }

  public Delete(final NodeLocation location, final Table table, final Optional<Expression> where) {
    this(Optional.of(location), table, where);
  }

  private Delete(
      final Optional<NodeLocation> location,
      final Table table,
      final Optional<Expression> where) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.where = requireNonNull(where, "where is null");
  }

  public Table getTable() {
    return table;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDelete(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, where);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Delete o = (Delete) obj;
    return Objects.equals(table, o.table)
           && Objects.equals(where, o.where);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table.getName())
        .add("where", where)
        .toString();
  }
}
