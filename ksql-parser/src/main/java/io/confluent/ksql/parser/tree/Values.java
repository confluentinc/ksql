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

import static java.util.Objects.requireNonNull;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class Values
    extends QueryBody {

  private final List<Expression> rows;

  public Values(final List<Expression> rows) {
    this(Optional.empty(), rows);
  }

  public Values(final NodeLocation location, final List<Expression> rows) {
    this(Optional.of(location), rows);
  }

  private Values(final Optional<NodeLocation> location, final List<Expression> rows) {
    super(location);
    requireNonNull(rows, "rows is null");
    this.rows = ImmutableList.copyOf(rows);
  }

  public List<Expression> getRows() {
    return rows;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitValues(this, context);
  }

  @Override
  public String toString() {
    return "(" + Joiner.on(", ").join(rows) + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(rows);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Values other = (Values) obj;
    return Objects.equals(this.rows, other.rows);
  }
}
