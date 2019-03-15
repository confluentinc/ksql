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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

@Immutable
public class Query extends Statement {

  private final Select select;
  private final Relation from;
  private final Optional<WindowExpression> window;
  private final Optional<Expression> where;
  private final Optional<GroupBy> groupBy;
  private final Optional<Expression> having;
  private final OptionalInt limit;

  public Query(
      final Select select,
      final Relation from,
      final Optional<WindowExpression> window,
      final Optional<Expression> where,
      final Optional<GroupBy> groupBy,
      final Optional<Expression> having,
      final OptionalInt limit
  ) {
    this(Optional.empty(), select, from, window, where, groupBy, having, limit);
  }

  public Query(
      final Optional<NodeLocation> location,
      final Select select,
      final Relation from,
      final Optional<WindowExpression> window,
      final Optional<Expression> where,
      final Optional<GroupBy> groupBy,
      final Optional<Expression> having,
      final OptionalInt limit
  ) {
    super(location);
    this.select = requireNonNull(select, "select");
    this.from = requireNonNull(from, "from");
    this.window = requireNonNull(window, "window");
    this.where = requireNonNull(where, "where");
    this.groupBy = requireNonNull(groupBy, "groupBy");
    this.having = requireNonNull(having, "having");
    this.limit = requireNonNull(limit, "limit");
  }

  public Select getSelect() {
    return select;
  }

  public Relation getFrom() {
    return from;
  }

  public Optional<WindowExpression> getWindow() {
    return window;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public Optional<GroupBy> getGroupBy() {
    return groupBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public OptionalInt getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitQuery(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("select", select)
        .add("from", from)
        .add("window", window.orElse(null))
        .add("where", where.orElse(null))
        .add("groupBy", groupBy.orElse(null))
        .add("having", having.orElse(null))
        .add("limit", limit)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Query o = (Query) obj;
    return Objects.equals(select, o.select)
        && Objects.equals(from, o.from)
        && Objects.equals(where, o.where)
        && Objects.equals(window, o.window)
        && Objects.equals(groupBy, o.groupBy)
        && Objects.equals(having, o.having)
        && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(select, from, where, window, groupBy, having, limit);
  }
}
