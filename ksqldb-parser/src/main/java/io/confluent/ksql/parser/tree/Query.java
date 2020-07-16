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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlPreconditions;
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
  private final Optional<PartitionBy> partitionBy;
  private final Optional<Expression> having;
  private final Optional<RefinementInfo> refinement;
  private final boolean pullQuery;
  private final OptionalInt limit;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public Query(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final Optional<NodeLocation> location,
      final Select select,
      final Relation from,
      final Optional<WindowExpression> window,
      final Optional<Expression> where,
      final Optional<GroupBy> groupBy,
      final Optional<PartitionBy> partitionBy,
      final Optional<Expression> having,
      final Optional<RefinementInfo> refinement,
      final boolean pullQuery,
      final OptionalInt limit
  ) {
    super(location);
    this.select = requireNonNull(select, "select");
    this.from = requireNonNull(from, "from");
    this.window = requireNonNull(window, "window");
    this.where = requireNonNull(where, "where");
    this.groupBy = requireNonNull(groupBy, "groupBy");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
    this.having = requireNonNull(having, "having");
    this.refinement = requireNonNull(refinement, "refinement");
    this.pullQuery = pullQuery;
    this.limit = requireNonNull(limit, "limit");

    KsqlPreconditions.checkArgument(
        !(partitionBy.isPresent() && groupBy.isPresent()),
        "Queries only support one of PARTITION BY and GROUP BY");
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

  public Optional<PartitionBy> getPartitionBy() {
    return partitionBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public Optional<RefinementInfo> getRefinement() {
    return refinement;
  }

  public boolean isPullQuery() {
    return pullQuery;
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
        .add("partitionBy", partitionBy.orElse(null))
        .add("having", having.orElse(null))
        .add("refinement", refinement)
        .add("pullQuery", pullQuery)
        .add("limit", limit)
        .omitNullValues()
        .toString();
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object obj) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity

    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Query o = (Query) obj;
    return pullQuery == o.pullQuery
        && Objects.equals(select, o.select)
        && Objects.equals(from, o.from)
        && Objects.equals(where, o.where)
        && Objects.equals(window, o.window)
        && Objects.equals(groupBy, o.groupBy)
        && Objects.equals(partitionBy, o.partitionBy)
        && Objects.equals(having, o.having)
        && Objects.equals(refinement, o.refinement)
        && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        select,
        from,
        where,
        window,
        groupBy,
        having,
        refinement,
        pullQuery,
        limit
    );
  }
}
