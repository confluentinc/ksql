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

public class Query
    extends Statement {

  private final QueryBody queryBody;
  private final Optional<Integer> limit;

  public Query(
      final QueryBody queryBody,
      final Optional<Integer> limit) {
    this(Optional.empty(), queryBody, limit);
  }

  public Query(
      final NodeLocation location,
      final QueryBody queryBody,
      final Optional<Integer> limit) {
    this(Optional.of(location), queryBody, limit);
  }

  private Query(
      final Optional<NodeLocation> location,
      final QueryBody queryBody,
      final Optional<Integer> limit) {
    super(location);
    requireNonNull(queryBody, "queryBody is null");
    requireNonNull(limit, "limit is null");

    this.queryBody = queryBody;
    this.limit = limit;
  }

  public QueryBody getQueryBody() {
    return queryBody;
  }

  public Optional<Integer> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitQuery(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("queryBody", queryBody)
        .add("limit", limit.orElse(null))
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
    return Objects.equals(queryBody, o.queryBody)
           && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryBody, limit);
  }
}
