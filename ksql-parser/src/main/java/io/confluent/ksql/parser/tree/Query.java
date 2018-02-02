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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Query
    extends Statement {

  private final QueryBody queryBody;
  private final Optional<String> limit;

  public Query(
      QueryBody queryBody,
      Optional<String> limit) {
    this(Optional.empty(), queryBody, limit);
  }

  public Query(
      NodeLocation location,
      QueryBody queryBody,
      Optional<String> limit) {
    this(Optional.of(location), queryBody, limit);
  }

  private Query(
      Optional<NodeLocation> location,
      QueryBody queryBody,
      Optional<String> limit) {
    super(location);
    requireNonNull(queryBody, "queryBody is null");
    requireNonNull(limit, "limit is null");

    this.queryBody = queryBody;
    this.limit = limit;
  }

  public QueryBody getQueryBody() {
    return queryBody;
  }

  public Optional<String> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Query o = (Query) obj;
    return Objects.equals(queryBody, o.queryBody)
           && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryBody, limit);
  }
}
