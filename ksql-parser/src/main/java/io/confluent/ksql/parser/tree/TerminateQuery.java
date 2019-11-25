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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.query.QueryId;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class TerminateQuery extends Statement {

  private final QueryId queryId;

  public TerminateQuery(final Optional<NodeLocation> location, QueryId queryId) {
    super(location);
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitTerminateQuery(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TerminateQuery that = (TerminateQuery) o;
    return Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(queryId);
  }

  @Override
  public String toString() {
    return "TerminateQuery{"
        + "queryId=" + queryId
        + '}';
  }
}
