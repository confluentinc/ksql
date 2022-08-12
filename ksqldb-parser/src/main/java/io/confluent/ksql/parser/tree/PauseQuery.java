/*
 * Copyright 2022 Confluent Inc.
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
public final class PauseQuery extends Statement {

  public static final String ALL_QUERIES = "ALL";
  private final Optional<QueryId> queryId;

  public static PauseQuery all(final Optional<NodeLocation> location,
                               final Optional<NodeLocation> endLocation) {
    return new PauseQuery(location, endLocation, Optional.empty());
  }

  public static PauseQuery query(final Optional<NodeLocation> location,
                                 final Optional<NodeLocation> endLocation,
                                 final QueryId queryId) {
    return new PauseQuery(location, endLocation, Optional.of(queryId));
  }

  private PauseQuery(final Optional<NodeLocation> location,
                     final Optional<NodeLocation> endLocation,
                     final Optional<QueryId> queryId) {
    super(location, endLocation);
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  /**
   * @return the id of the query to pause or {@code empty()} if all should be paused.
   */
  public Optional<QueryId> getQueryId() {
    return queryId;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPauseQuery(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PauseQuery that = (PauseQuery) o;
    return Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(queryId);
  }

  @Override
  public String toString() {
    return "PauseQuery{"
        + "queryId=" + queryId
        + '}';
  }
}
