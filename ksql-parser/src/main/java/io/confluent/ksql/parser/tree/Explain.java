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

import java.util.Objects;
import java.util.Optional;

public class Explain
    extends Statement {

  private final Statement statement;
  private final String queryId;
  private final boolean analyze;

  public Explain(
      final String queryId,
      final Statement statement,
      final boolean analyze
  ) {
    this(Optional.empty(), analyze, queryId, statement);
  }

  private Explain(
      final Optional<NodeLocation> location,
      final boolean analyze,
      final String queryId,
      final Statement statement
  ) {
    super(location);
    this.statement = statement;
    this.analyze = analyze;
    this.queryId = queryId;

    if (statement == null && queryId == null) {
      throw new NullPointerException("Must supply either queryId or statement");
    }
  }

  public Statement getStatement() {
    return statement;
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean isAnalyze() {
    return analyze;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, analyze);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Explain o = (Explain) obj;
    return Objects.equals(statement, o.statement)
           && Objects.equals(analyze, o.analyze);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
        .add("analyze", analyze)
        .toString();
  }
}
