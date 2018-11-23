/*
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

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Explain
    extends Statement {

  private final Statement statement;
  private final String queryId;
  private final boolean analyze;
  private final List<ExplainOption> options;

  public Explain(
      final String queryId,
      final Statement statement,
      final boolean analyze,
      final List<ExplainOption> options
  ) {
    this(Optional.empty(), analyze, queryId, statement, options);
  }

  public Explain(
      final NodeLocation location,
      final boolean analyze,
      final String queryId,
      final Statement statement,
      final List<ExplainOption> options
  ) {
    this(Optional.of(location), analyze, queryId, statement, options);
  }

  private Explain(
      final Optional<NodeLocation> location,
      final boolean analyze,
      final String queryId,
      final Statement statement,
      final List<ExplainOption> options
  ) {
    super(location);
    this.statement = statement;// requireNonNull(statement, "statement is null");
    this.analyze = analyze;
    this.queryId = queryId;
    if (options == null) {
      this.options = ImmutableList.of();
    } else {
      this.options = ImmutableList.copyOf(options);
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

  public List<ExplainOption> getOptions() {
    return options;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, options, analyze);
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
           && Objects.equals(options, o.options)
           && Objects.equals(analyze, o.analyze);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
        .add("options", options)
        .add("analyze", analyze)
        .toString();
  }
}
