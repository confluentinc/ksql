/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.name.Name;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

public final class QueryPlan  {
  private final ImmutableSet<SourceName> sources;
  private final SourceName sink;
  private final ExecutionStep<?> physicalPlan;
  private final QueryId queryId;

  public QueryPlan(
      @JsonProperty(value = "sources", required = true) final Set<SourceName> sources,
      @JsonProperty(value = "sink", required = true) final SourceName sink,
      @JsonProperty(value = "physicalPlan", required = true) final ExecutionStep<?> physicalPlan,
      @JsonProperty(value = "queryId", required = true) final QueryId queryId
  ) {
    this.sources = ImmutableSortedSet.copyOf(
        Comparator.comparing(Name::text),
        Objects.requireNonNull(sources, "sources")
    );
    this.sink = Objects.requireNonNull(sink, "sink");
    this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlan");
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  public SourceName getSink() {
    return sink;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "sources is ImmutableSet")
  public Set<SourceName> getSources() {
    return sources;
  }

  public ExecutionStep<?> getPhysicalPlan() {
    return physicalPlan;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryPlan queryPlan = (QueryPlan) o;
    return Objects.equals(sources, queryPlan.sources)
        && Objects.equals(sink, queryPlan.sink)
        && Objects.equals(physicalPlan, queryPlan.physicalPlan)
        && Objects.equals(queryId, queryPlan.queryId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(sources, sink, physicalPlan, queryId);
  }
}
