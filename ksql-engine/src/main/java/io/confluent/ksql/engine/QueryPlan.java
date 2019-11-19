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
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.PhysicalPlan;
import java.util.Objects;
import java.util.Set;

public final class QueryPlan  {
  private final Set<SourceName> sources;
  private final SourceName sink;
  private final PhysicalPlan physicalPlan;

  public QueryPlan(
      @JsonProperty(value = "sources", required = true) final Set<SourceName> sources,
      @JsonProperty(value = "sink", required = true) final SourceName sink,
      @JsonProperty(value = "physicalPlan", required = true) final PhysicalPlan physicalPlan
  ) {
    this.sources = Objects.requireNonNull(sources, "sources");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlan");
  }

  public SourceName getSink() {
    return sink;
  }

  public Set<SourceName> getSources() {
    return sources;
  }

  public PhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }
}
