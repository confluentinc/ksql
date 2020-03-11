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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.query.QueryId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryDescription {

  private final QueryId id;
  private final String statementText;
  private final Optional<WindowType> windowType;
  private final List<FieldInfo> fields;
  private final Set<String> sources;
  private final Set<String> sinks;
  private final String topology;
  private final String executionPlan;
  private final Map<String, Object> overriddenProperties;
  private final Optional<String> state;

  @SuppressWarnings("WeakerAccess") // Invoked via reflection
  @JsonCreator
  public QueryDescription(
      @JsonProperty("id") final QueryId id,
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("windowType") final Optional<WindowType> windowType,
      @JsonProperty("fields") final List<FieldInfo> fields,
      @JsonProperty("sources") final Set<String> sources,
      @JsonProperty("sinks") final Set<String> sinks,
      @JsonProperty("topology") final String topology,
      @JsonProperty("executionPlan") final String executionPlan,
      @JsonProperty("overriddenProperties") final Map<String, Object> overriddenProperties,
      @JsonProperty("state") final Optional<String> state
  ) {
    this.id = Objects.requireNonNull(id, "id");
    this.statementText = Objects.requireNonNull(statementText, "statementText");
    this.windowType = Objects.requireNonNull(windowType, "windowType");
    this.fields = ImmutableList.copyOf(Objects.requireNonNull(fields, "fields"));
    this.sources = ImmutableSet.copyOf(Objects.requireNonNull(sources, "sources"));
    this.sinks = ImmutableSet.copyOf(Objects.requireNonNull(sinks, "sinks"));
    this.topology = Objects.requireNonNull(topology, "topology");
    this.executionPlan = Objects.requireNonNull(executionPlan, "executionPlan");
    this.overriddenProperties = ImmutableMap.copyOf(Objects
        .requireNonNull(overriddenProperties, "overriddenProperties"));
    this.state = Objects.requireNonNull(state, "state");
  }

  public QueryId getId() {
    return id;
  }

  public String getStatementText() {
    return statementText;
  }

  public Optional<WindowType> getWindowType() {
    return windowType;
  }

  public List<FieldInfo> getFields() {
    return fields;
  }

  public String getTopology() {
    return topology;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  public Set<String> getSources() {
    return sources;
  }

  public Set<String> getSinks() {
    return sinks;
  }

  public Map<String, Object> getOverriddenProperties() {
    return overriddenProperties;
  }

  public Optional<String> getState() {
    return state;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryDescription)) {
      return false;
    }
    final QueryDescription that = (QueryDescription) o;
    return Objects.equals(id, that.id)
        && Objects.equals(statementText, that.statementText)
        && Objects.equals(windowType, that.windowType)
        && Objects.equals(fields, that.fields)
        && Objects.equals(topology, that.topology)
        && Objects.equals(executionPlan, that.executionPlan)
        && Objects.equals(sources, that.sources)
        && Objects.equals(sinks, that.sinks)
        && Objects.equals(overriddenProperties, that.overriddenProperties)
        && Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        statementText,
        windowType,
        fields,
        topology,
        executionPlan,
        sources,
        sinks,
        overriddenProperties,
        state
    );
  }
}
