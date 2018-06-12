/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class QueryDescription {

  private final EntityQueryId id;
  private final String statementText;
  private final List<FieldInfo> fields;
  private final Set<String> sources;
  private final Set<String> sinks;
  private final String topology;
  private final String executionPlan;
  private final Map<String, Object> overriddenProperties;

  @JsonCreator
  public QueryDescription(
      @JsonProperty("id") EntityQueryId id,
      @JsonProperty("statementText") String statementText,
      @JsonProperty("fields") List<FieldInfo> fields,
      @JsonProperty("sources") Set<String> sources,
      @JsonProperty("sinks") Set<String> sinks,
      @JsonProperty("topology") String topology,
      @JsonProperty("executionPlan") String executionPlan,
      @JsonProperty("overriddenProperties") Map<String, Object> overriddenProperties
  ) {
    this.id = id;
    this.statementText = statementText;
    this.fields = Collections.unmodifiableList(fields);
    this.sources = Collections.unmodifiableSet(sources);
    this.sinks = Collections.unmodifiableSet(sinks);
    this.topology = topology;
    this.executionPlan = executionPlan;
    this.overriddenProperties = Collections.unmodifiableMap(overriddenProperties);
  }

  private QueryDescription(String id, QueryMetadata queryMetadata, Set<String> sinks) {
    this(
        new EntityQueryId(id),
        queryMetadata.getStatementString(),
        EntityUtil.buildSourceSchemaEntity(queryMetadata.getResultSchema()),
        queryMetadata.getSourceNames(),
        sinks,
        queryMetadata.getTopologyDescription(),
        queryMetadata.getExecutionPlan(),
        queryMetadata.getOverriddenProperties());
  }

  public static QueryDescription forQueryMetadata(QueryMetadata queryMetadata) {
    if (queryMetadata instanceof PersistentQueryMetadata) {
      return new QueryDescription(
          ((PersistentQueryMetadata) queryMetadata).getQueryId().getId(), queryMetadata,
          ((PersistentQueryMetadata) queryMetadata).getSinkNames());
    }
    return new QueryDescription("", queryMetadata, Collections.emptySet());
  }

  public EntityQueryId getId() {
    return id;
  }

  public String getStatementText() {
    return statementText;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryDescription)) {
      return false;
    }
    QueryDescription that = (QueryDescription) o;
    return Objects.equals(id, that.id)
        && Objects.equals(statementText, that.statementText)
        && Objects.equals(fields, that.fields)
        && Objects.equals(topology, that.topology)
        && Objects.equals(executionPlan, that.executionPlan)
        && Objects.equals(sources, that.sources)
        && Objects.equals(sinks, that.sinks)
        && Objects.equals(overriddenProperties, that.overriddenProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, statementText, fields, topology, executionPlan, sources, sinks, overriddenProperties);
  }
}
