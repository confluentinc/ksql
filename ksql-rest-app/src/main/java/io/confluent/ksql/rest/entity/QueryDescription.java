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

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.ksql.util.SchemaUtil;

public class QueryDescription {

  private final QueryId id;
  private final String statementText;
  private final List<FieldSchemaInfo> schema;
  private final Set<String> sources;
  private final Set<String> sinks;
  private final String topology;
  private final String executionPlan;
  private final Map<String, Object> overriddenProperties;

  @JsonCreator
  public QueryDescription(
      @JsonProperty("id") QueryId id,
      @JsonProperty("statementText") String statementText,
      @JsonProperty("schema") List<FieldSchemaInfo> schema,
      @JsonProperty("sources") Set<String> sources,
      @JsonProperty("sinks") Set<String> sinks,
      @JsonProperty("topology") String topology,
      @JsonProperty("executionPlan") String executionPlan,
      @JsonProperty("overriddenProperties") Map<String, Object> overriddenProperties
  ) {
    this.id = id;
    this.statementText = statementText;
    this.schema = Collections.unmodifiableList(schema);
    this.sources = Collections.unmodifiableSet(sources);
    this.sinks = Collections.unmodifiableSet(sinks);
    this.topology = topology;
    this.executionPlan = executionPlan;
    this.overriddenProperties = Collections.unmodifiableMap(overriddenProperties);
  }

  private QueryDescription(QueryId queryId, QueryMetadata queryMetadata) {
    this(
        queryId,
        queryMetadata.getStatementString(),
        queryMetadata.getResultSchema().fields().stream().map(
            field -> new FieldSchemaInfo(field.name(), SchemaUtil.getSchemaFieldName(field))
        ).collect(Collectors.toList()),
        queryMetadata.getSourceNames(),
        Collections.emptySet(),
        queryMetadata.getTopologyDescription(),
        queryMetadata.getExecutionPlan(),
        queryMetadata.getOverriddenProperties());
  }

  public static QueryDescription forQueryMetadata(QueryMetadata queryMetadata) {
    if (queryMetadata instanceof PersistentQueryMetadata) {
      return new QueryDescription(
          ((PersistentQueryMetadata) queryMetadata).getQueryId(), queryMetadata);
    }
    return new QueryDescription(new QueryId(""), queryMetadata);
  }

  public QueryId getId() {
    return id;
  }

  public String getStatementText() {
    return statementText;
  }

  public List<FieldSchemaInfo> getSchema() {
    return schema;
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
        && Objects.equals(schema, that.schema)
        && Objects.equals(topology, that.topology)
        && Objects.equals(executionPlan, that.executionPlan)
        && Objects.equals(sources, that.sources)
        && Objects.equals(sinks, that.sinks)
        && Objects.equals(overriddenProperties, that.overriddenProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, statementText, schema, topology, executionPlan, sources, sinks, overriddenProperties);
  }
}
