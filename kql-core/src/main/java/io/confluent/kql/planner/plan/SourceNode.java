package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.kql.metastore.StructuredDataSource;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SourceNode extends PlanNode {

  private final StructuredDataSource.DataSourceType dataSourceType;

  public SourceNode(@JsonProperty("id") PlanNodeId id,
                    @JsonProperty("dataSourceType") StructuredDataSource.DataSourceType dataSourceType) {
    super(id);
    this.dataSourceType = dataSourceType;
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }
}
