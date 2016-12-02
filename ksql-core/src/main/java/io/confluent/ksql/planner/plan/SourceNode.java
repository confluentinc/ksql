package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.ksql.metastore.DataSource;

import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SourceNode extends PlanNode {

  private final DataSource.DataSourceType dataSourceType;

  public SourceNode(@JsonProperty("id") PlanNodeId id,
                    @JsonProperty("dataSourceType") DataSource.DataSourceType dataSourceType) {
    super(id);
    this.dataSourceType = dataSourceType;
  }

  public DataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }
}
