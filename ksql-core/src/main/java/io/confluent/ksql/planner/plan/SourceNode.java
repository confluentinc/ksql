/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.connect.data.Field;

import io.confluent.ksql.metastore.StructuredDataSource;

import javax.annotation.concurrent.Immutable;


@Immutable
public abstract class SourceNode extends PlanNode {

  private final StructuredDataSource.DataSourceType dataSourceType;
  final Field timestampField;

  public SourceNode(@JsonProperty("id") final PlanNodeId id,
                    @JsonProperty("timestampField") final Field timestampField,
                    @JsonProperty("dataSourceType") final StructuredDataSource.DataSourceType dataSourceType) {
    super(id);
    this.dataSourceType = dataSourceType;
    this.timestampField = timestampField;
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public Field getTimestampField() {
    return timestampField;
  }
}
