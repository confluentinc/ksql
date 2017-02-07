/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.kql.metastore.StructuredDataSource;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class StructuredDataSourceNode
    extends SourceNode {

  private final Schema schema;
  private final String topicName;
  private final Field keyField;
  private final String alias;
  StructuredDataSource structuredDataSource;

  // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public StructuredDataSourceNode(@JsonProperty("id") final PlanNodeId id,
                                  @JsonProperty("schema") final Schema schema,
                                  @JsonProperty("keyField") final Field keyField,
                                  @JsonProperty("topicName") final String topicName,
                                  @JsonProperty("alias") final String alias,
                                  @JsonProperty("dataSourceType") final StructuredDataSource.DataSourceType
                                      dataSourceType,
                                  @JsonProperty("structuredDataSource") final StructuredDataSource structuredDataSource) {
    super(id, dataSourceType);

    this.schema = schema;
    requireNonNull(topicName, "topicName is null");

    this.topicName = topicName;
    this.keyField = keyField;
    this.alias = alias;
    this.structuredDataSource = structuredDataSource;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  public String getAlias() {
    return alias;
  }

  public StructuredDataSource getStructuredDataSource() {
    return structuredDataSource;
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitStructuredDataSourceNode(this, context);
  }
}
