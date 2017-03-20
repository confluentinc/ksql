/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class PlanNode {

  private final PlanNodeId id;

  protected PlanNode(final PlanNodeId id) {
    requireNonNull(id, "id is null");
    this.id = id;
  }

  @JsonProperty("id")
  public PlanNodeId getId() {
    return id;
  }

  public abstract Schema getSchema();

  public abstract Field getKeyField();

  public abstract List<PlanNode> getSources();

  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitPlan(this, context);
  }

  public StructuredDataSourceNode getTheSourceNode() {
    if (this instanceof StructuredDataSourceNode) {
      return (StructuredDataSourceNode) this;
    } else if (this.getSources() != null && !this.getSources().isEmpty()) {
      return this.getSources().get(0).getTheSourceNode();
    }
    return null;
  }
}
