/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;

public class PlanVisitor<C, R> {

  protected R visitPlan(PlanNode node, C context) {
    return null;
  }

  public R visitFilter(FilterNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitProject(ProjectNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitStructuredDataSourceNode(StructuredDataSourceNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOutput(OutputNode node, C context) {
    return visitPlan(node, context);
  }

}