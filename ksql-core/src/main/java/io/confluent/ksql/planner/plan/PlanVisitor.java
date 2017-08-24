/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

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

  public R visitAggregate(AggregateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOutput(OutputNode node, C context) {
    return visitPlan(node, context);
  }

}