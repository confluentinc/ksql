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

package io.confluent.ksql.planner;

import java.util.HashSet;
import java.util.Set;

import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanVisitor;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;

public class PlanSourceExtractorVisitor<C, R> extends PlanVisitor<C, R> {

  final Set<String> sourceNames;

  public PlanSourceExtractorVisitor() {
    sourceNames = new HashSet<>();
  }

  public R process(PlanNode node, C context) {
    return node.accept(this, context);
  }

  protected R visitPlan(PlanNode node, C context) {
    return null;
  }

  protected R visitFilter(FilterNode node, C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitProject(ProjectNode node, C context) {
    return process(node.getSources().get(0), context);
  }

  protected R visitStructuredDataSourceNode(StructuredDataSourceNode node, C context) {
    sourceNames.add(node.getStructuredDataSource().getName());
    return null;
  }

  protected R visitJoin(JoinNode node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  protected R visitAggregate(AggregateNode node, C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitOutput(OutputNode node, C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  public Set<String> getSourceNames() {
    return sourceNames;
  }
}
