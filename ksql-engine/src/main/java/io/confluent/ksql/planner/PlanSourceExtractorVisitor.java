/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner;

import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanVisitor;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import java.util.HashSet;
import java.util.Set;

public class PlanSourceExtractorVisitor<C, R> extends PlanVisitor<C, R> {

  final Set<String> sourceNames;

  public PlanSourceExtractorVisitor() {
    sourceNames = new HashSet<>();
  }

  public R process(final PlanNode node, final C context) {
    return node.accept(this, context);
  }

  protected R visitPlan(final PlanNode node, final C context) {
    return null;
  }

  protected R visitFilter(final FilterNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitProject(final ProjectNode node, final C context) {
    return process(node.getSources().get(0), context);
  }

  protected R visitStructuredDataSourceNode(final StructuredDataSourceNode node, final C context) {
    sourceNames.add(node.getStructuredDataSource().getName());
    return null;
  }

  protected R visitJoin(final JoinNode node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  protected R visitAggregate(final AggregateNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitOutput(final OutputNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  public Set<String> getSourceNames() {
    return sourceNames;
  }
}
