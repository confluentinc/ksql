/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;

/**
 * The user supplied projection.
 *
 * <p>Used by all plans except those with GROUP BY, which has its own custom projection handling
 * in {@link AggregateNode}.
 */
public class FinalProjectNode extends ProjectNode implements VerifiableNode {

  private final Projection projection;

  public FinalProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectExpression> projectExpressions,
      final LogicalSchema schema,
      final List<SelectItem> selectItems
  ) {
    super(id, source, projectExpressions, schema, false);
    this.projection = Projection.of(selectItems);
  }

  @Override
  public void validateKeyPresent(final SourceName sinkName) {
    getSource().validateKeyPresent(sinkName, projection);
  }
}
