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

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;

public class QueryLimitNode extends SingleSourcePlanNode {
  private final int limit;

  public QueryLimitNode(final PlanNodeId id,
                        final PlanNode source,
                        final int limit
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.limit = limit;
  }

  @Override
  public LogicalSchema getSchema() {
    return getSource().getSchema();
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildCtx) {
    return getSource().buildStream(buildCtx);
  }

  public int getLimit() {
    return limit;
  }
}
