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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;

/**
 * Node to handle an implicit repartition required to enable a join.
 *
 * <p>Any join that is not on the key of the stream requires ksql to perform an
 * implicit repartition step before joining.
 */
public class PreJoinRepartitionNode extends SingleSourcePlanNode {

  private final Expression partitionBy;
  private final LogicalSchema schema;

  public PreJoinRepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression partitionBy
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);
    this.schema = requireNonNull(schema, "schema");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .selectKey(
            partitionBy,
            builder.buildNodeContext(getId().toString())
        );
  }
}
