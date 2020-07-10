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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.ResultMaterialization;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import java.util.Objects;

import io.confluent.ksql.util.KsqlException;


/**
 The suppress node is a plan node that is added to the logical plan if the user specifies in their
 ksqlDB query that the result materialization should be EMIT FINAL. The physical plan is then built
 and executed using the logical plan, and executes all of the plan nodes that were added from the
 logical plan including the suppress node if it was added. Currently the suppress node needs to be
 added at the end of the logical plan right before we build the output node, this is so that we can
 suppress results that may need to be aggregated or altered somehow. Using a suppress node also
 allows for more flexibility in the future in terms of enhancements or different types of
 suppression being supported.
 */
public class SuppressNode extends SingleSourcePlanNode implements VerifiableNode {

  private final ResultMaterialization resultMaterialization;

  public SuppressNode(
      final PlanNodeId id,
      final PlanNode source,
      final ResultMaterialization resultMaterialization
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    this.resultMaterialization = Objects.requireNonNull(
        resultMaterialization, "resultMaterialization");
  }

  public ResultMaterialization getResultMaterialization() {
    return resultMaterialization;
  }

  @Override
  public LogicalSchema getSchema() {
    return getSource().getSchema();
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());
    final SchemaKStream<?> schemaKStream = getSource().buildStream(
        builder.withKsqlConfig(builder.getKsqlConfig())
    );

    if (!(schemaKStream instanceof SchemaKTable)) {
      throw new KsqlException("Failed to build suppress node. Expected to find a Table, but "
          + "found a stream named " + schemaKStream.toString() + " instead.");
    }

    return (((SchemaKTable<?>) schemaKStream)
        .suppress(
            resultMaterialization,
            contextStacker
        ));
  }

  @Override
  public void validateKeyPresent(final SourceName sinkName) {
  }
}
