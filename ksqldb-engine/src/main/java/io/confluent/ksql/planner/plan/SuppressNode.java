/*
 * Copyright 2018 Confluent Inc.
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
import io.confluent.ksql.parser.ResultMaterialization;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;



public class SuppressNode extends SingleSourcePlanNode {

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
        builder.withKsqlConfig(builder.getKsqlConfig()
            .cloneWithPropertyOverwrite(Collections.singletonMap(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")))
    );

    if (!(schemaKStream instanceof SchemaKTable)) {
      throw new RuntimeException("Expected to find a Table, found a stream instead.");
    }

    return (((SchemaKTable<?>) schemaKStream)
        .suppress(
            resultMaterialization,
            contextStacker
        ));
  }
}
