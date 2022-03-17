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

package io.confluent.ksql.execution;

import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.StreamsBuilder;

public class ExecutionPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;

  public ExecutionPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public ExecutionPlan buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final QueryId queryId,
      final Optional<PlanInfo> oldPlanInfo
  ) {
    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    final PlanBuildContext buildContext = PlanBuildContext.of(
        ksqlConfig,
        serviceContext,
        functionRegistry,
        oldPlanInfo
    );

    final SchemaKStream<?> resultStream = outputNode.buildStream(buildContext);

    final LogicalSchema logicalSchema = outputNode.getSchema();
    final LogicalSchema physicalSchema = resultStream.getSchema();
    if (!logicalSchema.equals(physicalSchema)) {
      throw new IllegalStateException("Logical and Physical schemas do not match!"
          + System.lineSeparator()
          + "Logical : " + logicalSchema
          + System.lineSeparator()
          + "Physical: " + physicalSchema
      );
    }

    return new ExecutionPlan(
        queryId,
        resultStream.getSourceStep()
    );
  }
}
