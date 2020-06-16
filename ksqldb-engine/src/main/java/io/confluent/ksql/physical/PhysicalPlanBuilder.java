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

package io.confluent.ksql.physical;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.QueryIdGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import org.apache.kafka.streams.StreamsBuilder;

public class PhysicalPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final QueryIdGenerator queryIdGenerator;

  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
  }

  public PhysicalPlan buildPhysicalPlan(final LogicalPlanNode logicalPlanNode) {
    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    final KsqlQueryBuilder ksqlQueryBuilder = KsqlQueryBuilder.of(
        builder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );

    final SchemaKStream<?> resultStream = outputNode.buildStream(ksqlQueryBuilder);

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

    return new PhysicalPlan(
        queryId,
        resultStream.getSourceStep()
    );
  }
}
