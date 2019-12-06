/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.transform.select.Selection;
import io.confluent.ksql.execution.util.SinkSchemaUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;
import java.util.Optional;

/**
 * Computes the schema produced by an execution step, given the schema(s) going into the step.
 */
public final class StepSchemaResolver {
  private static final HandlerMaps.ClassHandlerMapR2
      <ExecutionStep, StepSchemaResolver, LogicalSchema, LogicalSchema> HANDLERS
      = HandlerMaps.forClass(ExecutionStep.class)
      .withArgTypes(StepSchemaResolver.class, LogicalSchema.class)
      .withReturnType(LogicalSchema.class)
      .put(StreamAggregate.class, StepSchemaResolver::handleStreamAggregate)
      .put(StreamWindowedAggregate.class, StepSchemaResolver::handleStreamWindowedAggregate)
      .put(StreamFilter.class, StepSchemaResolver::sameSchema)
      .put(StreamFlatMap.class, StepSchemaResolver::handleStreamFlatMap)
      .put(StreamGroupBy.class, StepSchemaResolver::sameSchema)
      .put(StreamGroupByKey.class, StepSchemaResolver::sameSchema)
      .put(StreamSelect.class, StepSchemaResolver::handleStreamSelect)
      .put(StreamSelectKey.class, StepSchemaResolver::sameSchema)
      .put(StreamSink.class, StepSchemaResolver::handleSink)
      .put(StreamSource.class, StepSchemaResolver::handleSource)
      .put(WindowedStreamSource.class, StepSchemaResolver::handleSource)
      .put(TableAggregate.class, StepSchemaResolver::handleTableAggregate)
      .put(TableFilter.class, StepSchemaResolver::sameSchema)
      .put(TableGroupBy.class, StepSchemaResolver::sameSchema)
      .put(TableSelect.class, StepSchemaResolver::handleTableSelect)
      .put(TableSink.class, StepSchemaResolver::handleSink)
      .put(TableSource.class, StepSchemaResolver::handleSource)
      .put(WindowedTableSource.class, StepSchemaResolver::handleSource)
      .build();

  private static final HandlerMaps.ClassHandlerMapR2
      <ExecutionStep, StepSchemaResolver, JoinSchemas, LogicalSchema> JOIN_HANDLERS
      = HandlerMaps.forClass(ExecutionStep.class)
      .withArgTypes(StepSchemaResolver.class, JoinSchemas.class)
      .withReturnType(LogicalSchema.class)
      .put(StreamTableJoin.class, StepSchemaResolver::handleJoin)
      .put(StreamStreamJoin.class, StepSchemaResolver::handleJoin)
      .put(TableTableJoin.class, StepSchemaResolver::handleJoin)
      .build();

  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;

  public StepSchemaResolver(
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  /**
   * Compute the output schema of a given execution step, given the input schema.
   * @param step The step to compute the output schema for.
   * @param schema The schema of the data going into the step.
   * @return The schema of the data outputted by the step.
   */
  public LogicalSchema resolve(final ExecutionStep<?> step, final LogicalSchema schema) {
    return Optional.ofNullable(HANDLERS.get(step.getClass()))
        .map(h -> h.handle(this, schema, step))
        .orElseThrow(() -> new IllegalStateException("Unhandled step class: " + step.getClass()));
  }

  /**
   * Compute the output schema of an execution step that defines a join.
   * @param step The step to compute the output schema for.
   * @param left The schema of the left side of the join.
   * @param right The schema of the right side of the join.
   * @return The schema of the data outputted by the step.
   */
  public LogicalSchema resolve(
      final ExecutionStep<?> step,
      final LogicalSchema left,
      final LogicalSchema right) {
    return Optional.ofNullable(JOIN_HANDLERS.get(step.getClass()))
        .map(h -> h.handle(this, new JoinSchemas(left, right), step))
        .orElseThrow(() -> new IllegalStateException("Unhandled step class: " + step.getClass()));
  }

  private LogicalSchema handleStreamAggregate(
      final LogicalSchema schema,
      final StreamAggregate step) {
    return new AggregateParamsFactory().create(
        schema,
        step.getNonFuncColumnCount(),
        functionRegistry,
        step.getAggregationFunctions()
    ).getSchema();
  }

  private LogicalSchema handleStreamWindowedAggregate(
      final LogicalSchema schema,
      final StreamWindowedAggregate step
  ) {
    return new AggregateParamsFactory().create(
        schema,
        step.getNonFuncColumnCount(),
        functionRegistry,
        step.getAggregationFunctions()
    ).getSchema();
  }

  private LogicalSchema handleStreamFlatMap(
      final LogicalSchema schema,
      final StreamFlatMap<?> streamFlatMap
  ) {
    return StreamFlatMapBuilder.buildSchema(
        schema,
        streamFlatMap.getTableFunctions(),
        functionRegistry
    );
  }

  private LogicalSchema handleStreamSelect(
      final LogicalSchema schema,
      final StreamSelect<?> streamSelect
  ) {
    return Selection.of(
        schema,
        streamSelect.getSelectExpressions(),
        ksqlConfig,
        functionRegistry
    ).getSchema();
  }

  private LogicalSchema handleSource(
      final LogicalSchema schema,
      final AbstractStreamSource<?> step) {
    return schema.withAlias(step.getAlias()).withMetaAndKeyColsInValue();
  }

  private LogicalSchema handleJoin(final JoinSchemas schemas, final ExecutionStep step) {
    return JoinParamsFactory.createSchema(schemas.left, schemas.right);
  }

  private LogicalSchema handleTableAggregate(
      final LogicalSchema schema,
      final TableAggregate step
  ) {
    return new AggregateParamsFactory().create(
        schema,
        step.getNonFuncColumnCount(),
        functionRegistry,
        step.getAggregationFunctions()
    ).getSchema();
  }

  private LogicalSchema handleTableSelect(
      final LogicalSchema schema,
      final TableSelect<?> step
  ) {
    return Selection.of(
        schema,
        step.getSelectExpressions(),
        ksqlConfig,
        functionRegistry
    ).getSchema();
  }

  private LogicalSchema handleSink(final LogicalSchema schema, final ExecutionStep<?> step) {
    return SinkSchemaUtil.sinkSchema(schema);
  }

  private LogicalSchema sameSchema(final LogicalSchema schema, final ExecutionStep step) {
    return schema;
  }

  private static final class JoinSchemas {
    private final LogicalSchema left;
    private final LogicalSchema right;

    private JoinSchemas(final LogicalSchema left, final LogicalSchema right) {
      this.left = Objects.requireNonNull(left, "left");
      this.right = Objects.requireNonNull(right, "right");
    }
  }
}
