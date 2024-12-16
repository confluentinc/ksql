/*
 * Copyright 2022 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.plan.StreamGroupByV1;
import io.confluent.ksql.execution.plan.StreamSelect;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSelectKeyV1;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableGroupByV1;
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.transform.select.Selection;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Computes the schema produced by an execution step, given the schema(s) going into the step.
 */
@SuppressWarnings("MethodMayBeStatic") // Methods can not be used in HANDLERS is static.
public final class StepSchemaResolver {
  @SuppressWarnings("rawtypes")
  private static final HandlerMaps.ClassHandlerMapR2
      <ExecutionStep, StepSchemaResolver, LogicalSchema, LogicalSchema> HANDLERS
          = HandlerMaps.forClass(ExecutionStep.class)
      .withArgTypes(StepSchemaResolver.class, LogicalSchema.class)
      .withReturnType(LogicalSchema.class)
      .put(StreamAggregate.class, StepSchemaResolver::handleStreamAggregate)
      .put(StreamWindowedAggregate.class, StepSchemaResolver::handleStreamWindowedAggregate)
      .put(StreamFilter.class, StepSchemaResolver::sameSchema)
      .put(StreamFlatMap.class, StepSchemaResolver::handleStreamFlatMap)
      .put(StreamGroupByV1.class, StepSchemaResolver::handleStreamGroupByV1)
      .put(StreamGroupBy.class, StepSchemaResolver::handleStreamGroupBy)
      .put(StreamGroupByKey.class, StepSchemaResolver::sameSchema)
      .put(StreamSelect.class, StepSchemaResolver::handleStreamSelect)
      .put(StreamSelectKeyV1.class, StepSchemaResolver::handleStreamSelectKeyV1)
      .put(StreamSelectKey.class, StepSchemaResolver::handleStreamSelectKey)
      .put(StreamSink.class, StepSchemaResolver::sameSchema)
      .put(StreamSource.class, StepSchemaResolver::handleSource)
      .put(WindowedStreamSource.class, StepSchemaResolver::handleWindowedSource)
      .put(TableAggregate.class, StepSchemaResolver::handleTableAggregate)
      .put(TableFilter.class, StepSchemaResolver::sameSchema)
      .put(TableGroupByV1.class, StepSchemaResolver::handleTableGroupByV1)
      .put(TableGroupBy.class, StepSchemaResolver::handleTableGroupBy)
      .put(TableSelect.class, StepSchemaResolver::handleTableSelect)
      .put(TableSelectKey.class, StepSchemaResolver::handleTableSelectKey)
      .put(TableSuppress.class, StepSchemaResolver::sameSchema)
      .put(TableSink.class, StepSchemaResolver::sameSchema)
      .put(TableSourceV1.class, StepSchemaResolver::handleSource)
      .put(TableSource.class, StepSchemaResolver::handleSource)
      .put(WindowedTableSource.class, StepSchemaResolver::handleWindowedSource)
      .build();

  @SuppressWarnings("rawtypes")
  private static final HandlerMaps.ClassHandlerMapR2
      <ExecutionStep, StepSchemaResolver, JoinSchemas, LogicalSchema> JOIN_HANDLERS
          = HandlerMaps.forClass(ExecutionStep.class)
      .withArgTypes(StepSchemaResolver.class, JoinSchemas.class)
      .withReturnType(LogicalSchema.class)
      .put(StreamTableJoin.class, StepSchemaResolver::handleStreamTableJoin)
      .put(StreamStreamJoin.class, StepSchemaResolver::handleStreamStreamJoin)
      .put(TableTableJoin.class, StepSchemaResolver::handleTableTableJoin)
      .put(ForeignKeyTableTableJoin.class, StepSchemaResolver::handleForeignKeyTableTableJoin)
      .build();

  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
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
      final StreamAggregate step
  ) {
    return buildAggregateSchema(
        schema,
        step.getNonAggregateColumns(),
        step.getAggregationFunctions(),
        false
    );
  }

  private LogicalSchema handleStreamWindowedAggregate(
      final LogicalSchema schema,
      final StreamWindowedAggregate step
  ) {
    return buildAggregateSchema(
        schema,
        step.getNonAggregateColumns(),
        step.getAggregationFunctions(),
        true
    );
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

  private LogicalSchema handleStreamGroupByV1(
      final LogicalSchema sourceSchema,
      final StreamGroupByV1<?> streamGroupBy
  ) {
    final List<CompiledExpression> compiledGroupBy = CodeGenRunner.compileExpressions(
        streamGroupBy.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        ksqlConfig,
        functionRegistry
    );

    return GroupByParamsV1Factory.buildSchema(sourceSchema, compiledGroupBy);
  }

  private LogicalSchema handleStreamGroupBy(
      final LogicalSchema sourceSchema,
      final StreamGroupBy<?> streamGroupBy
  ) {
    final List<CompiledExpression> compiledGroupBy = CodeGenRunner.compileExpressions(
        streamGroupBy.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        ksqlConfig,
        functionRegistry
    );

    return GroupByParamsFactory.buildSchema(sourceSchema, compiledGroupBy);
  }

  private LogicalSchema handleTableGroupByV1(
      final LogicalSchema sourceSchema,
      final TableGroupByV1<?> tableGroupBy
  ) {
    final List<CompiledExpression> compiledGroupBy = CodeGenRunner.compileExpressions(
        tableGroupBy.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        ksqlConfig,
        functionRegistry
    );

    return GroupByParamsV1Factory.buildSchema(sourceSchema, compiledGroupBy);
  }

  private LogicalSchema handleTableGroupBy(
      final LogicalSchema sourceSchema,
      final TableGroupBy<?> tableGroupBy
  ) {
    final List<CompiledExpression> compiledGroupBy = CodeGenRunner.compileExpressions(
        tableGroupBy.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        ksqlConfig,
        functionRegistry
    );

    return GroupByParamsFactory.buildSchema(sourceSchema, compiledGroupBy);
  }

  private LogicalSchema handleStreamSelect(
      final LogicalSchema schema,
      final StreamSelect<?> step
  ) {
    return buildSelectSchema(
        schema,
        step.getKeyColumnNames(),
        step.getSelectedKeys(),
        step.getSelectExpressions()
    );
  }

  private LogicalSchema handleStreamSelectKeyV1(
      final LogicalSchema sourceSchema,
      final StreamSelectKeyV1 step
  ) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(sourceSchema, functionRegistry);

    final SqlType keyType = expressionTypeManager
        .getExpressionSqlType(step.getKeyExpression());

    return LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, keyType)
        .valueColumns(sourceSchema.value())
        .build();
  }

  private LogicalSchema handleStreamSelectKey(
      final LogicalSchema sourceSchema,
      final StreamSelectKey<?> step
  ) {
    return PartitionByParamsFactory.buildSchema(
        sourceSchema,
        step.getKeyExpressions(),
        functionRegistry
    );
  }

  private LogicalSchema handleSource(final LogicalSchema schema, final SourceStep<?> step) {
    return buildSourceSchema(schema, false, step.getPseudoColumnVersion());
  }

  private LogicalSchema handleWindowedSource(final LogicalSchema schema, final SourceStep<?> step) {
    return buildSourceSchema(schema, true, step.getPseudoColumnVersion());
  }

  private LogicalSchema handleStreamStreamJoin(
      final JoinSchemas schemas,
      final StreamStreamJoin<?> step
  ) {
    return handleJoin(schemas, step.getKeyColName());
  }

  private LogicalSchema handleStreamTableJoin(
      final JoinSchemas schemas,
      final StreamTableJoin<?> step
  ) {
    return handleJoin(schemas, step.getKeyColName());
  }

  private LogicalSchema handleTableTableJoin(
      final JoinSchemas schemas,
      final TableTableJoin<?> step
  ) {
    return handleJoin(schemas, step.getKeyColName());
  }

  private LogicalSchema handleForeignKeyTableTableJoin(
      final JoinSchemas schemas,
      final ForeignKeyTableTableJoin<?, ?> step
  ) {
    return ForeignKeyJoinParamsFactory.createSchema(schemas.left, schemas.right);
  }

  private LogicalSchema handleJoin(
      final JoinSchemas schemas,
      final ColumnName keyColName
  ) {
    return JoinParamsFactory.createSchema(keyColName, schemas.left, schemas.right);
  }

  private LogicalSchema handleTableAggregate(
      final LogicalSchema schema,
      final TableAggregate step
  ) {
    return buildAggregateSchema(
        schema,
        step.getNonAggregateColumns(),
        step.getAggregationFunctions(),
        false
    );
  }

  private LogicalSchema handleTableSelect(
      final LogicalSchema schema,
      final TableSelect<?> step
  ) {
    return buildSelectSchema(
        schema,
        step.getKeyColumnNames(),
        Optional.empty(),
        step.getSelectExpressions()
    );
  }

  private LogicalSchema handleTableSelectKey(
      final LogicalSchema sourceSchema,
      final TableSelectKey<?> step
  ) {
    return PartitionByParamsFactory.buildSchema(
        sourceSchema,
        step.getKeyExpressions(),
        functionRegistry
    );
  }

  private LogicalSchema sameSchema(final LogicalSchema schema, final ExecutionStep<?> step) {
    return schema;
  }

  private LogicalSchema buildSourceSchema(
      final LogicalSchema schema,
      final boolean windowed,
      final int pseudoColumnVersion
  ) {
    return schema.withPseudoAndKeyColsInValue(windowed, pseudoColumnVersion);
  }

  private LogicalSchema buildSelectSchema(
      final LogicalSchema schema,
      final List<ColumnName> keyColumnNames,
      final Optional<ImmutableList<ColumnName>> selectedKeys,
      final List<SelectExpression> selectExpressions
  ) {
    return Selection.of(
        schema,
        keyColumnNames,
        selectedKeys,
        selectExpressions,
        ksqlConfig,
        functionRegistry
    ).getSchema();
  }

  private LogicalSchema buildAggregateSchema(
      final LogicalSchema schema,
      final List<ColumnName> nonAggregateColumns,
      final List<FunctionCall> aggregationFunctions,
      final boolean windowedAggregation
  ) {
    return new AggregateParamsFactory().create(
        schema,
        nonAggregateColumns,
        functionRegistry,
        aggregationFunctions,
        windowedAggregation,
        ksqlConfig
    ).getSchema();
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
