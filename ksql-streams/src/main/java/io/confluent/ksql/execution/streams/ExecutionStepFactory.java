/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.StreamToTable;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableMapValues;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ExecutionStepFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private ExecutionStepFactory() {
  }

  public static StreamSource<KStream<Windowed<Struct>, GenericRow>> streamSourceWindowed(
      final QueryContext.Stacker stacker,
      final LogicalSchemaWithMetaAndKeyFields schema,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSource<>(
        new DefaultExecutionStepProperties(
            schema.getSchema(),
            queryContext),
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schema.getOriginalSchema(),
        StreamSourceBuilder::buildWindowed
    );
  }

  public static StreamSource<KStream<Struct, GenericRow>> streamSource(
      final QueryContext.Stacker stacker,
      final LogicalSchemaWithMetaAndKeyFields schema,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSource<>(
        new DefaultExecutionStepProperties(
            schema.getSchema(),
            queryContext),
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schema.getOriginalSchema(),
        StreamSourceBuilder::buildUnwindowed
    );
  }

  public static <K> StreamToTable<KStream<K, GenericRow>, KTable<K, GenericRow>> streamToTable(
      final QueryContext.Stacker stacker,
      final Formats formats,
      final ExecutionStep<KStream<K, GenericRow>> source
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamToTable<>(
        source,
        formats,
        source.getProperties().withQueryContext(queryContext)
    );
  }

  public static <K> StreamSink<KStream<K, GenericRow>> streamSink(
      final QueryContext.Stacker stacker,
      final LogicalSchema outputSchema,
      final Formats formats,
      final ExecutionStep<KStream<K, GenericRow>> source,
      final String topicName
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSink<>(
        new DefaultExecutionStepProperties(outputSchema, queryContext),
        source,
        formats,
        topicName
    );
  }

  public static <K> StreamFilter<KStream<K, GenericRow>> streamFilter(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStream<K, GenericRow>> source,
      final Expression filterExpression
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamFilter<>(
        source.getProperties().withQueryContext(queryContext),
        source,
        filterExpression
    );
  }

  public static <K> StreamMapValues<KStream<K, GenericRow>> streamMapValues(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStream<K, GenericRow>> source,
      final List<SelectExpression> selectExpressions,
      final KsqlQueryBuilder queryBuilder
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    final Selection selection = Selection.of(
        queryContext,
        source.getProperties().getSchema(),
        selectExpressions,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        queryBuilder.getProcessingLogContext()
    );
    return new StreamMapValues<>(
        new DefaultExecutionStepProperties(selection.getSchema(), queryContext),
        source,
        selectExpressions
    );
  }

  public static <K> StreamTableJoin<KStream<K, GenericRow>, KTable<K, GenericRow>>
      streamTableJoin(
          final QueryContext.Stacker stacker,
          final JoinType joinType,
          final Formats formats,
          final ExecutionStep<KStream<K, GenericRow>> left,
          final ExecutionStep<KTable<K, GenericRow>> right,
          final LogicalSchema resultSchema
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamTableJoin<>(
        new DefaultExecutionStepProperties(resultSchema, queryContext),
        joinType,
        formats,
        left,
        right
    );
  }

  public static <K> StreamStreamJoin<KStream<K, GenericRow>> streamStreamJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final Formats leftFormats,
      final Formats rightFormats,
      final ExecutionStep<KStream<K, GenericRow>> left,
      final ExecutionStep<KStream<K, GenericRow>> right,
      final LogicalSchema resultSchema,
      final JoinWindows joinWindows
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamStreamJoin<>(
        new DefaultExecutionStepProperties(resultSchema, queryContext),
        joinType,
        leftFormats,
        rightFormats,
        left,
        right,
        Duration.ofMillis(joinWindows.beforeMs),
        Duration.ofMillis(joinWindows.afterMs)
    );
  }

  @SuppressWarnings("unchecked")
  public static <K> StreamSelectKey<K> streamSelectKey(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStream<K, GenericRow>> source,
      final String fieldName,
      final boolean updateRowKey
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSelectKey<>(
        new DefaultExecutionStepProperties(
            source.getProperties().getSchema(),
            queryContext
        ),
        source,
        fieldName,
        updateRowKey
    );
  }

  public static <K> TableSink<KTable<K, GenericRow>> tableSink(
      final QueryContext.Stacker stacker,
      final LogicalSchema outputSchema,
      final ExecutionStep<KTable<K, GenericRow>> source,
      final Formats formats,
      final String topicName
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableSink<>(
        new DefaultExecutionStepProperties(outputSchema, queryContext),
        source,
        formats,
        topicName
    );
  }

  public static <K> TableFilter<KTable<K, GenericRow>> tableFilter(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTable<K, GenericRow>> source,
      final Expression filterExpression
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableFilter<>(
        source.getProperties().withQueryContext(queryContext),
        source,
        filterExpression
    );
  }

  public static <K> TableMapValues<KTable<K, GenericRow>> tableMapValues(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTable<K, GenericRow>> source,
      final List<SelectExpression> selectExpressions,
      final KsqlQueryBuilder queryBuilder
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    final Selection selection = Selection.of(
        queryContext,
        source.getProperties().getSchema(),
        selectExpressions,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry(),
        queryBuilder.getProcessingLogContext()
    );
    return new TableMapValues<>(
        new DefaultExecutionStepProperties(selection.getSchema(), queryContext),
        source,
        selectExpressions
    );
  }

  public static <K> TableTableJoin<KTable<K, GenericRow>> tableTableJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final ExecutionStep<KTable<K, GenericRow>> left,
      final ExecutionStep<KTable<K, GenericRow>> right,
      final LogicalSchema resultSchema
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableTableJoin<>(
        new DefaultExecutionStepProperties(resultSchema, queryContext),
        joinType,
        left,
        right
    );
  }

  public static <K> StreamAggregate<KTable<K, GenericRow>, KGroupedStream<Struct, GenericRow>>
      streamAggregate(
          final QueryContext.Stacker stacker,
          final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep,
          final LogicalSchema resultSchema,
          final Formats formats,
          final int nonFuncColumnCount,
          final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamAggregate<>(
        new DefaultExecutionStepProperties(resultSchema, queryContext),
        sourceStep,
        formats,
        nonFuncColumnCount,
        aggregations
    );
  }

  public static <K> StreamGroupBy<KStream<K, GenericRow>, KGroupedStream<Struct, GenericRow>>
      streamGroupBy(
          final QueryContext.Stacker stacker,
          final ExecutionStep<KStream<K, GenericRow>> sourceStep,
          final Formats format,
          final List<Expression> groupingExpressions
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamGroupBy<>(
        sourceStep.getProperties().withQueryContext(queryContext),
        sourceStep,
        format,
        groupingExpressions
    );
  }

  public static TableAggregate<KTable<Struct, GenericRow>, KGroupedTable<Struct, GenericRow>>
      tableAggregate(
          final QueryContext.Stacker stacker,
          final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceStep,
          final LogicalSchema resultSchema,
          final Formats formats,
          final int nonFuncColumnCount,
          final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableAggregate<>(
        new DefaultExecutionStepProperties(resultSchema, queryContext),
        sourceStep,
        formats,
        nonFuncColumnCount,
        aggregations
    );
  }

  public static <K> TableGroupBy<KTable<K, GenericRow>, KGroupedTable<Struct, GenericRow>>
      tableGroupBy(
          final QueryContext.Stacker stacker,
          final ExecutionStep<KTable<K, GenericRow>> sourceStep,
          final Formats format,
          final List<Expression> groupingExpressions
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableGroupBy<>(
        sourceStep.getProperties().withQueryContext(queryContext),
        sourceStep,
        format,
        groupingExpressions
    );
  }
}
