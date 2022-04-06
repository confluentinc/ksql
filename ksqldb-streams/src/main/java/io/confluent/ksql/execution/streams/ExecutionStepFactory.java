/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SelectExpression;
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
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.streams.kstream.JoinWindows;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ExecutionStepFactory {

  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private ExecutionStepFactory() {
  }

  public static WindowedStreamSource streamSourceWindowed(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final WindowInfo windowInfo,
      final Optional<TimestampColumn> timestampColumn,
      final int pseudoColumnVersion
  ) {
    final QueryContext queryContext = stacker.getQueryContext();

    return new WindowedStreamSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        windowInfo,
        timestampColumn,
        sourceSchema,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  public static StreamSource streamSource(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final int pseudoColumnVersion
  ) {
    final QueryContext queryContext = stacker.getQueryContext();

    return new StreamSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        timestampColumn,
        sourceSchema,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  public static TableSourceV1 tableSourceV1(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final int pseudoColumnVersion
  ) {
    final QueryContext queryContext = stacker.getQueryContext();

    return new TableSourceV1(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        timestampColumn,
        sourceSchema,
        Optional.of(true),
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  public static TableSource tableSource(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final Formats stateStoreFormats,
      final int pseudoColumnVersion
  ) {
    final QueryContext queryContext = stacker.getQueryContext();

    return new TableSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        timestampColumn,
        sourceSchema,
        pseudoColumnVersion,
        stateStoreFormats
    );
  }

  public static WindowedTableSource tableSourceWindowed(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final WindowInfo windowInfo,
      final Optional<TimestampColumn> timestampColumn,
      final int pseudoColumnVersion
  ) {
    final QueryContext queryContext = stacker.getQueryContext();

    return new WindowedTableSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        windowInfo,
        timestampColumn,
        sourceSchema,
        OptionalInt.of(pseudoColumnVersion)
    );
  }

  public static <K> StreamSink<K> streamSink(
      final QueryContext.Stacker stacker,
      final Formats formats,
      final ExecutionStep<KStreamHolder<K>> source,
      final String topicName,
      final Optional<TimestampColumn> timestampColumn
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSink<>(new ExecutionStepPropertiesV1(queryContext),
        source,
        formats,
        topicName,
        timestampColumn
    );
  }

  public static <K> StreamFlatMap<K> streamFlatMap(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<FunctionCall> tableFunctions
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamFlatMap<>(new ExecutionStepPropertiesV1(queryContext),
        source,
        tableFunctions
    );
  }

  public static <K> StreamFilter<K> streamFilter(
      final Stacker stacker,
      final ExecutionStep<KStreamHolder<K>> source,
      final Expression filterExpression
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamFilter<>(
        new ExecutionStepPropertiesV1(queryContext),
        source,
        filterExpression
    );
  }

  public static <K> StreamSelect<K> streamSelect(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<ColumnName> keyColumnNames,
      final Optional<List<ColumnName>> selectedKeys,
      final List<SelectExpression> selectExpressions
  ) {
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(
        stacker.getQueryContext()
    );
    return new StreamSelect<>(
        properties,
        source,
        keyColumnNames,
        selectedKeys,
        selectExpressions
    );
  }

  public static <K> StreamTableJoin<K> streamTableJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final ColumnName keyColName,
      final Formats formats,
      final ExecutionStep<KStreamHolder<K>> left,
      final ExecutionStep<KTableHolder<K>> right
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        keyColName,
        formats,
        left,
        right
    );
  }

  public static <K> StreamStreamJoin<K> streamStreamJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final ColumnName keyColName,
      final Formats leftFormats,
      final Formats rightFormats,
      final ExecutionStep<KStreamHolder<K>> left,
      final ExecutionStep<KStreamHolder<K>> right,
      final JoinWindows joinWindows,
      final Optional<WindowTimeClause> gracePeriod
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        keyColName,
        leftFormats,
        rightFormats,
        left,
        right,
        Duration.ofMillis(joinWindows.beforeMs),
        Duration.ofMillis(joinWindows.afterMs),
        gracePeriod.map(grace -> Duration.ofMillis(grace.toDuration().toMillis()))
    );
  }

  public static <K> StreamSelectKey<K> streamSelectKey(
      final QueryContext.Stacker stacker,
      final ExecutionStep<? extends KStreamHolder<K>> source,
      final List<Expression> partitionBys
  ) {
    final ExecutionStepPropertiesV1 props =
        new ExecutionStepPropertiesV1(stacker.getQueryContext());

    return new StreamSelectKey<>(props, source, partitionBys);
  }

  public static <K> TableSink<K> tableSink(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTableHolder<K>> source,
      final Formats formats,
      final String topicName,
      final Optional<TimestampColumn> timestampColumn
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableSink<>(
        new ExecutionStepPropertiesV1(queryContext),
        source,
        formats,
        topicName,
        timestampColumn
    );
  }

  public static <K> TableFilter<K> tableFilter(
      final Stacker stacker,
      final ExecutionStep<KTableHolder<K>> source,
      final Expression filterExpression
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableFilter<>(
        new ExecutionStepPropertiesV1(queryContext),
        source,
        filterExpression
    );
  }

  public static <K> TableSelect<K> tableMapValues(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTableHolder<K>> source,
      final List<ColumnName> keyColumnNames,
      final List<SelectExpression> selectExpressions,
      final Formats format
  ) {
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(
        stacker.getQueryContext()
    );
    return new TableSelect<>(
        properties,
        source,
        keyColumnNames,
        selectExpressions,
        Optional.ofNullable(format)
    );
  }

  public static <K> TableTableJoin<K> tableTableJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final ColumnName keyColName,
      final ExecutionStep<KTableHolder<K>> left,
      final ExecutionStep<KTableHolder<K>> right
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        keyColName,
        left,
        right
    );
  }

  public static <KLeftT, KRightT> ForeignKeyTableTableJoin<KLeftT, KRightT>
      foreignKeyTableTableJoin(
          final QueryContext.Stacker stacker,
          final JoinType joinType,
          final Optional<ColumnName> leftJoinColumnName,
          final Formats formats,
          final ExecutionStep<KTableHolder<KLeftT>> left,
          final ExecutionStep<KTableHolder<KRightT>> right,
          final Optional<Expression> leftJoinExpression
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new ForeignKeyTableTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        leftJoinColumnName,
        leftJoinExpression,
        formats,
        left,
        right
    );
  }

  public static StreamAggregate streamAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedStreamHolder> sourceStep,
      final Formats formats,
      final List<ColumnName> nonAggregateColumns,
      final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonAggregateColumns,
        aggregations
    );
  }

  public static StreamWindowedAggregate streamWindowedAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedStreamHolder> sourceStep,
      final Formats formats,
      final List<ColumnName> nonAggregateColumns,
      final List<FunctionCall> aggregations,
      final KsqlWindowExpression window
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamWindowedAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonAggregateColumns,
        aggregations,
        window
    );
  }

  public static <K> StreamGroupBy<K> streamGroupBy(
      final Stacker stacker,
      final ExecutionStep<KStreamHolder<K>> sourceStep,
      final Formats format,
      final List<Expression> groupingExpressions
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamGroupBy<>(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        format,
        groupingExpressions
    );
  }

  public static StreamGroupByKey streamGroupByKey(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStreamHolder<GenericKey>> sourceStep,
      final Formats formats
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamGroupByKey(new ExecutionStepPropertiesV1(queryContext), sourceStep, formats);
  }

  public static TableAggregate tableAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedTableHolder> sourceStep,
      final Formats formats,
      final List<ColumnName> nonAggregateColumns,
      final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonAggregateColumns,
        aggregations
    );
  }

  public static <K> TableGroupBy<K> tableGroupBy(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTableHolder<K>> sourceStep,
      final Formats format,
      final List<Expression> groupingExpressions
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableGroupBy<>(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        format,
        groupingExpressions
    );
  }

  public static <K> TableSelectKey<K> tableSelectKey(
      final QueryContext.Stacker stacker,
      final ExecutionStep<? extends KTableHolder<K>> source,
      final Formats formats,
      final List<Expression> partitionBys
  ) {
    final ExecutionStepPropertiesV1 props =
        new ExecutionStepPropertiesV1(stacker.getQueryContext());

    return new TableSelectKey<>(props, source, formats, partitionBys);
  }

  public static <K> TableSuppress<K> tableSuppress(
      final Stacker stacker,
      final ExecutionStep<KTableHolder<K>> sourceStep,
      final RefinementInfo refinementInfo,
      final Formats formats
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableSuppress<>(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        refinementInfo,
        formats
    );
  }
}
