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

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepPropertiesV1;
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
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.StreamStreamJoin;
import io.confluent.ksql.execution.plan.StreamTableJoin;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.plan.TableAggregate;
import io.confluent.ksql.execution.plan.TableFilter;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.plan.TableMapValues;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
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
      final SourceName alias
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new WindowedStreamSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        windowInfo,
        timestampColumn,
        sourceSchema,
        alias
    );
  }

  public static StreamSource streamSource(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final SourceName alias
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        timestampColumn,
        sourceSchema,
        alias
    );
  }

  public static TableSource tableSource(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final SourceName alias
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        timestampColumn,
        sourceSchema,
        alias
    );
  }

  public static WindowedTableSource tableSourceWindowed(
      final QueryContext.Stacker stacker,
      final LogicalSchema sourceSchema,
      final String topicName,
      final Formats formats,
      final WindowInfo windowInfo,
      final Optional<TimestampColumn> timestampColumn,
      final SourceName alias
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new WindowedTableSource(
        new ExecutionStepPropertiesV1(queryContext),
        topicName,
        formats,
        windowInfo,
        timestampColumn,
        sourceSchema,
        alias
    );
  }

  public static <K> StreamSink<K> streamSink(
      final QueryContext.Stacker stacker,
      final Formats formats,
      final ExecutionStep<KStreamHolder<K>> source,
      final String topicName
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSink<>(new ExecutionStepPropertiesV1(queryContext),
        source,
        formats,
        topicName
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

  public static <K> StreamMapValues<K> streamMapValues(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KStreamHolder<K>> source,
      final List<SelectExpression> selectExpressions
  ) {
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(
        stacker.getQueryContext()
    );
    return new StreamMapValues<>(
        properties,
        source,
        selectExpressions
    );
  }

  public static <K> StreamTableJoin<K> streamTableJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final Formats formats,
      final ExecutionStep<KStreamHolder<K>> left,
      final ExecutionStep<KTableHolder<K>> right
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        formats,
        left,
        right
    );
  }

  public static <K> StreamStreamJoin<K> streamStreamJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final Formats leftFormats,
      final Formats rightFormats,
      final ExecutionStep<KStreamHolder<K>> left,
      final ExecutionStep<KStreamHolder<K>> right,
      final JoinWindows joinWindows
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamStreamJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        leftFormats,
        rightFormats,
        left,
        right,
        Duration.ofMillis(joinWindows.beforeMs),
        Duration.ofMillis(joinWindows.afterMs)
    );
  }

  public static StreamSelectKey streamSelectKey(
      final QueryContext.Stacker stacker,
      final ExecutionStep<? extends KStreamHolder<?>> source,
      final Expression fieldName
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamSelectKey(new ExecutionStepPropertiesV1(queryContext), source, fieldName);
  }

  public static <K> TableSink<K> tableSink(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTableHolder<K>> source,
      final Formats formats,
      final String topicName
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableSink<>(
        new ExecutionStepPropertiesV1(queryContext),
        source,
        formats,
        topicName
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

  public static <K> TableMapValues<K> tableMapValues(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KTableHolder<K>> source,
      final List<SelectExpression> selectExpressions
  ) {
    final ExecutionStepPropertiesV1 properties = new ExecutionStepPropertiesV1(
        stacker.getQueryContext()
    );
    return new TableMapValues<>(
        properties,
        source,
        selectExpressions
    );
  }

  public static <K> TableTableJoin<K> tableTableJoin(
      final QueryContext.Stacker stacker,
      final JoinType joinType,
      final ExecutionStep<KTableHolder<K>> left,
      final ExecutionStep<KTableHolder<K>> right
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableTableJoin<>(
        new ExecutionStepPropertiesV1(queryContext),
        joinType,
        left,
        right
    );
  }

  public static StreamAggregate streamAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedStreamHolder> sourceStep,
      final Formats formats,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonFuncColumnCount,
        aggregations
    );
  }

  public static StreamWindowedAggregate streamWindowedAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedStreamHolder> sourceStep,
      final Formats formats,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final KsqlWindowExpression window
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamWindowedAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonFuncColumnCount,
        aggregations,
        window
    );
  }

  public static <K> StreamGroupBy<K> streamGroupBy(
      final QueryContext.Stacker stacker,
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
      final ExecutionStep<KStreamHolder<Struct>> sourceStep,
      final Formats formats
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new StreamGroupByKey(new ExecutionStepPropertiesV1(queryContext), sourceStep, formats);
  }

  public static TableAggregate tableAggregate(
      final QueryContext.Stacker stacker,
      final ExecutionStep<KGroupedTableHolder> sourceStep,
      final Formats formats,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations
  ) {
    final QueryContext queryContext = stacker.getQueryContext();
    return new TableAggregate(
        new ExecutionStepPropertiesV1(queryContext),
        sourceStep,
        formats,
        nonFuncColumnCount,
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
}
