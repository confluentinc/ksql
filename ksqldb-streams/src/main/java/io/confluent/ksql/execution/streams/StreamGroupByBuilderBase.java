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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;

class StreamGroupByBuilderBase {

  private final RuntimeBuildContext buildContext;
  private final GroupedFactory groupedFactory;
  private final ParamsFactory paramsFactory;

  StreamGroupByBuilderBase(
      final RuntimeBuildContext buildContext,
      final GroupedFactory groupedFactory,
      final ParamsFactory paramsFactory
  ) {
    this.buildContext = requireNonNull(buildContext, "buildContext");
    this.groupedFactory = requireNonNull(groupedFactory, "groupedFactory");
    this.paramsFactory = requireNonNull(paramsFactory, "paramsFactory");
  }

  public KGroupedStreamHolder build(
      final KStreamHolder<GenericKey> stream,
      final StreamGroupByKey step
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final QueryContext queryContext = step.getProperties().getQueryContext();
    final Formats formats = step.getInternalFormats();
    final Grouped<GenericKey, GenericRow> grouped = buildGrouped(
        formats,
        sourceSchema,
        queryContext,
        buildContext,
        groupedFactory
    );
    return KGroupedStreamHolder.of(stream.getStream().groupByKey(grouped), stream.getSchema());
  }

  public <K> KGroupedStreamHolder build(
      final KStreamHolder<K> stream,
      final QueryContext queryContext,
      final Formats formats,
      final List<Expression> groupByExpressions
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();

    final List<CompiledExpression> groupBy = CodeGenRunner.compileExpressions(
        groupByExpressions.stream(),
        "Group By",
        sourceSchema,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final ProcessingLogger logger = buildContext.getProcessingLogger(queryContext);

    final GroupByParams params = paramsFactory
        .build(sourceSchema, groupBy, logger);

    final Grouped<GenericKey, GenericRow> grouped = buildGrouped(
        formats,
        params.getSchema(),
        queryContext,
        buildContext,
        groupedFactory
    );

    final KGroupedStream<GenericKey, GenericRow> groupedStream = stream.getStream()
        .filter((k, v) -> v != null)
        .groupBy((k, v) -> params.getMapper().apply(v), grouped);

    return KGroupedStreamHolder.of(groupedStream, params.getSchema());
  }

  private static Grouped<GenericKey, GenericRow> buildGrouped(
      final Formats formats,
      final LogicalSchema schema,
      final QueryContext queryContext,
      final RuntimeBuildContext buildContext,
      final GroupedFactory groupedFactory
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        schema,
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valSerde = buildContext.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        queryContext
    );
    return groupedFactory.create(StreamsUtil.buildOpName(queryContext), keySerde, valSerde);
  }

  interface ParamsFactory {

    GroupByParams build(
        LogicalSchema sourceSchema,
        List<CompiledExpression> groupBys,
        ProcessingLogger logger
    );
  }
}
