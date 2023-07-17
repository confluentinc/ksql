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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;

public final class StreamGroupByBuilder {

  private final KsqlQueryBuilder queryBuilder;
  private final GroupedFactory groupedFactory;
  private final ParamsFactory paramsFactory;

  public StreamGroupByBuilder(
      final KsqlQueryBuilder queryBuilder,
      final GroupedFactory groupedFactory
  ) {
    this(queryBuilder, groupedFactory, GroupByParamsFactory::build);
  }

  @VisibleForTesting
  StreamGroupByBuilder(
      final KsqlQueryBuilder queryBuilder,
      final GroupedFactory groupedFactory,
      final ParamsFactory paramsFactory
  ) {
    this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder");
    this.groupedFactory = requireNonNull(groupedFactory, "groupedFactory");
    this.paramsFactory = requireNonNull(paramsFactory, "paramsFactory");
  }

  public KGroupedStreamHolder build(
      final KStreamHolder<Struct> stream,
      final StreamGroupByKey step
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final QueryContext queryContext = step.getProperties().getQueryContext();
    final Formats formats = step.getInternalFormats();
    final Grouped<Struct, GenericRow> grouped = buildGrouped(
        formats,
        sourceSchema,
        queryContext,
        queryBuilder,
        groupedFactory
    );
    return KGroupedStreamHolder.of(stream.getStream().groupByKey(grouped), stream.getSchema());
  }

  public <K> KGroupedStreamHolder build(
      final KStreamHolder<K> stream,
      final StreamGroupBy<K> step
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final QueryContext queryContext = step.getProperties().getQueryContext();
    final Formats formats = step.getInternalFormats();

    final List<ExpressionMetadata> groupBy = CodeGenRunner.compileExpressions(
        step.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );

    final ProcessingLogger logger = queryBuilder.getProcessingLogger(queryContext);

    final GroupByParams params = paramsFactory
        .build(sourceSchema, groupBy, logger);

    final Grouped<Struct, GenericRow> grouped = buildGrouped(
        formats,
        params.getSchema(),
        queryContext,
        queryBuilder,
        groupedFactory
    );

    final KGroupedStream<Struct, GenericRow> groupedStream = stream.getStream()
        .filter((k, v) -> v != null)
        .groupBy((k, v) -> params.getMapper().apply(v), grouped);

    return KGroupedStreamHolder.of(groupedStream, params.getSchema());
  }

  private static Grouped<Struct, GenericRow> buildGrouped(
      final Formats formats,
      final LogicalSchema schema,
      final QueryContext queryContext,
      final KsqlQueryBuilder queryBuilder,
      final GroupedFactory groupedFactory
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        schema,
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );

    final Serde<Struct> keySerde = queryBuilder.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valSerde = queryBuilder.buildValueSerde(
        formats.getValueFormat(),
        physicalSchema,
        queryContext
    );
    return groupedFactory.create(StreamsUtil.buildOpName(queryContext), keySerde, valSerde);
  }

  interface ParamsFactory {

    GroupByParams build(
        LogicalSchema sourceSchema,
        List<ExpressionMetadata> groupBys,
        ProcessingLogger logger
    );
  }
}
