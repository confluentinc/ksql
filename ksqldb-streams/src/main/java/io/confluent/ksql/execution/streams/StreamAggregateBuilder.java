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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.KeySerdeFactory;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.streams.transform.KsTransformer;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.execution.windows.WindowVisitor;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

public final class StreamAggregateBuilder {
  private StreamAggregateBuilder() {
  }

  public static KTableHolder<Struct> build(
      final KGroupedStreamHolder groupedStream,
      final StreamAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory) {
    return build(
        groupedStream,
        aggregate,
        queryBuilder,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  static KTableHolder<Struct> build(
      final KGroupedStreamHolder groupedStream,
      final StreamAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory) {
    final LogicalSchema sourceSchema = groupedStream.getSchema();
    final List<ColumnName> nonFuncColumns = aggregate.getNonAggregateColumns();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregationFunctions(),
        false
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        AggregateBuilderUtils.buildMaterialized(
            aggregate,
            aggregateSchema,
            aggregate.getInternalFormats(),
            queryBuilder,
            materializedFactory
        );

    final KudafAggregator<Struct> aggregator = aggregateParams.getAggregator();

    final KTable<Struct, GenericRow> aggregated = groupedStream.getGroupedStream().aggregate(
        aggregateParams.getInitializer(),
        aggregateParams.getAggregator(),
        materialized
    );

    final MaterializationInfo.Builder materializationBuilder =
        AggregateBuilderUtils.materializationInfoBuilder(
            aggregateParams.getAggregator(),
            aggregate,
            aggregateSchema,
            resultSchema
        );

    final KTable<Struct, GenericRow> result = aggregated
        .transformValues(
            () -> new KsTransformer<>(aggregator.getResultMapper()),
            Named.as(StreamsUtil.buildOpName(
                AggregateBuilderUtils.outputContext(aggregate)))
        );

    return KTableHolder.materialized(
        result,
        resultSchema,
        KeySerdeFactory.unwindowed(queryBuilder),
        materializationBuilder
    );
  }

  public static KTableHolder<Windowed<Struct>> build(
      final KGroupedStreamHolder groupedStream,
      final StreamWindowedAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory
  ) {
    return build(
        groupedStream,
        aggregate,
        queryBuilder,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static KTableHolder<Windowed<Struct>> build(
      final KGroupedStreamHolder groupedStream,
      final StreamWindowedAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory
  ) {
    final LogicalSchema sourceSchema = groupedStream.getSchema();
    final List<ColumnName> nonFuncColumns = aggregate.getNonAggregateColumns();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregationFunctions(),
        true
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final KsqlWindowExpression ksqlWindowExpression = aggregate.getWindowExpression();
    final KTable<Windowed<Struct>, GenericRow> aggregated = ksqlWindowExpression.accept(
        new WindowedAggregator(
            groupedStream.getGroupedStream(),
            aggregate,
            aggregateSchema,
            queryBuilder,
            materializedFactory,
            aggregateParams
        ),
        null
    );

    final KudafAggregator<Windowed<Struct>> aggregator = aggregateParams.getAggregator();

    KTable<Windowed<Struct>, GenericRow> reduced = aggregated.transformValues(
        () -> new KsTransformer<>(aggregator.getResultMapper()),
        Named.as(StreamsUtil.buildOpName(AggregateBuilderUtils.outputContext(aggregate)))
    );

    final MaterializationInfo.Builder materializationBuilder =
        AggregateBuilderUtils.materializationInfoBuilder(
            aggregateParams.getAggregator(),
            aggregate,
            aggregateSchema,
            resultSchema
        );

    reduced = reduced.transformValues(
        () -> new KsTransformer<>(new WindowBoundsPopulator()),
        Named.as(StreamsUtil.buildOpName(
            AggregateBuilderUtils.windowSelectContext(aggregate)
        ))
    );

    materializationBuilder.map(
        pl -> (KsqlTransformer) new WindowBoundsPopulator(),
        resultSchema,
        AggregateBuilderUtils.windowSelectContext(aggregate)
    );

    return KTableHolder.materialized(
        reduced,
        resultSchema,
        KeySerdeFactory.windowed(queryBuilder, ksqlWindowExpression.getWindowInfo()),
        materializationBuilder
    );
  }

  private static class WindowedAggregator
      implements WindowVisitor<KTable<Windowed<Struct>, GenericRow>, Void> {
    final QueryContext queryContext;
    final Formats formats;
    final KGroupedStream<Struct, GenericRow> groupedStream;
    final KsqlQueryBuilder queryBuilder;
    final MaterializedFactory materializedFactory;
    final Serde<Struct> keySerde;
    final Serde<GenericRow> valueSerde;
    final AggregateParams aggregateParams;

    WindowedAggregator(
        final KGroupedStream<Struct, GenericRow> groupedStream,
        final StreamWindowedAggregate aggregate,
        final LogicalSchema aggregateSchema,
        final KsqlQueryBuilder queryBuilder,
        final MaterializedFactory materializedFactory,
        final AggregateParams aggregateParams) {
      Objects.requireNonNull(aggregate, "aggregate");
      this.groupedStream = Objects.requireNonNull(groupedStream, "groupedStream");
      this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
      this.materializedFactory = Objects.requireNonNull(materializedFactory, "materializedFactory");
      this.aggregateParams = Objects.requireNonNull(aggregateParams, "aggregateParams");
      this.queryContext = AggregateBuilderUtils.materializeContext(aggregate);
      this.formats = aggregate.getInternalFormats();
      final PhysicalSchema physicalSchema = PhysicalSchema.from(
          aggregateSchema,
          formats.getKeyFeatures(),
          formats.getValueFeatures()
      );

      keySerde = queryBuilder.buildKeySerde(
          formats.getKeyFormat(),
          physicalSchema,
          queryContext
      );
      valueSerde = queryBuilder.buildValueSerde(
          formats.getValueFormat(),
          physicalSchema,
          queryContext
      );
    }

    @Override
    public KTable<Windowed<Struct>, GenericRow>  visitHoppingWindowExpression(
        final HoppingWindowExpression window,
        final Void ctx) {
      TimeWindows windows = TimeWindows
          .of(window.getSize().toDuration())
          .advanceBy(window.getAdvanceBy().toDuration());
      windows = window.getGracePeriod().map(WindowTimeClause::toDuration)
          .map(windows::grace)
          .orElse(windows);

      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(keySerde,
                  valueSerde,
                  StreamsUtil.buildOpName(queryContext),
                  window.getRetention().map(WindowTimeClause::toDuration))
          );
    }

    @Override
    public KTable<Windowed<Struct>, GenericRow>  visitSessionWindowExpression(
        final SessionWindowExpression window,
        final Void ctx) {
      SessionWindows windows = SessionWindows.with(window.getGap().toDuration());
      windows = window.getGracePeriod().map(WindowTimeClause::toDuration)
          .map(windows::grace)
          .orElse(windows);

      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              aggregateParams.getAggregator().getMerger(),
              materializedFactory.create(keySerde,
                  valueSerde,
                  StreamsUtil.buildOpName(queryContext),
                  window.getRetention().map(WindowTimeClause::toDuration))
          );
    }

    @Override
    public KTable<Windowed<Struct>, GenericRow> visitTumblingWindowExpression(
        final TumblingWindowExpression window,
        final Void ctx) {
      TimeWindows windows = TimeWindows.of(window.getSize().toDuration());
      windows = window.getGracePeriod().map(WindowTimeClause::toDuration)
          .map(windows::grace)
          .orElse(windows);

      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(keySerde,
                  valueSerde,
                  StreamsUtil.buildOpName(queryContext),
                  window.getRetention().map(WindowTimeClause::toDuration))
          );
    }
  }

  private static final class WindowBoundsPopulator
      implements KsqlTransformer<Windowed<Struct>, GenericRow> {

    @Override
    public GenericRow transform(
        final Windowed<Struct> readOnlyKey,
        final GenericRow value,
        final KsqlProcessingContext ctx
    ) {
      if (value == null) {
        return null;
      }
      final Window window = readOnlyKey.window();
      value.ensureAdditionalCapacity(2);
      value.append(window.start());
      value.append(window.end());
      return value;
    }
  }
}
