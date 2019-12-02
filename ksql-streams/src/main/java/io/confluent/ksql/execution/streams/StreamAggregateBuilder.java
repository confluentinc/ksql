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
import io.confluent.ksql.execution.transform.window.WindowSelectMapper;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowVisitor;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.time.Duration;
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
    final int nonFuncColumns = aggregate.getNonFuncColumnCount();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregations()
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final Materialized<Struct, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        AggregateBuilderUtils.buildMaterialized(
            aggregate.getProperties().getQueryContext(),
            aggregateSchema,
            aggregate.getFormats(),
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
            aggregate.getProperties().getQueryContext(),
            aggregateSchema,
            resultSchema
        );

    final KTable<Struct, GenericRow> result = aggregated
        .transformValues(
            () -> new KsTransformer<>(aggregator.getResultMapper()),
            Named.as(queryBuilder.buildUniqueNodeName(AggregateBuilderUtils.STEP_NAME))
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

  static KTableHolder<Windowed<Struct>> build(
      final KGroupedStreamHolder groupedStream,
      final StreamWindowedAggregate aggregate,
      final KsqlQueryBuilder queryBuilder,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory
  ) {
    final LogicalSchema sourceSchema = groupedStream.getSchema();
    final int nonFuncColumns = aggregate.getNonFuncColumnCount();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        queryBuilder.getFunctionRegistry(),
        aggregate.getAggregations()
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
        Named.as(queryBuilder.buildUniqueNodeName(AggregateBuilderUtils.STEP_NAME))
    );

    final MaterializationInfo.Builder materializationBuilder =
        AggregateBuilderUtils.materializationInfoBuilder(
            aggregateParams.getAggregator(),
            aggregate.getProperties().getQueryContext(),
            aggregateSchema,
            resultSchema
        );

    final WindowSelectMapper windowSelectMapper = aggregateParams.getWindowSelectMapper();
    if (windowSelectMapper.hasSelects()) {
      reduced = reduced.transformValues(
          () -> new KsTransformer<>(windowSelectMapper.getTransformer()),
          Named.as(queryBuilder.buildUniqueNodeName("AGGREGATE-WINDOW-SELECT"))
      );
    }

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
      this.queryContext = aggregate.getProperties().getQueryContext();
      this.formats = aggregate.getFormats();
      final PhysicalSchema physicalSchema = PhysicalSchema.from(
          aggregateSchema,
          formats.getOptions()
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
      final TimeWindows windows = TimeWindows
          .of(Duration.ofMillis(window.getSizeUnit().toMillis(window.getSize())))
          .advanceBy(
              Duration.ofMillis(window.getAdvanceByUnit().toMillis(window.getAdvanceBy()))
          );

      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(
                  keySerde, valueSerde, StreamsUtil.buildOpName(queryContext))
          );
    }

    @Override
    public KTable<Windowed<Struct>, GenericRow>  visitSessionWindowExpression(
        final SessionWindowExpression window,
        final Void ctx) {
      final SessionWindows windows = SessionWindows.with(
          Duration.ofMillis(window.getSizeUnit().toMillis(window.getGap()))
      );
      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              aggregateParams.getAggregator().getMerger(),
              materializedFactory.create(
                  keySerde, valueSerde, StreamsUtil.buildOpName(queryContext))
          );
    }

    @Override
    public KTable<Windowed<Struct>, GenericRow> visitTumblingWindowExpression(
        final TumblingWindowExpression window,
        final Void ctx) {
      final TimeWindows windows = TimeWindows.of(
          Duration.ofMillis(window.getSizeUnit().toMillis(window.getSize())));
      return groupedStream
          .windowedBy(windows)
          .aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(
                  keySerde, valueSerde, StreamsUtil.buildOpName(queryContext))
          );
    }
  }
}
