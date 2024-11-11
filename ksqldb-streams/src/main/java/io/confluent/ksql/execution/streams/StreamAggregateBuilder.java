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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamWindowedAggregate;
import io.confluent.ksql.execution.runtime.MaterializedFactory;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.execution.windows.WindowVisitor;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

// Can be fixed after GRACE is mandatory
@SuppressWarnings("deprecation")
public final class StreamAggregateBuilder {

  private static final long DEFAULT_24_HR_GRACE_PERIOD = 24 * 60 * 60 * 1000L;

  private StreamAggregateBuilder() {
    super();
  }

  public static KTableHolder<GenericKey> build(
      final KGroupedStreamHolder groupedStream,
      final StreamAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory) {
    return build(
        groupedStream,
        aggregate,
        buildContext,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  static KTableHolder<GenericKey> build(
      final KGroupedStreamHolder groupedStream,
      final StreamAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory) {
    final LogicalSchema sourceSchema = groupedStream.getSchema();
    final List<ColumnName> nonFuncColumns = aggregate.getNonAggregateColumns();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        buildContext.getFunctionRegistry(),
        aggregate.getAggregationFunctions(),
        false,
        buildContext.getKsqlConfig()
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> materialized =
        MaterializationUtil.buildMaterialized(
            aggregate,
            aggregateSchema,
            aggregate.getInternalFormats(),
            buildContext,
            materializedFactory,
            ExecutionKeyFactory.unwindowed(buildContext)
        );

    final KudafAggregator<GenericKey> aggregator = aggregateParams.getAggregator();

    final KTable<GenericKey, GenericRow> aggregated = groupedStream.getGroupedStream().aggregate(
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

    final KTable<GenericKey, GenericRow> result = aggregated
        .transformValues(
            () -> new KsValueTransformer<>(aggregator.getResultMapper()),
            Named.as(StreamsUtil.buildOpName(
                AggregateBuilderUtils.outputContext(aggregate)))
        );

    return KTableHolder.materialized(
        result,
        resultSchema,
        ExecutionKeyFactory.unwindowed(buildContext),
        materializationBuilder
    );
  }

  public static KTableHolder<Windowed<GenericKey>> build(
      final KGroupedStreamHolder groupedStream,
      final StreamWindowedAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory
  ) {
    return build(
        groupedStream,
        aggregate,
        buildContext,
        materializedFactory,
        new AggregateParamsFactory()
    );
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static KTableHolder<Windowed<GenericKey>> build(
      final KGroupedStreamHolder groupedStream,
      final StreamWindowedAggregate aggregate,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final AggregateParamsFactory aggregateParamsFactory
  ) {
    final LogicalSchema sourceSchema = groupedStream.getSchema();
    final List<ColumnName> nonFuncColumns = aggregate.getNonAggregateColumns();
    final AggregateParams aggregateParams = aggregateParamsFactory.create(
        sourceSchema,
        nonFuncColumns,
        buildContext.getFunctionRegistry(),
        aggregate.getAggregationFunctions(),
        true,
        buildContext.getKsqlConfig()
    );
    final LogicalSchema aggregateSchema = aggregateParams.getAggregateSchema();
    final LogicalSchema resultSchema = aggregateParams.getSchema();
    final KsqlWindowExpression ksqlWindowExpression = aggregate.getWindowExpression();
    final KTable<Windowed<GenericKey>, GenericRow> aggregated = ksqlWindowExpression.accept(
        new WindowedAggregator(
            groupedStream.getGroupedStream(),
            aggregate,
            aggregateSchema,
            buildContext,
            materializedFactory,
            aggregateParams
        ),
        null
    );

    final KudafAggregator<Windowed<GenericKey>> aggregator = aggregateParams.getAggregator();

    KTable<Windowed<GenericKey>, GenericRow> reduced = aggregated.transformValues(
        () -> new KsValueTransformer<>(aggregator.getResultMapper()),
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
        () -> new KsValueTransformer<>(new WindowBoundsPopulator()),
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
        ExecutionKeyFactory.windowed(buildContext, ksqlWindowExpression.getWindowInfo()),
        materializationBuilder
    );
  }

  private static class WindowedAggregator
      implements WindowVisitor<KTable<Windowed<GenericKey>, GenericRow>, Void> {
    final QueryContext queryContext;
    final Formats formats;
    final KGroupedStream<GenericKey, GenericRow> groupedStream;
    final RuntimeBuildContext buildContext;
    final MaterializedFactory materializedFactory;
    final Serde<GenericKey> keySerde;
    final Serde<GenericRow> valueSerde;
    final AggregateParams aggregateParams;

    WindowedAggregator(
        final KGroupedStream<GenericKey, GenericRow> groupedStream,
        final StreamWindowedAggregate aggregate,
        final LogicalSchema aggregateSchema,
        final RuntimeBuildContext buildContext,
        final MaterializedFactory materializedFactory,
        final AggregateParams aggregateParams) {
      Objects.requireNonNull(aggregate, "aggregate");
      this.groupedStream = Objects.requireNonNull(groupedStream, "groupedStream");
      this.buildContext = Objects.requireNonNull(buildContext, "buildContext");
      this.materializedFactory = Objects.requireNonNull(materializedFactory, "materializedFactory");
      this.aggregateParams = Objects.requireNonNull(aggregateParams, "aggregateParams");
      this.queryContext = MaterializationUtil.materializeContext(aggregate);
      this.formats = aggregate.getInternalFormats();
      final PhysicalSchema physicalSchema = PhysicalSchema.from(
          aggregateSchema,
          formats.getKeyFeatures(),
          formats.getValueFeatures()
      );

      keySerde = buildContext.buildKeySerde(
          formats.getKeyFormat(),
          physicalSchema,
          queryContext
      );
      valueSerde = buildContext.buildValueSerde(
          formats.getValueFormat(),
          physicalSchema,
          queryContext
      );
    }

    @Override
    @SuppressWarnings("deprecation")  // can be fixed after GRACE clause is made mandatory
    public KTable<Windowed<GenericKey>, GenericRow> visitHoppingWindowExpression(
        final HoppingWindowExpression window,
        final Void ctx) {
      final Duration windowSize = window.getSize().toDuration();
      final Duration advanceBy = window.getAdvanceBy().toDuration();
      final Duration grace = window.getGracePeriod()
              .map(WindowTimeClause::toDuration)
              .orElse(defaultGrace(windowSize));

      final TimeWindows windows =
              TimeWindows.ofSizeAndGrace(windowSize, grace).advanceBy(advanceBy);

      TimeWindowedKStream<GenericKey, GenericRow> timeWindowedKStream =
          groupedStream.windowedBy(windows);

      if (window.getEmitStrategy().isPresent()
          && window.getEmitStrategy().get() == OutputRefinement.FINAL) {
        timeWindowedKStream = timeWindowedKStream.emitStrategy(EmitStrategy.onWindowClose());
      }

      return timeWindowedKStream.aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(keySerde,
                  valueSerde,
                  StreamsUtil.buildOpName(queryContext),
                  window.getRetention().map(WindowTimeClause::toDuration))
          );
    }

    @Override
    @SuppressWarnings("deprecation")  // can be fixed after GRACE clause is made mandatory
    public KTable<Windowed<GenericKey>, GenericRow> visitSessionWindowExpression(
        final SessionWindowExpression window,
        final Void ctx) {
      final Duration windowSize = window.getGap().toDuration();
      final Duration grace = window.getGracePeriod()
              .map(WindowTimeClause::toDuration)
              .orElse(defaultGrace(windowSize));
      final SessionWindows windows =
              SessionWindows.ofInactivityGapAndGrace(windowSize, grace);
      SessionWindowedKStream<GenericKey, GenericRow> sessionWindowedKStream =
          groupedStream.windowedBy(windows);

      if (window.getEmitStrategy().isPresent()
          && window.getEmitStrategy().get() == OutputRefinement.FINAL) {
        sessionWindowedKStream = sessionWindowedKStream.emitStrategy(EmitStrategy.onWindowClose());
      }

      return sessionWindowedKStream.aggregate(
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
    @SuppressWarnings("deprecation")  // can be fixed after GRACE clause is made mandatory
    public KTable<Windowed<GenericKey>, GenericRow> visitTumblingWindowExpression(
        final TumblingWindowExpression window,
        final Void ctx) {
      final Duration windowSize = window.getSize().toDuration();
      final Duration grace = window.getGracePeriod()
              .map(WindowTimeClause::toDuration)
              .orElse(defaultGrace(windowSize));
      TimeWindows windows = TimeWindows.ofSizeAndGrace(windowSize, grace);

      TimeWindowedKStream<GenericKey, GenericRow> timeWindowedKStream =
          groupedStream.windowedBy(windows);

      if (window.getEmitStrategy().isPresent()
          && window.getEmitStrategy().get() == OutputRefinement.FINAL) {
        timeWindowedKStream = timeWindowedKStream.emitStrategy(EmitStrategy.onWindowClose());
      }

      return timeWindowedKStream.aggregate(
              aggregateParams.getInitializer(),
              aggregateParams.getAggregator(),
              materializedFactory.create(keySerde,
                  valueSerde,
                  StreamsUtil.buildOpName(queryContext),
                  window.getRetention().map(WindowTimeClause::toDuration))
          );
    }

    private Duration defaultGrace(Duration windowSize) {
      return Duration.ofMillis(Math.max(DEFAULT_24_HR_GRACE_PERIOD - windowSize.toMillis(), 0));
    }
  }

  private static final class WindowBoundsPopulator
      implements KsqlTransformer<Windowed<GenericKey>, GenericRow> {

    @Override
    public GenericRow transform(
        final Windowed<GenericKey> readOnlyKey,
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
