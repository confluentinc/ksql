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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.StreamAggregate;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.plan.StreamGroupBy;
import io.confluent.ksql.execution.plan.StreamGroupByKey;
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
import io.confluent.ksql.execution.plan.TableSelect;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * An implementation of PlanBuilder that builds an execution plan into a
 * Kafka Streams app
 */
public final class KSPlanBuilder implements PlanBuilder {
  private final KsqlQueryBuilder queryBuilder;
  private final SqlPredicateFactory sqlPredicateFactory;
  private final AggregateParamsFactory aggregateParamFactory;
  private final StreamsFactories streamsFactories;

  public KSPlanBuilder(final KsqlQueryBuilder queryBuilder) {
    this(
        queryBuilder,
        SqlPredicate::new,
        new AggregateParamsFactory(),
        StreamsFactories.create(queryBuilder.getKsqlConfig())
    );
  }

  public KSPlanBuilder(
      final KsqlQueryBuilder queryBuilder,
      final SqlPredicateFactory sqlPredicateFactory,
      final AggregateParamsFactory aggregateParamFactory,
      final StreamsFactories streamsFactories) {
    this.queryBuilder = Objects.requireNonNull(queryBuilder, "queryBuilder");
    this.sqlPredicateFactory = Objects.requireNonNull(sqlPredicateFactory, "sqlPredicateFactory");
    this.aggregateParamFactory =
        Objects.requireNonNull(aggregateParamFactory, "aggregateParamsFactory");
    this.streamsFactories = Objects.requireNonNull(streamsFactories, "streamsFactories");
  }

  public <K> KStreamHolder<K> visitStreamFilter(final StreamFilter<K> streamFilter) {
    final KStreamHolder<K> source = streamFilter.getSource().build(this);
    return StreamFilterBuilder.build(source, streamFilter, queryBuilder, sqlPredicateFactory);
  }

  @Override
  public <K> KGroupedStreamHolder visitStreamGroupBy(
      final StreamGroupBy<K> streamGroupBy) {
    final KStreamHolder<K> source = streamGroupBy.getSource().build(this);
    return new StreamGroupByBuilder(
        queryBuilder,
        streamsFactories.getGroupedFactory()
    ).build(
        source,
        streamGroupBy
    );
  }

  @Override
  public KGroupedStreamHolder visitStreamGroupByKey(
      final StreamGroupByKey streamGroupByKey) {
    final KStreamHolder<Struct> source = streamGroupByKey.getSource().build(this);
    return new StreamGroupByBuilder(
        queryBuilder,
        streamsFactories.getGroupedFactory()
    ).build(
        source,
        streamGroupByKey
    );
  }

  @Override
  public KTableHolder<Struct> visitStreamAggregate(
      final StreamAggregate streamAggregate) {
    final KGroupedStreamHolder source = streamAggregate.getSource().build(this);
    return StreamAggregateBuilder.build(
        source,
        streamAggregate,
        queryBuilder,
        streamsFactories.getMaterializedFactory(),
        aggregateParamFactory
    );
  }

  @Override
  public <K> KStreamHolder<K> visitStreamSelect(
      final StreamSelect<K> streamSelect) {
    final KStreamHolder<K> source = streamSelect.getSource().build(this);
    return StreamSelectBuilder.build(source, streamSelect, queryBuilder);
  }

  @Override
  public <K> KStreamHolder<K> visitFlatMap(final StreamFlatMap<K> streamFlatMap) {
    final KStreamHolder<K> source = streamFlatMap.getSource().build(this);
    return StreamFlatMapBuilder.build(source, streamFlatMap, queryBuilder);
  }

  @Override
  public KStreamHolder<Struct> visitStreamSelectKey(
      final StreamSelectKeyV1 streamSelectKey
  ) {
    final KStreamHolder<?> source = streamSelectKey.getSource().build(this);
    return StreamSelectKeyBuilderV1.build(source, streamSelectKey, queryBuilder);
  }

  @Override
  public KStreamHolder<Struct> visitStreamSelectKey(
      final StreamSelectKey streamSelectKey
  ) {
    final KStreamHolder<?> source = streamSelectKey.getSource().build(this);
    return StreamSelectKeyBuilder.build(source, streamSelectKey, queryBuilder);
  }

  @Override
  public <K> KStreamHolder<K> visitStreamSink(final StreamSink<K> streamSink) {
    final KStreamHolder<K> source = streamSink.getSource().build(this);
    StreamSinkBuilder.build(source, streamSink, queryBuilder);
    return null;
  }

  @Override
  public KStreamHolder<Struct> visitStreamSource(final StreamSource streamSource) {
    return SourceBuilder.buildStream(
        queryBuilder,
        streamSource,
        streamsFactories.getConsumedFactory()
    );
  }

  @Override
  public KStreamHolder<Windowed<Struct>> visitWindowedStreamSource(
      final WindowedStreamSource windowedStreamSource) {
    return SourceBuilder.buildWindowedStream(
        queryBuilder,
        windowedStreamSource,
        streamsFactories.getConsumedFactory()
    );
  }

  @Override
  public <K> KStreamHolder<K> visitStreamStreamJoin(final StreamStreamJoin<K> join) {
    final KStreamHolder<K> left = join.getLeftSource().build(this);
    final KStreamHolder<K> right = join.getRightSource().build(this);
    return StreamStreamJoinBuilder.build(
        left,
        right,
        join,
        queryBuilder,
        streamsFactories.getStreamJoinedFactory()
    );
  }

  @Override
  public <K> KStreamHolder<K> visitStreamTableJoin(final StreamTableJoin<K> join) {
    final KTableHolder<K> right = join.getRightSource().build(this);
    final KStreamHolder<K> left = join.getLeftSource().build(this);
    return StreamTableJoinBuilder.build(
        left,
        right,
        join,
        queryBuilder,
        streamsFactories.getJoinedFactory()
    );
  }

  @Override
  public KTableHolder<Struct> visitTableSource(final TableSource tableSource) {
    return SourceBuilder.buildTable(
        queryBuilder,
        tableSource,
        streamsFactories.getConsumedFactory(),
        streamsFactories.getMaterializedFactory()
    );
  }

  @Override
  public KTableHolder<Windowed<Struct>> visitWindowedTableSource(
      final WindowedTableSource windowedTableSource
  ) {
    return SourceBuilder.buildWindowedTable(
        queryBuilder,
        windowedTableSource,
        streamsFactories.getConsumedFactory(),
        streamsFactories.getMaterializedFactory()
    );
  }

  @Override
  public KTableHolder<Windowed<Struct>> visitStreamWindowedAggregate(
      final StreamWindowedAggregate aggregate) {
    final KGroupedStreamHolder source = aggregate.getSource().build(this);
    return StreamAggregateBuilder.build(
        source,
        aggregate,
        queryBuilder,
        streamsFactories.getMaterializedFactory(),
        aggregateParamFactory
    );
  }

  @Override
  public KTableHolder<Struct> visitTableAggregate(final TableAggregate aggregate) {
    final KGroupedTableHolder source = aggregate.getSource().build(this);
    return TableAggregateBuilder.build(
        source,
        aggregate,
        queryBuilder,
        streamsFactories.getMaterializedFactory(),
        aggregateParamFactory
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableFilter(final TableFilter<K> tableFilter) {
    final KTableHolder<K> source = tableFilter.getSource().build(this);
    return TableFilterBuilder.build(source, tableFilter, queryBuilder, sqlPredicateFactory);
  }

  @Override
  public <K> KGroupedTableHolder visitTableGroupBy(
      final TableGroupBy<K> tableGroupBy) {
    final KTableHolder<K> source = tableGroupBy.getSource().build(this);
    return new TableGroupByBuilder(
        queryBuilder,
        streamsFactories.getGroupedFactory()
    ).build(
        source,
        tableGroupBy
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableSelect(
      final TableSelect<K> tableSelect) {
    final KTableHolder<K> source = tableSelect.getSource().build(this);
    return TableSelectBuilder.build(source, tableSelect, queryBuilder);
  }

  @Override
  public <K> KTableHolder<K> visitTableSink(final TableSink<K> tableSink) {
    final KTableHolder<K> source = tableSink.getSource().build(this);
    TableSinkBuilder.build(source, tableSink, queryBuilder);
    return source;
  }

  @Override
  public <K> KTableHolder<K> visitTableSuppress(final TableSuppress<K> tableSuppress) {
    final KTableHolder<K>  source = tableSuppress.getSource().build(this);
    return new TableSuppressBuilder().build(
        source,
        tableSuppress,
        queryBuilder,
        source.getKeySerdeFactory()
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableTableJoin(
      final TableTableJoin<K> tableTableJoin) {
    final KTableHolder<K> left = tableTableJoin.getLeftSource().build(this);
    final KTableHolder<K> right = tableTableJoin.getRightSource().build(this);
    return TableTableJoinBuilder.build(left, right, tableTableJoin);
  }
}
