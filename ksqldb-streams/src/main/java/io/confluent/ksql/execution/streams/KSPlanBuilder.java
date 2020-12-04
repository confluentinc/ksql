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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.KGroupedStreamHolder;
import io.confluent.ksql.execution.plan.KGroupedTableHolder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanBuilder;
import io.confluent.ksql.execution.plan.PlanInfo;
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
import io.confluent.ksql.execution.plan.TableSelectKey;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.TableSuppress;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import java.util.Objects;
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

  @Override
  public <K> KStreamHolder<K> visitStreamFilter(
      final StreamFilter<K> streamFilter,
      final PlanInfo planInfo) {
    final KStreamHolder<K> source = streamFilter.getSource().build(this, planInfo);
    return StreamFilterBuilder.build(source, streamFilter, queryBuilder, sqlPredicateFactory);
  }

  @Override
  public <K> KGroupedStreamHolder visitStreamGroupBy(
      final StreamGroupBy<K> streamGroupBy,
      final PlanInfo planInfo) {
    final KStreamHolder<K> source = streamGroupBy.getSource().build(this, planInfo);
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
      final StreamGroupByKey streamGroupByKey,
      final PlanInfo planInfo) {
    final KStreamHolder<GenericKey> source = streamGroupByKey.getSource().build(this, planInfo);
    return new StreamGroupByBuilder(
        queryBuilder,
        streamsFactories.getGroupedFactory()
    ).build(
        source,
        streamGroupByKey
    );
  }

  @Override
  public KTableHolder<GenericKey> visitStreamAggregate(
      final StreamAggregate streamAggregate,
      final PlanInfo planInfo) {
    final KGroupedStreamHolder source = streamAggregate.getSource().build(this, planInfo);
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
      final StreamSelect<K> streamSelect,
      final PlanInfo planInfo) {
    final KStreamHolder<K> source = streamSelect.getSource().build(this, planInfo);
    return StreamSelectBuilder.build(source, streamSelect, queryBuilder);
  }

  @Override
  public <K> KStreamHolder<K> visitFlatMap(
      final StreamFlatMap<K> streamFlatMap,
      final PlanInfo planInfo) {
    final KStreamHolder<K> source = streamFlatMap.getSource().build(this, planInfo);
    return StreamFlatMapBuilder.build(source, streamFlatMap, queryBuilder);
  }

  @Override
  public KStreamHolder<GenericKey> visitStreamSelectKey(
      final StreamSelectKeyV1 streamSelectKey,
      final PlanInfo planInfo
  ) {
    final KStreamHolder<?> source = streamSelectKey.getSource().build(this, planInfo);
    return StreamSelectKeyBuilderV1.build(source, streamSelectKey, queryBuilder);
  }

  @Override
  public <K> KStreamHolder<K> visitStreamSelectKey(
      final StreamSelectKey<K> streamSelectKey,
      final PlanInfo planInfo
  ) {
    final KStreamHolder<K> source = streamSelectKey.getSource().build(this, planInfo);
    return StreamSelectKeyBuilder.build(source, streamSelectKey, queryBuilder);
  }

  @Override
  public <K> KStreamHolder<K> visitStreamSink(
      final StreamSink<K> streamSink,
      final PlanInfo planInfo) {
    final KStreamHolder<K> source = streamSink.getSource().build(this, planInfo);
    StreamSinkBuilder.build(source, streamSink, queryBuilder);
    return null;
  }

  @Override
  public KStreamHolder<GenericKey> visitStreamSource(
      final StreamSource streamSource,
      final PlanInfo planInfo) {
    return SourceBuilder.buildStream(
        queryBuilder,
        streamSource,
        streamsFactories.getConsumedFactory()
    );
  }

  @Override
  public KStreamHolder<Windowed<GenericKey>> visitWindowedStreamSource(
      final WindowedStreamSource windowedStreamSource,
      final PlanInfo planInfo) {
    return SourceBuilder.buildWindowedStream(
        queryBuilder,
        windowedStreamSource,
        streamsFactories.getConsumedFactory()
    );
  }

  @Override
  public <K> KStreamHolder<K> visitStreamStreamJoin(
      final StreamStreamJoin<K> join,
      final PlanInfo planInfo) {
    final KStreamHolder<K> left = join.getLeftSource().build(this, planInfo);
    final KStreamHolder<K> right = join.getRightSource().build(this, planInfo);
    return StreamStreamJoinBuilder.build(
        left,
        right,
        join,
        queryBuilder,
        streamsFactories.getStreamJoinedFactory()
    );
  }

  @Override
  public <K> KStreamHolder<K> visitStreamTableJoin(
      final StreamTableJoin<K> join,
      final PlanInfo planInfo) {
    final KTableHolder<K> right = join.getRightSource().build(this, planInfo);
    final KStreamHolder<K> left = join.getLeftSource().build(this, planInfo);
    return StreamTableJoinBuilder.build(
        left,
        right,
        join,
        queryBuilder,
        streamsFactories.getJoinedFactory()
    );
  }

  @Override
  public KTableHolder<GenericKey> visitTableSource(
      final TableSource tableSource,
      final PlanInfo planInfo) {
    return SourceBuilder.buildTable(
        queryBuilder,
        tableSource,
        streamsFactories.getConsumedFactory(),
        streamsFactories.getMaterializedFactory(),
        (PlanInfo) planInfo
    );
  }

  @Override
  public KTableHolder<Windowed<GenericKey>> visitWindowedTableSource(
      final WindowedTableSource windowedTableSource,
      final PlanInfo planInfo
  ) {
    return SourceBuilder.buildWindowedTable(
        queryBuilder,
        windowedTableSource,
        streamsFactories.getConsumedFactory(),
        streamsFactories.getMaterializedFactory(),
        (PlanInfo) planInfo
    );
  }

  @Override
  public KTableHolder<Windowed<GenericKey>> visitStreamWindowedAggregate(
      final StreamWindowedAggregate aggregate,
      final PlanInfo planInfo) {
    final KGroupedStreamHolder source = aggregate.getSource().build(this, planInfo);
    return StreamAggregateBuilder.build(
        source,
        aggregate,
        queryBuilder,
        streamsFactories.getMaterializedFactory(),
        aggregateParamFactory
    );
  }

  @Override
  public KTableHolder<GenericKey> visitTableAggregate(
      final TableAggregate aggregate,
      final PlanInfo planInfo) {
    final KGroupedTableHolder source = aggregate.getSource().build(this, planInfo);
    return TableAggregateBuilder.build(
        source,
        aggregate,
        queryBuilder,
        streamsFactories.getMaterializedFactory(),
        aggregateParamFactory
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableFilter(
      final TableFilter<K> tableFilter,
      final PlanInfo planInfo) {
    final KTableHolder<K> source = tableFilter.getSource().build(this, planInfo);
    return TableFilterBuilder.build(source, tableFilter, queryBuilder, sqlPredicateFactory);
  }

  @Override
  public <K> KGroupedTableHolder visitTableGroupBy(
      final TableGroupBy<K> tableGroupBy,
      final PlanInfo planInfo) {
    final KTableHolder<K> source = tableGroupBy.getSource().build(this, planInfo);
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
      final TableSelect<K> tableSelect,
      final PlanInfo planInfo) {
    final KTableHolder<K> source = tableSelect.getSource().build(this, planInfo);
    return TableSelectBuilder.build(source, tableSelect, queryBuilder);
  }

  @Override
  public <K> KTableHolder<K> visitTableSelectKey(
      final TableSelectKey<K> tableSelectKey,
      final PlanInfo planInfo) {
    final KTableHolder<K> source = tableSelectKey.getSource().build(this, planInfo);
    return TableSelectKeyBuilder.build(
        source,
        tableSelectKey,
        queryBuilder,
        streamsFactories.getMaterializedFactory()
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableSink(
      final TableSink<K> tableSink,
      final PlanInfo planInfo) {
    final KTableHolder<K> source = tableSink.getSource().build(this, planInfo);
    TableSinkBuilder.build(source, tableSink, queryBuilder);
    return source;
  }

  @Override
  public <K> KTableHolder<K> visitTableSuppress(
      final TableSuppress<K> tableSuppress,
      final PlanInfo planInfo) {
    final KTableHolder<K>  source = tableSuppress.getSource().build(this, planInfo);
    return new TableSuppressBuilder().build(
        source,
        tableSuppress,
        queryBuilder,
        source.getExecutionKeyFactory()
    );
  }

  @Override
  public <K> KTableHolder<K> visitTableTableJoin(
      final TableTableJoin<K> tableTableJoin,
      final PlanInfo planInfo) {
    final KTableHolder<K> left = tableTableJoin.getLeftSource().build(this, planInfo);
    final KTableHolder<K> right = tableTableJoin.getRightSource().build(this, planInfo);
    return TableTableJoinBuilder.build(left, right, tableTableJoin);
  }
}
