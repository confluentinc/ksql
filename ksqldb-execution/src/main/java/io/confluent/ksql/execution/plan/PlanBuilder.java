/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.GenericKey;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * A visitor interface for building a query from an execution plan. There is a single
 * visit method for each execution step type (final implementations of ExecutionStep).
 * Implementers typically implement the visit methods by applying the transformation
 * specified in the step to the stream/table(s) returned by visiting the source steps.
 */
public interface PlanBuilder {

  <K> KStreamHolder<K> visitStreamFilter(StreamFilter<K> streamFilter, PlanInfo planInfo);

  <K> KGroupedStreamHolder visitStreamGroupBy(StreamGroupByV1<K> streamGroupBy, PlanInfo planInfo);

  <K> KGroupedStreamHolder visitStreamGroupBy(StreamGroupBy<K> streamGroupBy, PlanInfo planInfo);

  KGroupedStreamHolder visitStreamGroupByKey(StreamGroupByKey streamGroupByKey, PlanInfo planInfo);

  KTableHolder<GenericKey> visitStreamAggregate(StreamAggregate streamAggregate, PlanInfo planInfo);

  <K> KStreamHolder<K> visitStreamSelect(StreamSelect<K> streamSelect, PlanInfo planInfo);

  <K> KStreamHolder<K> visitFlatMap(StreamFlatMap<K> streamFlatMap, PlanInfo planInfo);

  KStreamHolder<GenericKey> visitStreamSelectKey(
      StreamSelectKeyV1 streamSelectKey, PlanInfo planInfo);

  <K> KStreamHolder<K> visitStreamSelectKey(StreamSelectKey<K> streamSelectKey, PlanInfo planInfo);

  <K> KStreamHolder<K> visitStreamSink(StreamSink<K> streamSink, PlanInfo planInfo);

  KStreamHolder<GenericKey> visitStreamSource(StreamSource streamSource, PlanInfo planInfo);

  KStreamHolder<Windowed<GenericKey>> visitWindowedStreamSource(
      WindowedStreamSource windowedStreamSource, PlanInfo planInfo);

  <K> KStreamHolder<K> visitStreamStreamJoin(
      StreamStreamJoin<K> streamStreamJoin, PlanInfo planInfo);

  <K> KStreamHolder<K> visitStreamTableJoin(StreamTableJoin<K> streamTableJoin, PlanInfo planInfo);

  KTableHolder<GenericKey> visitTableSource(TableSourceV1 tableSourceV1, PlanInfo planInfo);

  KTableHolder<GenericKey> visitTableSource(TableSource tableSource, PlanInfo planInfo);

  KTableHolder<Windowed<GenericKey>> visitWindowedTableSource(
      WindowedTableSource windowedTableSource, PlanInfo planInfo);

  KTableHolder<Windowed<GenericKey>> visitStreamWindowedAggregate(
      StreamWindowedAggregate streamWindowedAggregate, PlanInfo planInfo);

  KTableHolder<GenericKey> visitTableAggregate(TableAggregate tableAggregate, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableFilter(TableFilter<K> tableFilter, PlanInfo planInfo);

  <K> KGroupedTableHolder visitTableGroupBy(TableGroupByV1<K> tableGroupBy, PlanInfo planInfo);

  <K> KGroupedTableHolder visitTableGroupBy(TableGroupBy<K> tableGroupBy, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableSelect(TableSelect<K> tableSelect, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableSelectKey(TableSelectKey<K> tableSelectKey, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableSink(TableSink<K> tableSink, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableSuppress(TableSuppress<K> tableSuppress, PlanInfo planInfo);

  <K> KTableHolder<K> visitTableTableJoin(TableTableJoin<K> tableTableJoin, PlanInfo planInfo);

  <KLeftT, KRightT> KTableHolder<KLeftT> visitForeignKeyTableTableJoin(
      ForeignKeyTableTableJoin<KLeftT, KRightT> foreignKeyTableTableJoin,
      PlanInfo planInfo
  );
}
