/*
 * Copyright 2020 Confluent Inc.
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

/**
 * A visitor for traversing an execution step plan, prior to the plan being
 * passed to a {@link PlanBuilder} for translation into a physical plan.
 */
public interface ExecutionStepVisitor<R> {

  <K> R visitStreamFilter(StreamFilter<K> streamFilter);

  <K> R visitStreamGroupBy(StreamGroupBy<K> streamGroupBy);

  R visitStreamGroupByKey(StreamGroupByKey streamGroupByKey);

  R visitStreamAggregate(StreamAggregate streamAggregate);

  <K> R visitStreamSelect(StreamSelect<K> streamSelect);

  <K> R visitFlatMap(StreamFlatMap<K> streamFlatMap);

  R visitStreamSelectKey(StreamSelectKeyV1 streamSelectKey);

  <K> R visitStreamSelectKey(StreamSelectKey<K> streamSelectKey);

  <K> R visitStreamSink(StreamSink<K> streamSink);

  R visitStreamSource(StreamSource streamSource);

  R visitWindowedStreamSource(
      WindowedStreamSource windowedStreamSource);

  <K> R visitStreamStreamJoin(StreamStreamJoin<K> streamStreamJoin);

  <K> R visitStreamTableJoin(StreamTableJoin<K> streamTableJoin);

  R visitTableSource(TableSource tableSource);

  R visitWindowedTableSource(
      WindowedTableSource windowedTableSource);

  R visitStreamWindowedAggregate(
      StreamWindowedAggregate streamWindowedAggregate);

  R visitTableAggregate(TableAggregate tableAggregate);

  <K> R visitTableFilter(TableFilter<K> tableFilter);

  <K> R visitTableGroupBy(TableGroupBy<K> tableGroupBy);

  <K> R visitTableSelect(TableSelect<K> tableSelect);

  <K> R visitTableSelectKey(TableSelectKey<K> tableSelectKey);

  <K> R visitTableSink(TableSink<K> tableSink);

  <K> R visitTableSuppress(TableSuppress<K> tableSuppress);

  <K> R visitTableTableJoin(TableTableJoin<K> tableTableJoin);
}
