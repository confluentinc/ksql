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
 * A visitor to extract information about an execution step plan to be passed
 * to a {@link PlanBuilder} for use when translating the execution step plan
 * into a physical plan.
 */
public interface PlanInfoExtractor {
  <K> PlanInfo visitStreamFilter(StreamFilter<K> streamFilter);

  <K> PlanInfo visitStreamGroupBy(StreamGroupBy<K> streamGroupBy);

  PlanInfo visitStreamGroupByKey(StreamGroupByKey streamGroupByKey);

  PlanInfo visitStreamAggregate(StreamAggregate streamAggregate);

  <K> PlanInfo visitStreamSelect(StreamSelect<K> streamSelect);

  <K> PlanInfo visitFlatMap(StreamFlatMap<K> streamFlatMap);

  PlanInfo visitStreamSelectKey(StreamSelectKeyV1 streamSelectKey);

  <K> PlanInfo visitStreamSelectKey(StreamSelectKey<K> streamSelectKey);

  <K> PlanInfo visitStreamSink(StreamSink<K> streamSink);

  PlanInfo visitStreamSource(StreamSource streamSource);

  PlanInfo visitWindowedStreamSource(
      WindowedStreamSource windowedStreamSource);

  <K> PlanInfo visitStreamStreamJoin(StreamStreamJoin<K> streamStreamJoin);

  <K> PlanInfo visitStreamTableJoin(StreamTableJoin<K> streamTableJoin);

  PlanInfo visitTableSource(TableSource tableSource);

  PlanInfo visitWindowedTableSource(
      WindowedTableSource windowedTableSource);

  PlanInfo visitStreamWindowedAggregate(
      StreamWindowedAggregate streamWindowedAggregate);

  PlanInfo visitTableAggregate(TableAggregate tableAggregate);

  <K> PlanInfo visitTableFilter(TableFilter<K> tableFilter);

  <K> PlanInfo visitTableGroupBy(TableGroupBy<K> tableGroupBy);

  <K> PlanInfo visitTableSelect(TableSelect<K> tableSelect);

  <K> PlanInfo visitTableSelectKey(TableSelectKey<K> tableSelectKey);

  <K> PlanInfo visitTableSink(TableSink<K> tableSink);

  <K> PlanInfo visitTableSuppress(TableSuppress<K> tableSuppress);

  <K> PlanInfo visitTableTableJoin(TableTableJoin<K> tableTableJoin);
}
