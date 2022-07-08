/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (final the "License"); you may not use
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
 * Visitor for extracting {@link PlanInfo} from an execution step plan,
 * to be passed to a {@link PlanBuilder} for use when translating the
 * execution step plan into a physical plan.
 *
 * <p>See {@link PlanInfo} description for more.
 */
public class PlanInfoExtractor {
  
  public <K> PlanInfo visitStreamFilter(final StreamFilter<K> streamFilter) {
    return visitSingleSourceStep(streamFilter);
  }

  public <K> PlanInfo visitStreamGroupBy(final StreamGroupByV1<K> streamGroupBy) {
    return visitSingleSourceStep(streamGroupBy);
  }

  public <K> PlanInfo visitStreamGroupBy(final StreamGroupBy<K> streamGroupBy) {
    return visitSingleSourceStep(streamGroupBy);
  }

  public PlanInfo visitStreamGroupByKey(final StreamGroupByKey streamGroupByKey) {
    return visitSingleSourceStep(streamGroupByKey);
  }

  public PlanInfo visitStreamAggregate(final StreamAggregate streamAggregate) {
    return visitSingleSourceStep(streamAggregate);
  }

  public <K> PlanInfo visitStreamSelect(final StreamSelect<K> streamSelect) {
    return visitSingleSourceStep(streamSelect);
  }

  public <K> PlanInfo visitFlatMap(final StreamFlatMap<K> streamFlatMap) {
    return visitSingleSourceStep(streamFlatMap);
  }

  public PlanInfo visitStreamSelectKey(final StreamSelectKeyV1 streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  public <K> PlanInfo visitStreamSelectKey(final StreamSelectKey<K> streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  public <K> PlanInfo visitStreamSink(final StreamSink<K> streamSink) {
    return visitSingleSourceStep(streamSink);
  }

  public PlanInfo visitStreamSource(final StreamSource streamSource) {
    return visitSourceStep(streamSource);
  }

  public PlanInfo visitWindowedStreamSource(final WindowedStreamSource windowedStreamSource) {
    return visitSourceStep(windowedStreamSource);
  }

  public <K> PlanInfo visitStreamStreamJoin(final StreamStreamJoin<K> streamStreamJoin) {
    return visitJoinStep(streamStreamJoin);
  }

  public <K> PlanInfo visitStreamTableJoin(final StreamTableJoin<K> streamTableJoin) {
    return visitJoinStep(streamTableJoin);
  }

  public PlanInfo visitTableSource(final TableSourceV1 tableSourceV1) {
    return visitSourceStep(tableSourceV1);
  }

  public PlanInfo visitTableSource(final TableSource tableSource) {
    return visitSourceStep(tableSource);
  }

  public PlanInfo visitWindowedTableSource(final WindowedTableSource windowedTableSource) {
    return visitSourceStep(windowedTableSource);
  }

  public PlanInfo visitStreamWindowedAggregate(
      final StreamWindowedAggregate streamWindowedAggregate
  ) {
    return visitSingleSourceStep(streamWindowedAggregate);
  }

  public PlanInfo visitTableAggregate(final TableAggregate tableAggregate) {
    return visitSingleSourceStep(tableAggregate);
  }

  public <K> PlanInfo visitTableFilter(final TableFilter<K> tableFilter) {
    return visitSingleSourceStep(tableFilter);
  }

  public <K> PlanInfo visitTableGroupBy(final TableGroupByV1<K> tableGroupBy) {
    return visitSingleSourceStep(tableGroupBy);
  }

  public <K> PlanInfo visitTableGroupBy(final TableGroupBy<K> tableGroupBy) {
    return visitSingleSourceStep(tableGroupBy);
  }

  public <K> PlanInfo visitTableSelect(final TableSelect<K> tableSelect) {
    return visitSingleSourceStep(tableSelect);
  }

  public <K> PlanInfo visitTableSelectKey(final TableSelectKey<K> tableSelectKey) {
    return visitRepartitionStep(tableSelectKey);
  }

  public <K> PlanInfo visitTableSink(final TableSink<K> tableSink) {
    return visitSingleSourceStep(tableSink);
  }

  public <K> PlanInfo visitTableSuppress(final TableSuppress<K> tableSuppress) {
    return visitSingleSourceStep(tableSuppress);
  }

  public <K> PlanInfo visitTableTableJoin(final TableTableJoin<K> tableTableJoin) {
    return visitJoinStep(tableTableJoin);
  }

  public <KLeftT, KRightT> PlanInfo visitForeignKeyTableTableJoin(
      final ForeignKeyTableTableJoin<KLeftT, KRightT> foreignKeyTableTableJoin) {

    return visitJoinStep(foreignKeyTableTableJoin);
  }

  private PlanInfo visitSourceStep(final ExecutionStep<?> step) {
    return new PlanInfo(step);
  }

  private PlanInfo visitRepartitionStep(final ExecutionStep<?> step) {
    final PlanInfo sourceInfo = step.getSources().get(0).extractPlanInfo(this);
    return sourceInfo.setIsRepartitionedInPlan();
  }

  private PlanInfo visitJoinStep(final ExecutionStep<?> step) {
    final PlanInfo leftInfo = step.getSources().get(0).extractPlanInfo(this);
    final PlanInfo rightInfo = step.getSources().get(1).extractPlanInfo(this);
    return leftInfo.merge(rightInfo);
  }

  private PlanInfo visitSingleSourceStep(final ExecutionStep<?> step) {
    return step.getSources().get(0).extractPlanInfo(this);
  }
}
