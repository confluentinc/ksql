/*
 * Copyright 2020 Confluent Inc.
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
public class PlanInfoExtractor implements ExecutionStepVisitor<PlanInfo> {
  
  @Override
  public <K> PlanInfo visitStreamFilter(final StreamFilter<K> streamFilter) {
    return visitSingleSourceStep(streamFilter);
  }

  @Override
  public <K> PlanInfo visitStreamGroupBy(final StreamGroupBy<K> streamGroupBy) {
    return visitSingleSourceStep(streamGroupBy);
  }

  @Override
  public PlanInfo visitStreamGroupByKey(final StreamGroupByKey streamGroupByKey) {
    return visitSingleSourceStep(streamGroupByKey);
  }

  @Override
  public PlanInfo visitStreamAggregate(final StreamAggregate streamAggregate) {
    return visitSingleSourceStep(streamAggregate);
  }

  @Override
  public <K> PlanInfo visitStreamSelect(final StreamSelect<K> streamSelect) {
    return visitSingleSourceStep(streamSelect);
  }

  @Override
  public <K> PlanInfo visitFlatMap(final StreamFlatMap<K> streamFlatMap) {
    return visitSingleSourceStep(streamFlatMap);
  }

  @Override
  public PlanInfo visitStreamSelectKey(final StreamSelectKeyV1 streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  @Override
  public <K> PlanInfo visitStreamSelectKey(final StreamSelectKey<K> streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  @Override
  public <K> PlanInfo visitStreamSink(final StreamSink<K> streamSink) {
    return visitSingleSourceStep(streamSink);
  }

  @Override
  public PlanInfo visitStreamSource(final StreamSource streamSource) {
    return visitSourceStep(streamSource);
  }

  @Override
  public PlanInfo visitWindowedStreamSource(final WindowedStreamSource windowedStreamSource) {
    return visitSourceStep(windowedStreamSource);
  }

  @Override
  public <K> PlanInfo visitStreamStreamJoin(final StreamStreamJoin<K> streamStreamJoin) {
    return visitJoinStep(streamStreamJoin);
  }

  @Override
  public <K> PlanInfo visitStreamTableJoin(final StreamTableJoin<K> streamTableJoin) {
    return visitJoinStep(streamTableJoin);
  }

  @Override
  public PlanInfo visitTableSource(final TableSource tableSource) {
    return visitSourceStep(tableSource);
  }

  @Override
  public PlanInfo visitWindowedTableSource(final WindowedTableSource windowedTableSource) {
    return visitSourceStep(windowedTableSource);
  }

  @Override
  public PlanInfo visitStreamWindowedAggregate(
      final StreamWindowedAggregate streamWindowedAggregate
  ) {
    return visitSingleSourceStep(streamWindowedAggregate);
  }

  @Override
  public PlanInfo visitTableAggregate(final TableAggregate tableAggregate) {
    return visitSingleSourceStep(tableAggregate);
  }

  @Override
  public <K> PlanInfo visitTableFilter(final TableFilter<K> tableFilter) {
    return visitSingleSourceStep(tableFilter);
  }

  @Override
  public <K> PlanInfo visitTableGroupBy(final TableGroupBy<K> tableGroupBy) {
    return visitSingleSourceStep(tableGroupBy);
  }

  @Override
  public <K> PlanInfo visitTableSelect(final TableSelect<K> tableSelect) {
    return visitSingleSourceStep(tableSelect);
  }

  @Override
  public <K> PlanInfo visitTableSelectKey(final TableSelectKey<K> tableSelectKey) {
    return visitRepartitionStep(tableSelectKey);
  }

  @Override
  public <K> PlanInfo visitTableSink(final TableSink<K> tableSink) {
    return visitSingleSourceStep(tableSink);
  }

  @Override
  public <K> PlanInfo visitTableSuppress(final TableSuppress<K> tableSuppress) {
    return visitSingleSourceStep(tableSuppress);
  }

  @Override
  public <K> PlanInfo visitTableTableJoin(final TableTableJoin<K> tableTableJoin) {
    return visitJoinStep(tableTableJoin);
  }

  private PlanInfo visitSourceStep(final ExecutionStep<?> step) {
    return new PlanInfo(step);
  }

  private PlanInfo visitRepartitionStep(final ExecutionStep<?> step) {
    final PlanInfo sourceInfo = (PlanInfo) step.getSources().get(0).extractPlanInfo(this);
    return sourceInfo.setIsRepartitionedInPlan();
  }

  private PlanInfo visitJoinStep(final ExecutionStep<?> step) {
    final PlanInfo leftInfo = (PlanInfo) step.getSources().get(0).extractPlanInfo(this);
    final PlanInfo rightInfo = (PlanInfo) step.getSources().get(1).extractPlanInfo(this);
    return leftInfo.merge(rightInfo);
  }

  private PlanInfo visitSingleSourceStep(final ExecutionStep<?> step) {
    return (PlanInfo) step.getSources().get(0).extractPlanInfo(this);
  }
}
