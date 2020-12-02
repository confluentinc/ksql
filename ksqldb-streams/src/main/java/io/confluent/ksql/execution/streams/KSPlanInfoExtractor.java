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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.PlanInfoExtractor;
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

/**
 * Visitor for extracting {@link KSPlanInfo} from an execution step plan.
 * See {@link KSPlanInfo} description for more.
 */
public class KSPlanInfoExtractor implements PlanInfoExtractor {
  
  @Override
  public <K> KSPlanInfo visitStreamFilter(final StreamFilter<K> streamFilter) {
    return visitSingleSourceStep(streamFilter);
  }

  @Override
  public <K> KSPlanInfo visitStreamGroupBy(final StreamGroupBy<K> streamGroupBy) {
    return visitSingleSourceStep(streamGroupBy);
  }

  @Override
  public KSPlanInfo visitStreamGroupByKey(final StreamGroupByKey streamGroupByKey) {
    return visitSingleSourceStep(streamGroupByKey);
  }

  @Override
  public KSPlanInfo visitStreamAggregate(final StreamAggregate streamAggregate) {
    return visitSingleSourceStep(streamAggregate);
  }

  @Override
  public <K> KSPlanInfo visitStreamSelect(final StreamSelect<K> streamSelect) {
    return visitSingleSourceStep(streamSelect);
  }

  @Override
  public <K> KSPlanInfo visitFlatMap(final StreamFlatMap<K> streamFlatMap) {
    return visitSingleSourceStep(streamFlatMap);
  }

  @Override
  public KSPlanInfo visitStreamSelectKey(final StreamSelectKeyV1 streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  @Override
  public <K> KSPlanInfo visitStreamSelectKey(final StreamSelectKey<K> streamSelectKey) {
    return visitRepartitionStep(streamSelectKey);
  }

  @Override
  public <K> KSPlanInfo visitStreamSink(final StreamSink<K> streamSink) {
    return visitSingleSourceStep(streamSink);
  }

  @Override
  public KSPlanInfo visitStreamSource(final StreamSource streamSource) {
    return visitSourceStep(streamSource);
  }

  @Override
  public KSPlanInfo visitWindowedStreamSource(final WindowedStreamSource windowedStreamSource) {
    return visitSourceStep(windowedStreamSource);
  }

  @Override
  public <K> KSPlanInfo visitStreamStreamJoin(final StreamStreamJoin<K> streamStreamJoin) {
    return visitJoinStep(streamStreamJoin);
  }

  @Override
  public <K> KSPlanInfo visitStreamTableJoin(final StreamTableJoin<K> streamTableJoin) {
    return visitJoinStep(streamTableJoin);
  }

  @Override
  public KSPlanInfo visitTableSource(final TableSource tableSource) {
    return visitSourceStep(tableSource);
  }

  @Override
  public KSPlanInfo visitWindowedTableSource(final WindowedTableSource windowedTableSource) {
    return visitSourceStep(windowedTableSource);
  }

  @Override
  public KSPlanInfo visitStreamWindowedAggregate(
      final StreamWindowedAggregate streamWindowedAggregate
  ) {
    return visitSingleSourceStep(streamWindowedAggregate);
  }

  @Override
  public KSPlanInfo visitTableAggregate(final TableAggregate tableAggregate) {
    return visitSingleSourceStep(tableAggregate);
  }

  @Override
  public <K> KSPlanInfo visitTableFilter(final TableFilter<K> tableFilter) {
    return visitSingleSourceStep(tableFilter);
  }

  @Override
  public <K> KSPlanInfo visitTableGroupBy(final TableGroupBy<K> tableGroupBy) {
    return visitSingleSourceStep(tableGroupBy);
  }

  @Override
  public <K> KSPlanInfo visitTableSelect(final TableSelect<K> tableSelect) {
    return visitSingleSourceStep(tableSelect);
  }

  @Override
  public <K> KSPlanInfo visitTableSelectKey(final TableSelectKey<K> tableSelectKey) {
    return visitRepartitionStep(tableSelectKey);
  }

  @Override
  public <K> KSPlanInfo visitTableSink(final TableSink<K> tableSink) {
    return visitSingleSourceStep(tableSink);
  }

  @Override
  public <K> KSPlanInfo visitTableSuppress(final TableSuppress<K> tableSuppress) {
    return visitSingleSourceStep(tableSuppress);
  }

  @Override
  public <K> KSPlanInfo visitTableTableJoin(final TableTableJoin<K> tableTableJoin) {
    return visitJoinStep(tableTableJoin);
  }

  private KSPlanInfo visitSourceStep(final ExecutionStep<?> step) {
    return new KSPlanInfo(step);
  }

  private KSPlanInfo visitRepartitionStep(final ExecutionStep<?> step) {
    final KSPlanInfo sourceInfo = (KSPlanInfo) step.getSources().get(0).extractPlanInfo(this);
    return sourceInfo.setIsRepartitionedInPlan();
  }

  private KSPlanInfo visitJoinStep(final ExecutionStep<?> step) {
    final KSPlanInfo leftInfo = (KSPlanInfo) step.getSources().get(0).extractPlanInfo(this);
    final KSPlanInfo rightInfo = (KSPlanInfo) step.getSources().get(1).extractPlanInfo(this);
    return leftInfo.merge(rightInfo);
  }

  private KSPlanInfo visitSingleSourceStep(final ExecutionStep<?> step) {
    return (KSPlanInfo) step.getSources().get(0).extractPlanInfo(this);
  }
}
