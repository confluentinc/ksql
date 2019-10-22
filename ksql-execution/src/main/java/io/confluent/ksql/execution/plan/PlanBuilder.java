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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * A visitor interface for building a query from an execution plan. There is a single
 * visit method for each execution step type (final implementations of ExecutionStep).
 * Implementers typically implement the visit methods by applying the transformation
 * specified in the step to the stream/table(s) returned by visiting hte source steps.
 */
public interface PlanBuilder {
  <K> KStreamHolder<K> visitStreamFilter(StreamFilter<K> streamFilter);

  <K> KGroupedStream<Struct, GenericRow> visitStreamGroupBy(StreamGroupBy<K> streamGroupBy);

  KGroupedStream<Struct, GenericRow> visitStreamGroupByKey(StreamGroupByKey streamGroupByKey);

  KTableHolder<Struct> visitStreamAggregate(StreamAggregate streamAggregate);

  <K> KStreamHolder<K> visitStreamMapValues(StreamMapValues<K> streamMapValues);

  <K> KStreamHolder<K> visitFlatMap(StreamFlatMap<K> streamFlatMap);

  KStreamHolder<Struct> visitStreamSelectKey(StreamSelectKey<?> streamSelectKey);

  <K> KStreamHolder<K> visitStreamSink(StreamSink<K> streamSink);

  KStreamHolder<Struct> visitStreamSource(StreamSource streamSource);

  KStreamHolder<Windowed<Struct>> visitWindowedStreamSource(
      WindowedStreamSource windowedStreamSource);

  <K> KStreamHolder<K> visitStreamStreamJoin(StreamStreamJoin<K> streamStreamJoin);

  <K> KStreamHolder<K> visitStreamTableJoin(StreamTableJoin<K> streamTableJoin);

  <K> KTableHolder<K> visitStreamToTable(StreamToTable<K> streamToTable);

  KTableHolder<Windowed<Struct>> visitStreamWindowedAggregate(
      StreamWindowedAggregate streamWindowedAggregate);

  KTableHolder<Struct> visitTableAggregate(TableAggregate tableAggregate);

  <K> KTableHolder<K> visitTableFilter(TableFilter<K> tableFilter);

  <K> KGroupedTable<Struct, GenericRow> visitTableGroupBy(TableGroupBy<K> tableGroupBy);

  <K> KTableHolder<K> visitTableMapValues(TableMapValues<K> tableMapValues);

  <K> KTableHolder<K> visitTableSink(TableSink<K> tableSink);

  <K> KTableHolder<K> visitTableTableJoin(TableTableJoin<K> tableTableJoin);
}
