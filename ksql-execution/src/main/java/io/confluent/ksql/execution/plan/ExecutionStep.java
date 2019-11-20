/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.PROPERTY
)
@JsonSubTypes({
    @Type(value = StreamAggregate.class, name = "streamAggregateV1"),
    @Type(value = StreamFilter.class, name = "streamFilterV1"),
    @Type(value = StreamFlatMap.class, name = "streamFlatMapV1"),
    @Type(value = StreamGroupBy.class, name = "streamGroupByV1"),
    @Type(value = StreamGroupByKey.class, name = "streamGroupByKeyV1"),
    @Type(value = StreamMapValues.class, name = "streamMapValuesV1"),
    @Type(value = StreamSelectKey.class, name = "streamSelectKeyV1"),
    @Type(value = StreamSink.class, name = "streamSinkV1"),
    @Type(value = StreamSource.class, name = "streamSourceV1"),
    @Type(value = WindowedStreamSource.class, name = "windowedStreamSourceV1"),
    @Type(value = StreamStreamJoin.class, name = "streamStreamJoinV1"),
    @Type(value = StreamTableJoin.class, name = "streamTableJoinV1"),
    @Type(value = StreamToTable.class, name = "streamToTableV1"),
    @Type(value = StreamWindowedAggregate.class, name = "streamWindowedAggregateV1"),
    @Type(value = TableAggregate.class, name = "tableAggregateV1"),
    @Type(value = TableFilter.class, name = "tableFilterV1"),
    @Type(value = TableGroupBy.class, name = "tableGroupByV1"),
    @Type(value = TableMapValues.class, name = "tableMapValuesV1"),
    @Type(value = TableSink.class, name = "tableSinkV1"),
    @Type(value = TableTableJoin.class, name = "tableTableJoinV1")
})
public interface ExecutionStep<S> {
  ExecutionStepProperties getProperties();

  @JsonIgnore
  List<ExecutionStep<?>> getSources();

  S build(PlanBuilder planBuilder);

  @JsonIgnore
  default LogicalSchema getSchema() {
    return getProperties().getSchema();
  }
}
