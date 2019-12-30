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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;

@Immutable
public class StreamWindowedAggregate
    implements ExecutionStep<KTableHolder<Windowed<Struct>>> {
  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KGroupedStreamHolder> source;
  private final Formats internalFormats;
  private final ImmutableList<ColumnRef> nonAggregateColumns;
  private final ImmutableList<FunctionCall> aggregationFunctions;
  private final KsqlWindowExpression windowExpression;

  public StreamWindowedAggregate(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final
      ExecutionStep<KGroupedStreamHolder> source,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats,
      @JsonProperty(value = "nonAggregateColumns", required = true) final
      List<ColumnRef> nonAggregateColumns,
      @JsonProperty(value = "aggregationFunctions", required = true) final
      List<FunctionCall> aggregationFunctions,
      @JsonProperty(value = "windowExpression", required = true) final
      KsqlWindowExpression windowExpression) {
    this.properties = requireNonNull(props, "props");
    this.source = requireNonNull(source, "source");
    this.internalFormats = requireNonNull(internalFormats, "internalFormats");
    this.nonAggregateColumns
        = ImmutableList.copyOf(requireNonNull(nonAggregateColumns, "nonAggregateColumns"));
    this.aggregationFunctions = ImmutableList.copyOf(
        requireNonNull(aggregationFunctions, "aggregationFunctions"));
    this.windowExpression = requireNonNull(windowExpression, "windowExpression");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public List<FunctionCall> getAggregationFunctions() {
    return aggregationFunctions;
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  public List<ColumnRef> getNonAggregateColumns() {
    return nonAggregateColumns;
  }

  public KsqlWindowExpression getWindowExpression() {
    return windowExpression;
  }

  public ExecutionStep<KGroupedStreamHolder> getSource() {
    return source;
  }

  @Override
  public KTableHolder<Windowed<Struct>> build(final PlanBuilder builder) {
    return builder.visitStreamWindowedAggregate(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamWindowedAggregate that = (StreamWindowedAggregate) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(internalFormats, that.internalFormats)
        && Objects.equals(aggregationFunctions, that.aggregationFunctions)
        && Objects.equals(windowExpression, that.windowExpression)
        && Objects.equals(nonAggregateColumns, that.nonAggregateColumns);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        properties,
        source,
        internalFormats,
        aggregationFunctions,
        nonAggregateColumns,
        windowExpression
    );
  }
}
