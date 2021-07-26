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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.name.ColumnName;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Windowed;

@Immutable
public class StreamWindowedAggregate
    implements ExecutionStep<KTableHolder<Windowed<GenericKey>>> {

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KGroupedStreamHolder> source;
  private final Formats internalFormats;
  private final ImmutableList<ColumnName> nonAggregateColumns;
  private final ImmutableList<FunctionCall> aggregationFunctions;
  private final KsqlWindowExpression windowExpression;

  public StreamWindowedAggregate(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final
      ExecutionStep<KGroupedStreamHolder> source,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats,
      @JsonProperty(value = "nonAggregateColumns", required = true) final
      List<ColumnName> nonAggregateColumns,
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

    if (aggregationFunctions.isEmpty()) {
      throw new IllegalArgumentException("Need at least one aggregate function");
    }
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

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "aggregationFunctions is ImmutableList"
  )
  public List<FunctionCall> getAggregationFunctions() {
    return aggregationFunctions;
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  @JsonInclude(Include.NON_NULL)
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "nonAggregateColumns is ImmutableList"
  )
  public List<ColumnName> getNonAggregateColumns() {
    return nonAggregateColumns;
  }

  public KsqlWindowExpression getWindowExpression() {
    return windowExpression;
  }

  public ExecutionStep<KGroupedStreamHolder> getSource() {
    return source;
  }

  @Override
  public KTableHolder<Windowed<GenericKey>> build(final PlanBuilder builder, final PlanInfo info) {
    return builder.visitStreamWindowedAggregate(this, info);
  }

  @Override
  public PlanInfo extractPlanInfo(final PlanInfoExtractor extractor) {
    return extractor.visitStreamWindowedAggregate(this);
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
