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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class TableAggregate implements ExecutionStep<KTableHolder<Struct>> {
  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KGroupedTableHolder> source;
  private final Formats internalFormats;
  private final int nonFuncColumnCount;
  private final ImmutableList<FunctionCall> aggregationFunctions;

  public TableAggregate(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "source", required = true)
      ExecutionStep<KGroupedTableHolder> source,
      @JsonProperty(value = "internalFormats", required = true) Formats internalFormats,
      @JsonProperty(value = "nonFuncColumnCount", required = true) int nonFuncColumnCount,
      @JsonProperty(value = "aggregationFunctions", required = true)
      List<FunctionCall> aggregationFunctions) {
    this.properties = requireNonNull(properties, "properties");
    this.source = requireNonNull(source, "source");
    this.internalFormats = requireNonNull(internalFormats, "internalFormats");
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggregationFunctions = ImmutableList
        .copyOf(requireNonNull(aggregationFunctions, "aggValToFunctionMap"));
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

  public Formats getInternalFormats() {
    return internalFormats;
  }

  public List<FunctionCall> getAggregationFunctions() {
    return aggregationFunctions;
  }

  public int getNonFuncColumnCount() {
    return nonFuncColumnCount;
  }

  public ExecutionStep<KGroupedTableHolder> getSource() {
    return source;
  }

  @Override
  public KTableHolder<Struct> build(PlanBuilder builder) {
    return builder.visitTableAggregate(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableAggregate that = (TableAggregate) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(internalFormats, that.internalFormats)
        && nonFuncColumnCount == that.nonFuncColumnCount
        && Objects.equals(aggregationFunctions, that.aggregationFunctions);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, internalFormats, nonFuncColumnCount,
        aggregationFunctions);
  }
}
