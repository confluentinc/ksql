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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class TableAggregate implements ExecutionStep<KTableHolder<Struct>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KGroupedTableHolder> source;
  private final Formats formats;
  private final int nonFuncColumnCount;
  private final List<FunctionCall> aggregations;

  public TableAggregate(
      @JsonProperty(value = "properties", required = true) ExecutionStepProperties properties,
      @JsonProperty(value = "source", required = true)
      ExecutionStep<KGroupedTableHolder> source,
      @JsonProperty(value = "formats", required = true) Formats formats,
      @JsonProperty(value = "nonFuncColumnCount", required = true) int nonFuncColumnCount,
      @JsonProperty(value = "aggregations", required = true)
      List<FunctionCall> aggregations) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.nonFuncColumnCount = nonFuncColumnCount;
    this.aggregations = Objects.requireNonNull(aggregations, "aggValToFunctionMap");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public Formats getFormats() {
    return formats;
  }

  public List<FunctionCall> getAggregations() {
    return aggregations;
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
        && Objects.equals(formats, that.formats)
        && nonFuncColumnCount == that.nonFuncColumnCount
        && Objects.equals(aggregations, that.aggregations);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, nonFuncColumnCount, aggregations);
  }
}
