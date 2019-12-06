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
import io.confluent.ksql.execution.expression.tree.Expression;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamGroupBy<K> implements ExecutionStep<KGroupedStreamHolder> {

  private final ExecutionStepPropertiesV1 properties;
  private final ExecutionStep<KStreamHolder<K>> source;
  private final Formats internalFormats;
  private final ImmutableList<Expression> groupByExpressions;

  public StreamGroupBy(
      @JsonProperty(value = "properties", required = true) ExecutionStepPropertiesV1 properties,
      @JsonProperty(value = "source", required = true) ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "internalFormats", required = true) Formats internalFormats,
      @JsonProperty(value = "groupByExpressions", required = true)
      List<Expression> groupByExpressions) {
    this.properties = requireNonNull(properties, "properties");
    this.internalFormats = requireNonNull(internalFormats, "internalFormats");
    this.source = requireNonNull(source, "source");
    this.groupByExpressions = ImmutableList
        .copyOf(requireNonNull(groupByExpressions, "groupByExpressions"));
  }

  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
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

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KGroupedStreamHolder build(PlanBuilder planVisitor) {
    return planVisitor.visitStreamGroupBy(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamGroupBy<?> that = (StreamGroupBy<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(internalFormats, that.internalFormats)
        && Objects.equals(groupByExpressions, that.groupByExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, internalFormats, groupByExpressions);
  }
}
