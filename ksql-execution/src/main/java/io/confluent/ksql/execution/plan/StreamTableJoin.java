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
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamTableJoin<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final JoinType joinType;
  private final Formats internalFormats;
  private final ExecutionStep<KStreamHolder<K>> leftSource;
  private final ExecutionStep<KTableHolder<K>> rightSource;

  public StreamTableJoin(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "joinType", required = true) final JoinType joinType,
      @JsonProperty(value = "internalFormats", required = true) final Formats internalFormats,
      @JsonProperty(value = "leftSource", required = true) final
      ExecutionStep<KStreamHolder<K>> leftSource,
      @JsonProperty(value = "rightSource", required = true) final
      ExecutionStep<KTableHolder<K>> rightSource) {
    this.properties = Objects.requireNonNull(props, "props");
    this.internalFormats = Objects.requireNonNull(internalFormats, "internalFormats");
    this.joinType = Objects.requireNonNull(joinType, "joinType");
    this.leftSource = Objects.requireNonNull(leftSource, "leftSource");
    this.rightSource = Objects.requireNonNull(rightSource, "rightSource");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return ImmutableList.of(leftSource, rightSource);
  }

  public Formats getInternalFormats() {
    return internalFormats;
  }

  public ExecutionStep<KStreamHolder<K>> getLeftSource() {
    return leftSource;
  }

  public ExecutionStep<KTableHolder<K>> getRightSource() {
    return rightSource;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder) {
    return builder.visitStreamTableJoin(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamTableJoin<?> that = (StreamTableJoin<?>) o;
    return Objects.equals(properties, that.properties)
        && joinType == that.joinType
        && Objects.equals(internalFormats, that.internalFormats)
        && Objects.equals(leftSource, that.leftSource)
        && Objects.equals(rightSource, that.rightSource);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, joinType, internalFormats, leftSource, rightSource);
  }
}
